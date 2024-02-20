/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reclaim

import (
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (ra *Action) Name() string {
	return "reclaim"
}

func (ra *Action) Initialize() {}

// 回收：当 queue 使用的资源超过软约束后，回收资源;
// 即如果资源不够时（存在 pending 的任务时），回收其他队列的pod
func (ra *Action) Execute(ssn *framework.Session) {
	klog.V(5).Infof("Enter Reclaim ...")
	defer klog.V(5).Infof("Leaving Reclaim ...")

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueMap := map[api.QueueID]*api.QueueInfo{}

	// q2jobs
	preemptorsMap := map[api.QueueID]*util.PriorityQueue{}
	// j2tasks
	preemptorTasks := map[api.JobID]*util.PriorityQueue{}

	klog.V(3).Infof("There are <%d> Jobs and <%d> Queues in total for scheduling.",
		len(ssn.Jobs), len(ssn.Queues))

	for _, job := range ssn.Jobs {
		// 只处理 running后 的任务
		if job.IsPending() {
			continue
		}

		if vr := ssn.JobValid(job); vr != nil && !vr.Pass {
			klog.V(4).Infof("Job <%s/%s> Queue <%s> skip reclaim, reason: %v, message %v", job.Namespace, job.Name, job.Queue, vr.Reason, vr.Message)
			continue
		}

		// 这里就是拿到这些 job 的 queues，其实就是一行 getRelatedQueues(jobs) 的事情
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else if _, existed := queueMap[queue.UID]; !existed {
			klog.V(4).Infof("Added Queue <%s> for Job <%s/%s>", queue.Name, job.Namespace, job.Name)
			queueMap[queue.UID] = queue
			queues.Push(queue)
		}

		if job.HasPendingTasks() {
			// q2jobs
			if _, found := preemptorsMap[job.Queue]; !found {
				preemptorsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			preemptorsMap[job.Queue].Push(job)
			// j2tasks
			preemptorTasks[job.UID] = util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				preemptorTasks[job.UID].Push(task)
			}
		}
	}

	for {
		// If no queues, break
		if queues.Empty() {
			break
		}

		var job *api.JobInfo
		var task *api.TaskInfo

		queue := queues.Pop().(*api.QueueInfo)
		if ssn.Overused(queue) {
			klog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		// Found "high" priority job
		jobs, found := preemptorsMap[queue.UID] // q2jobs
		if !found || jobs.Empty() {
			continue
		} else {
			job = jobs.Pop().(*api.JobInfo)
		}

		// Found "high" priority task to reclaim others
		if tasks, found := preemptorTasks[job.UID]; !found || tasks.Empty() {
			continue
		} else {
			task = tasks.Pop().(*api.TaskInfo)
		}

		// task 是当前最高优先queue里的最高优先job 里的最高优先 task

		if !ssn.Allocatable(queue, task) {
			klog.V(3).Infof("Queue <%s> is overused when considering task <%s>, ignore it.", queue.Name, task.Name)
			continue
		}

		if err := ssn.PrePredicateFn(task); err != nil {
			klog.V(3).Infof("PrePredicate for task %s/%s failed for: %v", task.Namespace, task.Name, err)
			continue
		}

		assigned := false
		for _, n := range ssn.Nodes {
			// statusSets 是所有 tier、所有 plugin 的 predicateStatus
			var statusSets util.StatusSets
			statusSets, err := ssn.PredicateFn(task, n)
			if err != nil {
				klog.V(5).Infof("reclaim predicates failed for task <%s/%s> on node <%s>: %v",
					task.Namespace, task.Name, n.Name, err)
				continue
			}

			// Allows scheduling to nodes that are in Success or Unschedulable state after filtering by predicate.
			if statusSets.ContainsUnschedulableAndUnresolvable() || statusSets.ContainsErrorSkipOrWait() {
				klog.V(5).Infof("predicates failed in reclaim for task <%s/%s> on node <%s>, reason is %s.",
					task.Namespace, task.Name, n.Name, statusSets.Message())
				continue
			}
			klog.V(3).Infof("Considering Task <%s/%s> on Node <%s>.",
				task.Namespace, task.Name, n.Name)

			// reclaimees： 回收候选集
			var reclaimees []*api.TaskInfo
			// 遍历该节点托管的所有running任务，看这 running 任务
			for _, task := range n.Tasks {
				// Ignore non running task.
				if task.Status != api.Running {
					continue
				}
				if !task.Preemptable {
					continue
				}

				if j, found := ssn.Jobs[task.Job]; !found {
					continue
				} else if j.Queue != job.Queue {
					// 这里不在同一个队列，如果队列可以回收，就加到回收候选集里
					// 这里的Reclaimable只是判断了一个 flag: Reclaimable
					// 不用考虑队列 quota 和优先级吗？
					q := ssn.Queues[j.Queue]
					if !q.Reclaimable() {
						continue
					}
					// Clone task to avoid modify Task's status on node.
					reclaimees = append(reclaimees, task.Clone())
				}
			}

			if len(reclaimees) == 0 {
				klog.V(4).Infof("No reclaimees on Node <%s>.", n.Name)
				continue
			}

			// 对reclaimees，取plugins 认可的交集
			// plugin一般可能是判断对应job少掉一个 pod 后，是不是还是大于 minAvail
			victims := ssn.Reclaimable(task, reclaimees)

			// 这里看抢了之后的资源能不能容纳新 task，如果不够，就不抢了
			if err := util.ValidateVictims(task, n, victims); err != nil {
				klog.V(3).Infof("No validated victims on Node <%s>: %v", n.Name, err)
				continue
			}

			resreq := task.InitResreq.Clone()
			reclaimed := api.EmptyResource()

			// Reclaim victims for tasks.
			for _, reclaimee := range victims {
				klog.Errorf("Try to reclaim Task <%s/%s> for Tasks <%s/%s>",
					reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name)
				if err := ssn.Evict(reclaimee, "reclaim"); err != nil {
					klog.Errorf("Failed to reclaim Task <%s/%s> for Tasks <%s/%s>: %v",
						reclaimee.Namespace, reclaimee.Name, task.Namespace, task.Name, err)
					continue
				}
				reclaimed.Add(reclaimee.Resreq)
				// If reclaimed enough resources, break loop to avoid Sub panic.
				// 回收了足够资源了，就不继续回收了
				// 但是这里判断是否回收够资源的算法有点奇怪，为啥是回收资源>大于申请资源，但之前ValidateVictims的时候是加上了节点剩余资源的
				if resreq.LessEqual(reclaimed, api.Zero) {
					break
				}
			}

			klog.V(3).Infof("Reclaimed <%v> for task <%s/%s> requested <%v>.",
				reclaimed, task.Namespace, task.Name, task.InitResreq)

			if task.InitResreq.LessEqual(reclaimed, api.Zero) {
				if err := ssn.Pipeline(task, n.Name); err != nil {
					klog.Errorf("Failed to pipeline Task <%s/%s> on Node <%s>",
						task.Namespace, task.Name, n.Name)
				}

				// Ignore error of pipeline, will be corrected in next scheduling loop.
				assigned = true

				break
			}
		}

		// 如果分配成功，就继续分配这个job的其他较低优先级的task
		// 否则就分配其他任务（因为这个 job 分配不了，必须先分配高优task）
		if assigned {
			jobs.Push(job)
		}
		queues.Push(queue)
	}
}

func (ra *Action) UnInitialize() {
}
