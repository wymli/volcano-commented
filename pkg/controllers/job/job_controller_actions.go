/*
Copyright 2019 The Volcano Authors.

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

package job

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/klog/v2"

	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	scheduling "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	"volcano.sh/volcano/pkg/controllers/apis"
	jobhelpers "volcano.sh/volcano/pkg/controllers/job/helpers"
	"volcano.sh/volcano/pkg/controllers/job/state"
	"volcano.sh/volcano/pkg/controllers/util"
)

var calMutex sync.Mutex

// killJob： 删除对应的 pod and podgroup
func (cc *jobcontroller) killJob(jobInfo *apis.JobInfo, podRetainPhase state.PhaseMap, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Killing Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> killing, current version %d", job.Namespace, job.Name, job.Status.Version)

	// DeletionTimestamp： 这是 k8s 的延迟删除策略
	if job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			job.Namespace, job.Name)
		return nil
	}

	var pending, running, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)

	var errs []error
	var total int

	for _, pods := range jobInfo.Pods {
		for _, pod := range pods {
			total++

			if pod.DeletionTimestamp != nil {
				klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
				terminating++
				continue
			}

			// 判断是否是最后一次重试（代码写的比较垃）这里应该把这个逻辑抽成函数 func IsLastRetry(RetryCount，MaxRetry)  , 否则代码太散，徒增理解成本
			maxRetry := job.Spec.MaxRetry
			lastRetry := false
			if job.Status.RetryCount >= maxRetry-1 {
				lastRetry = true
			}

			// Only retain the Failed and Succeeded pods at the last retry.
			// If it is not the last retry, kill pod as defined in `podRetainPhase`.
			// 这里最后一次 retry，保留完成的 pod（succeed/failed），job 状态也会被置为 failed（在 restartingState.UpdateStatus 里）
			retainPhase := podRetainPhase
			if lastRetry {
				retainPhase = state.PodRetainPhaseSoft
			}
			_, retain := retainPhase[pod.Status.Phase]

			if !retain {
				// 调用 kubeclient 删除 pod
				err := cc.deleteJobPod(job.Name, pod)
				if err == nil {
					terminating++
					// 这种长逻辑，用 return 比用 continue 好，最好单独写个函数
					continue
				}
				// record the err, and then collect the pod info like retained pod
				errs = append(errs, err)
				// 加入错误处理队列，稍后重新处理。这是一个异步操作，这里只是加入了队列，这个函数名容易误导为立即resync
				cc.resyncTask(pod)
			}

			// 计算该 pod 对应 job 对应的 各个pod状态的 num。
			// 这个函数名又长又不明确，真拉。要么直接 addPodStatusNum 完事了，add才是重要的，classify就是一个没啥意义的单词，重要的是 classify 之后做了什么
			// 这里各个状态的num变量也太多了，我会写成:
			// podStatusStat := PodStatusStat{pending: 0, running: 0, ...}
			// podStatusStat.Add(pod.status.state.phase)
			classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
			// 每个pod 的 annotation 上会挂一个 volcano.sh/task-spec, 这里就是计算该 pod 对应 task 对应的 各个pod状态的 num。
			// again：函数名太傻逼了
			calcPodStatus(pod, taskStatusCount)
		}
	}

	if len(errs) != 0 {
		klog.Errorf("failed to kill pods for job %s/%s, with err %+v", job.Namespace, job.Name, errs)
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", errs))
		return fmt.Errorf("failed to kill %d pods of %d", len(errs), total)
	}

	job = job.DeepCopy()
	// Job version is bumped only when job is killed
	// bump version: 将版本号递增到一个新的、唯一的值
	job.Status.Version++
	job.Status.Pending = pending
	job.Status.Running = running
	job.Status.Succeeded = succeeded
	job.Status.Failed = failed
	job.Status.Terminating = terminating
	job.Status.Unknown = unknown
	job.Status.TaskStatusCount = taskStatusCount

	// Update running duration
	// 这里应该是保存上一次运行的时长（因为这里 killjob了）
	klog.V(3).Infof("Running duration is %s", metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}.ToUnstructured())
	job.Status.RunningDuration = &metav1.Duration{Duration: time.Since(jobInfo.Job.CreationTimestamp.Time)}

	if updateStatus != nil {
		// updateStatus 原地更新 job.status, 主要是更新 job.status.state
		if updateStatus(&job.Status) {
			job.Status.State.LastTransitionTime = metav1.Now()
			jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
			job.Status.Conditions = append(job.Status.Conditions, jobCondition)
		}
	}

	// must be called before update job status
	if err := cc.pluginOnJobDelete(job); err != nil {
		return err
	}

	// Update Job status
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("KillJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	// Delete PodGroup
	// 这个函数是在 kill job，所以把 pg 也 kill 了
	pgName := job.Name + "-" + string(job.UID)
	if err := cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Delete(context.TODO(), pgName, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to delete PodGroup of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
	}

	// NOTE(k82cn): DO NOT delete input/output until job is deleted.

	return nil
}

func (cc *jobcontroller) initiateJob(job *batch.Job) (*batch.Job, error) {
	klog.V(3).Infof("Starting to initiate Job <%s/%s>", job.Namespace, job.Name)
	// 如果 phase 为空，初始化 phase=pending
	jobInstance, err := cc.initJobStatus(job)
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.JobStatusError),
			fmt.Sprintf("Failed to initialize job status, err: %v", err))
		return nil, err
	}

	// onCreateJobHook
	if err := cc.pluginOnJobAdd(jobInstance); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError),
			fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return nil, err
	}

	// 这里猜测函数命名是 create job if and only if not exist 的意思，只能是这个committer真菜，下次记得用iff
	// 猜错了，这里是 create Job IO IfNotExist， 里面实际是在做创建 pvc 的事情，真 sb, 命名成 createPVC 会死吗?
	// IfNotExist 在命名中完全没必要说明，加个注释说明一下这是一个可重入的函数就行了
	newJob, err := cc.createJobIOIfNotExist(jobInstance) // 创建 PVC
	if err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PVCError),
			fmt.Sprintf("Failed to create PVC, err: %v", err))
		return nil, err
	}

	if err := cc.createOrUpdatePodGroup(newJob); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError),
			fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return nil, err
	}

	return newJob, nil
}

func (cc *jobcontroller) initOnJobUpdate(job *batch.Job) error {
	klog.V(3).Infof("Starting to initiate Job <%s/%s> on update", job.Namespace, job.Name)

	if err := cc.pluginOnJobUpdate(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PluginError),
			fmt.Sprintf("Execute plugin when job add failed, err: %v", err))
		return err
	}

	if err := cc.createOrUpdatePodGroup(job); err != nil {
		cc.recorder.Event(job, v1.EventTypeWarning, string(batch.PodGroupError),
			fmt.Sprintf("Failed to create PodGroup, err: %v", err))
		return err
	}

	return nil
}

func (cc *jobcontroller) GetQueueInfo(queue string) (*scheduling.Queue, error) {
	queueInfo, err := cc.queueLister.Get(queue)
	if err != nil {
		klog.Errorf("Failed to get queue from queueLister, error: %s", err.Error())
	}

	return queueInfo, err
}

func (cc *jobcontroller) syncJob(jobInfo *apis.JobInfo, updateStatus state.UpdateStatusFn) error {
	job := jobInfo.Job
	klog.V(3).Infof("Starting to sync up Job <%s/%s>, current version %d", job.Namespace, job.Name, job.Status.Version)
	defer klog.V(3).Infof("Finished Job <%s/%s> sync up, current version %d", job.Namespace, job.Name, job.Status.Version)

	if jobInfo.Job.DeletionTimestamp != nil {
		klog.Infof("Job <%s/%s> is terminating, skip management process.",
			jobInfo.Job.Namespace, jobInfo.Job.Name)
		return nil
	}

	// deep copy job to prevent mutate it
	job = job.DeepCopy()

	// Find queue that job belongs to, and check if the queue has forwarding metadata
	queueInfo, err := cc.GetQueueInfo(job.Spec.Queue)
	if err != nil {
		return err
	}

	var jobForwarding bool
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		if len(job.Annotations) == 0 {
			job.Annotations = make(map[string]string)
		}
		job.Annotations[batch.JobForwardingKey] = "true"
		job, err = cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	// Skip job initiation if job is already initiated
	// 注意一个没有初始化的 job，其 state.phase = "" 空
	if !isInitiated(job) {
		// 创建pvc/pg
		if job, err = cc.initiateJob(job); err != nil {
			return err
		}
	} else {
		// TODO: optimize this call it only when scale up/down
		if err = cc.initOnJobUpdate(job); err != nil {
			return err
		}
	}

	// 这是啥？看着像merge冲突了
	if len(queueInfo.Spec.ExtendClusters) != 0 {
		jobForwarding = true
		job.Annotations[batch.JobForwardingKey] = "true"
		_, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("failed to update job: %s/%s, error: %s", job.Namespace, job.Name, err.Error())
			return err
		}
	}

	var syncTask bool
	pgName := job.Name + "-" + string(job.UID)
	if pg, _ := cc.pgLister.PodGroups(job.Namespace).Get(pgName); pg != nil {
		if pg.Status.Phase != "" && pg.Status.Phase != scheduling.PodGroupPending {
			// pg 已经启动，这时候已经有对应的 pod 被创建出来了，需要同步这些 pod 列表
			syncTask = true
		}

		for _, condition := range pg.Status.Conditions {
			if condition.Type == scheduling.PodGroupUnschedulableType {
				cc.recorder.Eventf(job, v1.EventTypeWarning, string(batch.PodGroupPending),
					fmt.Sprintf("PodGroup %s:%s unschedule,reason: %s", job.Namespace, job.Name, condition.Message))
			}
		}
	}

	var jobCondition batch.JobCondition
	// 如果不 sync task, 简单更新下 job 的状态后退出（如果有传入状态更新函数的话）
	if !syncTask {
		if updateStatus != nil {
			if updateStatus(&job.Status) {
				job.Status.State.LastTransitionTime = metav1.Now()
				jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
				job.Status.Conditions = append(job.Status.Conditions, jobCondition)
			}
		}
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update status of Job %v/%v: %v",
				job.Namespace, job.Name, err)
			return err
		}
		if e := cc.cache.Update(newJob); e != nil {
			klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
				newJob.Namespace, newJob.Name, e)
			return e
		}
		return nil
	}

	// 下面是更新 task 的逻辑
	var running, pending, terminating, succeeded, failed, unknown int32
	taskStatusCount := make(map[string]batch.TaskState)

	podToCreate := make(map[string][]*v1.Pod)
	var podToDelete []*v1.Pod
	var creationErrs []error
	var deletionErrs []error
	appendMutex := sync.Mutex{}

	appendError := func(container *[]error, err error) {
		appendMutex.Lock()
		defer appendMutex.Unlock()
		*container = append(*container, err)
	}

	waitCreationGroup := sync.WaitGroup{}

	for _, ts := range job.Spec.Tasks {
		ts.Template.Name = ts.Name
		tc := ts.Template.DeepCopy()
		name := ts.Template.Name

		pods, found := jobInfo.Pods[name]
		if !found {
			pods = map[string]*v1.Pod{}
		}

		var podToCreateEachTask []*v1.Pod // 这里完全没有必要命名成 EachTask
		for i := 0; i < int(ts.Replicas); i++ {
			podName := fmt.Sprintf(jobhelpers.PodNameFmt, job.Name, name, i) // use MakePodName
			if pod, found := pods[podName]; !found {
				// 如果pod 不存在，就创建新的 pod，包括job  pending阶段的创建 pod，和pod running阶段 pod 被驱逐后的重建
				newPod := createJobPod(job, tc, ts.TopologyPolicy, i, jobForwarding)
				if err := cc.pluginOnPodCreate(job, newPod); err != nil {
					return err
				}
				podToCreateEachTask = append(podToCreateEachTask, newPod)
				waitCreationGroup.Add(1) // 蠢货，不在 goroutine 创建时add，在这里 add
			} else {
				// 这里 delete，代表不想把podName加到 #L401 的 podToDelete里面
				delete(pods, podName)
				if pod.DeletionTimestamp != nil {
					klog.Infof("Pod <%s/%s> is terminating", pod.Namespace, pod.Name)
					atomic.AddInt32(&terminating, 1)
					continue
				}

				classifyAndAddUpPodBaseOnPhase(pod, &pending, &running, &succeeded, &failed, &unknown)
				calcPodStatus(pod, taskStatusCount)
			}
		}
		// 此时，pods 里的 pod 都是待删除的（因为不在 spec 里）
		podToCreate[ts.Name] = podToCreateEachTask
		for _, pod := range pods {
			// 删除多的实例
			podToDelete = append(podToDelete, pod)
		}
	}

	for taskName, podToCreateEachTask := range podToCreate {
		if len(podToCreateEachTask) == 0 {
			continue
		}
		go func(taskName string, podToCreateEachTask []*v1.Pod) {
			// 很蠢，这里其实就是要一个 task_name->task 的映射而已，可以在前面构建好，而不是在这里再搞个什么 taskIndex
			taskIndex := jobhelpers.GetTaskIndexUnderJob(taskName, job)
			if job.Spec.Tasks[taskIndex].DependsOn != nil {
				if !cc.waitDependsOnTaskMeetCondition(taskName, taskIndex, podToCreateEachTask, job) {
					klog.V(3).Infof("Job %s/%s depends on task not ready", job.Name, job.Namespace)
					// release wait group
					for _, pod := range podToCreateEachTask {
						go func(pod *v1.Pod) {
							defer waitCreationGroup.Done() // 这尼玛是什么？估计是之前这里有逻辑，现在没了。但是，既然删除了逻辑，就应该把这里一起改下。
						}(pod)
					}
					// 直接 return，不进入后面的逻辑了
					return
				}
			}

			for _, pod := range podToCreateEachTask {
				go func(pod *v1.Pod) {
					defer waitCreationGroup.Done()
					newPod, err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
					if err != nil && !apierrors.IsAlreadyExists(err) {
						// Failed to create Pod, waitCreationGroup a moment and then create it again
						// This is to ensure all podsMap under the same Job created
						// So gang-scheduling could schedule the Job successfully
						klog.Errorf("Failed to create pod %s for Job %s, err %#v",
							pod.Name, job.Name, err)
						appendError(&creationErrs, fmt.Errorf("failed to create pod %s, err: %#v", pod.Name, err))
					} else {
						classifyAndAddUpPodBaseOnPhase(newPod, &pending, &running, &succeeded, &failed, &unknown)
						calcPodStatus(pod, taskStatusCount)
						klog.V(5).Infof("Created Task <%s> of Job <%s/%s>",
							pod.Name, job.Namespace, job.Name)
					}
				}(pod)
			}
		}(taskName, podToCreateEachTask)
	}

	waitCreationGroup.Wait()

	if len(creationErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedCreatePodReason,
			fmt.Sprintf("Error creating pods: %+v", creationErrs))
		return fmt.Errorf("failed to create %d pods of %d", len(creationErrs), len(podToCreate))
	}

	// Delete pods when scale down.
	waitDeletionGroup := sync.WaitGroup{}
	waitDeletionGroup.Add(len(podToDelete))
	for _, pod := range podToDelete {
		go func(pod *v1.Pod) {
			defer waitDeletionGroup.Done()
			err := cc.deleteJobPod(job.Name, pod)
			if err != nil {
				// Failed to delete Pod, waitCreationGroup a moment and then create it again
				// This is to ensure all podsMap under the same Job created
				// So gang-scheduling could schedule the Job successfully
				klog.Errorf("Failed to delete pod %s for Job %s, err %#v",
					pod.Name, job.Name, err)
				appendError(&deletionErrs, err)
				cc.resyncTask(pod)
			} else {
				klog.V(3).Infof("Deleted Task <%s> of Job <%s/%s>",
					pod.Name, job.Namespace, job.Name)
				atomic.AddInt32(&terminating, 1)
			}
		}(pod)
	}
	waitDeletionGroup.Wait()

	if len(deletionErrs) != 0 {
		cc.recorder.Event(job, v1.EventTypeWarning, FailedDeletePodReason,
			fmt.Sprintf("Error deleting pods: %+v", deletionErrs))
		return fmt.Errorf("failed to delete %d pods of %d", len(deletionErrs), len(podToDelete))
	}
	job.Status = batch.JobStatus{
		State: job.Status.State,

		Pending:             pending,
		Running:             running,
		Succeeded:           succeeded,
		Failed:              failed,
		Terminating:         terminating,
		Unknown:             unknown,
		Version:             job.Status.Version,
		MinAvailable:        job.Spec.MinAvailable,
		TaskStatusCount:     taskStatusCount,
		ControlledResources: job.Status.ControlledResources,
		Conditions:          job.Status.Conditions,
		RetryCount:          job.Status.RetryCount,
	}

	if updateStatus != nil && updateStatus(&job.Status) {
		job.Status.State.LastTransitionTime = metav1.Now()
		jobCondition = newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
		job.Status.Conditions = append(job.Status.Conditions, jobCondition)
	}
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return err
	}
	if e := cc.cache.Update(newJob); e != nil {
		klog.Errorf("SyncJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, e)
		return e
	}

	return nil
}

func (cc *jobcontroller) waitDependsOnTaskMeetCondition(taskName string, taskIndex int, podToCreateEachTask []*v1.Pod, job *batch.Job) bool {
	if job.Spec.Tasks[taskIndex].DependsOn == nil {
		return true
	}
	dependsOn := *job.Spec.Tasks[taskIndex].DependsOn
	// 这里是要支持对依赖的 all / any , 写法太蠢了，直接就不支持其他的谓词predicate了
	if len(dependsOn.Name) > 1 && dependsOn.Iteration == batch.IterationAny {
		// any ready to create task, return true
		for _, task := range dependsOn.Name {
			// 检查前置任务是否 running
			if cc.isDependsOnPodsReady(task, job) {
				return true
			}
		}
		// all not ready to skip create task, return false
		return false
	}
	for _, dependsOnTask := range dependsOn.Name {
		// any not ready to skip create task, return false
		// 检查前置任务是否 running
		if !cc.isDependsOnPodsReady(dependsOnTask, job) {
			return false
		}
	}
	// all ready to create task, return true
	return true
}

// 检查依赖的任务的 pods的 的 running 数是否已经超过其 minAvail
func (cc *jobcontroller) isDependsOnPodsReady(task string, job *batch.Job) bool {
	dependsOnPods := jobhelpers.GetPodsNameUnderTask(task, job)
	dependsOnTaskIndex := jobhelpers.GetTaskIndexUnderJob(task, job)
	runningPodCount := 0
	for _, podName := range dependsOnPods {
		pod, err := cc.podLister.Pods(job.Namespace).Get(podName)
		if err != nil {
			// If pod is not found. There are 2 possibilities.
			// 1. vcjob has been deleted. This function should return true.
			// 2. pod is not created. This function should return false, continue waiting.
			if apierrors.IsNotFound(err) {
				_, errGetJob := cc.jobLister.Jobs(job.Namespace).Get(job.Name)
				if errGetJob != nil {
					return apierrors.IsNotFound(errGetJob)
				}
			}
			klog.Errorf("Failed to get pod %v/%v %v", job.Namespace, podName, err)
			continue
		}

		if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodSucceeded {
			klog.V(5).Infof("Sequential state, pod %v/%v of depends on tasks is not running", pod.Namespace, pod.Name)
			continue
		}

		allContainerReady := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if !containerStatus.Ready {
				allContainerReady = false
				break
			}
		}
		if allContainerReady {
			runningPodCount++
		}
	}
	dependsOnTaskMinReplicas := job.Spec.Tasks[dependsOnTaskIndex].MinAvailable
	if dependsOnTaskMinReplicas != nil {
		if runningPodCount < int(*dependsOnTaskMinReplicas) {
			klog.V(5).Infof("In a depends on startup state, there are already %d pods running, which is less than the minimum number of runs", runningPodCount)
			return false
		}
	}
	return true
}

func (cc *jobcontroller) createJobIOIfNotExist(job *batch.Job) (*batch.Job, error) {
	// If PVC does not exist, create them for Job.
	var needUpdate bool
	if job.Status.ControlledResources == nil {
		job.Status.ControlledResources = make(map[string]string)
	}
	for index, volume := range job.Spec.Volumes {
		vcName := volume.VolumeClaimName
		// 这里的意思是用户没有自己给volumn 一个名字，所以我们自己生成名字
		if len(vcName) == 0 {
			// NOTE(k82cn): Ensure never have duplicated generated names.
			// 又是一个 sb 写法，这里的 for 的意思是尝试随机生成一个 pvc 的名字，如果名字存在，就重新生成
			// 这种脏逻辑麻烦写在函数里，别放在主流程
			// 另外最好搞成不可能冲突的，搞个毫秒时间戳+随机数，咋可能冲突，就可以丢掉这个脏逻辑了
			for {
				vcName = jobhelpers.GenPVCName(job.Name)
				exist, err := cc.checkPVCExist(job, vcName)
				if err != nil {
					return job, err
				}
				if exist {
					continue
				}
				job.Spec.Volumes[index].VolumeClaimName = vcName
				needUpdate = true
				break
			}
			// TODO: check VolumeClaim must be set if VolumeClaimName is empty
			if volume.VolumeClaim != nil {
				// 调用 kubeclient 创建 pvc, 并设置 ownerRef
				if err := cc.createPVC(job, vcName, volume.VolumeClaim); err != nil {
					return job, err
				}
			}
		} else {
			exist, err := cc.checkPVCExist(job, vcName)
			if err != nil {
				return job, err
			}
			if !exist {
				return job, fmt.Errorf("pvc %s is not found, the job will be in the Pending state until the PVC is created", vcName)
			}
		}
		job.Status.ControlledResources["volume-pvc-"+vcName] = vcName
		// 这里应该少了一个 needUpdate = true
	}
	if needUpdate {
		newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).Update(context.TODO(), job, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Failed to update Job %v/%v for volume claim name: %v ",
				job.Namespace, job.Name, err)
			return job, err
		}

		newJob.Status = job.Status
		return newJob, err
	}
	return job, nil
}

func (cc *jobcontroller) checkPVCExist(job *batch.Job, pvc string) (bool, error) {
	if _, err := cc.pvcLister.PersistentVolumeClaims(job.Namespace).Get(pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		klog.V(3).Infof("Failed to get PVC %s for job <%s/%s>: %v",
			pvc, job.Namespace, job.Name, err)
		return false, err
	}
	return true, nil
}

func (cc *jobcontroller) createPVC(job *batch.Job, vcName string, volumeClaim *v1.PersistentVolumeClaimSpec) error {
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      vcName,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, helpers.JobKind),
			},
		},
		Spec: *volumeClaim,
	}

	klog.V(3).Infof("Try to create PVC: %v", pvc)

	if _, e := cc.kubeClient.CoreV1().PersistentVolumeClaims(job.Namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); e != nil {
		klog.V(3).Infof("Failed to create PVC for Job <%s/%s>: %v",
			job.Namespace, job.Name, e)
		return e
	}
	return nil
}

func (cc *jobcontroller) createOrUpdatePodGroup(job *batch.Job) error {
	// If PodGroup does not exist, create one for Job.
	pgName := job.Name + "-" + string(job.UID)
	var pg *scheduling.PodGroup
	var err error
	pg, err = cc.pgLister.PodGroups(job.Namespace).Get(pgName)
	// 这里的处理很蠢，创建和更新的逻辑直接裸写在这个函数里，if err!=nil 里面是创建，外面是更新
	if err != nil {
		if !apierrors.IsNotFound(err) {
			klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v",
				job.Namespace, job.Name, err)
			return err
		}
		// try to get old pg if new pg not exist
		pg, err = cc.pgLister.PodGroups(job.Namespace).Get(job.Name)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to get PodGroup for Job <%s/%s>: %v",
					job.Namespace, job.Name, err)
				return err
			}

			minTaskMember := map[string]int32{}
			for _, task := range job.Spec.Tasks {
				if task.MinAvailable != nil {
					minTaskMember[task.Name] = *task.MinAvailable
				} else {
					minTaskMember[task.Name] = task.Replicas
				}
			}

			pg := &scheduling.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: job.Namespace,
					// add job.UID into its name when create new PodGroup
					Name:        pgName,
					Annotations: job.Annotations,
					Labels:      job.Labels,
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(job, helpers.JobKind),
					},
				},
				Spec: scheduling.PodGroupSpec{
					MinMember:         job.Spec.MinAvailable,
					MinTaskMember:     minTaskMember,
					Queue:             job.Spec.Queue,
					MinResources:      cc.calcPGMinResources(job),
					PriorityClassName: job.Spec.PriorityClassName,
				},
			}

			if _, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Create(context.TODO(), pg, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					klog.Errorf("Failed to create PodGroup for Job <%s/%s>: %v",
						job.Namespace, job.Name, err)
					return err
				}
			}
			return nil
		}
	}

	// 这里对pgShouldUpdate的处理虽然有点丑，但是胜在简单易懂，问题也不大
	pgShouldUpdate := false
	if pg.Spec.PriorityClassName != job.Spec.PriorityClassName {
		pg.Spec.PriorityClassName = job.Spec.PriorityClassName
		pgShouldUpdate = true
	}

	minResources := cc.calcPGMinResources(job)
	if pg.Spec.MinMember != job.Spec.MinAvailable || !reflect.DeepEqual(pg.Spec.MinResources, minResources) {
		pg.Spec.MinMember = job.Spec.MinAvailable
		pg.Spec.MinResources = minResources
		pgShouldUpdate = true
	}

	if pg.Spec.MinTaskMember == nil {
		pgShouldUpdate = true
		pg.Spec.MinTaskMember = make(map[string]int32)
	}

	for _, task := range job.Spec.Tasks {
		cnt := task.Replicas
		if task.MinAvailable != nil {
			cnt = *task.MinAvailable
		}

		if taskMember, ok := pg.Spec.MinTaskMember[task.Name]; !ok {
			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		} else {
			if taskMember == cnt {
				continue
			}

			pgShouldUpdate = true
			pg.Spec.MinTaskMember[task.Name] = cnt
		}
	}

	if !pgShouldUpdate {
		return nil
	}

	_, err = cc.vcClient.SchedulingV1beta1().PodGroups(job.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
	if err != nil {
		klog.V(3).Infof("Failed to update PodGroup for Job <%s/%s>: %v",
			job.Namespace, job.Name, err)
	}
	return err
}

func (cc *jobcontroller) deleteJobPod(jobName string, pod *v1.Pod) error {
	err := cc.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		klog.Errorf("Failed to delete pod %s/%s for Job %s, err %#v",
			pod.Namespace, pod.Name, jobName, err)

		return fmt.Errorf("failed to delete pod %s, err %#v", pod.Name, err)
	}

	return nil
}

func (cc *jobcontroller) calcPGMinResources(job *batch.Job) *v1.ResourceList {
	// sort task by priorityClasses
	var tasksPriority TasksPriority
	for _, task := range job.Spec.Tasks {
		tp := TaskPriority{0, task}
		pc := task.Template.Spec.PriorityClassName

		if pc != "" {
			priorityClass, err := cc.pcLister.Get(pc)
			if err != nil || priorityClass == nil {
				klog.Warningf("Ignore task %s priority class %s: %v", task.Name, pc, err)
			} else {
				tp.priority = priorityClass.Value
			}
		}
		tasksPriority = append(tasksPriority, tp)
	}

	sort.Sort(tasksPriority)

	minReq := v1.ResourceList{}
	podCnt := int32(0)
	// 按优先级统计资源，先统计高优先级
	// MinAvailable 的 pod 拉起顺序也是按优先级排，先拉起前面的MinAvailable个 pod，所以这里就统计了前MinAvailable个 pod 的资源
	for _, task := range tasksPriority {
		for i := int32(0); i < task.Replicas; i++ {
			if podCnt >= job.Spec.MinAvailable {
				break // 这里其实是 break 了所有循环，这个写法会有点费解，直接 return 比较好
			}

			podCnt++
			pod := &v1.Pod{
				Spec: task.Template.Spec,
			}
			minReq = quotav1.Add(minReq, *util.GetPodQuotaUsage(pod))
		}
	}

	return &minReq
}

func (cc *jobcontroller) initJobStatus(job *batch.Job) (*batch.Job, error) {
	if job.Status.State.Phase != "" {
		return job, nil
	}

	job.Status.State.Phase = batch.Pending
	job.Status.State.LastTransitionTime = metav1.Now()
	job.Status.MinAvailable = job.Spec.MinAvailable
	jobCondition := newCondition(job.Status.State.Phase, &job.Status.State.LastTransitionTime)
	job.Status.Conditions = append(job.Status.Conditions, jobCondition)
	newJob, err := cc.vcClient.BatchV1alpha1().Jobs(job.Namespace).UpdateStatus(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update status of Job %v/%v: %v",
			job.Namespace, job.Name, err)
		return nil, err
	}
	if err := cc.cache.Update(newJob); err != nil {
		klog.Errorf("CreateJob - Failed to update Job %v/%v in cache:  %v",
			newJob.Namespace, newJob.Name, err)
		return nil, err
	}

	return newJob, nil
}

// 计算该 pod 对应 job 对应的 各个pod状态的 num。
func classifyAndAddUpPodBaseOnPhase(pod *v1.Pod, pending, running, succeeded, failed, unknown *int32) {
	switch pod.Status.Phase {
	case v1.PodPending:
		atomic.AddInt32(pending, 1)
	case v1.PodRunning:
		atomic.AddInt32(running, 1)
	case v1.PodSucceeded:
		atomic.AddInt32(succeeded, 1)
	case v1.PodFailed:
		atomic.AddInt32(failed, 1)
	default:
		atomic.AddInt32(unknown, 1)
	}
}

// 每个pod 的 annotation 上会挂一个 volcano.sh/task-spec, 这里就是计算该 pod 对应 task 对应的 各个pod状态的 num。
// 函数名太傻逼了
func calcPodStatus(pod *v1.Pod, taskStatusCount map[string]batch.TaskState) {
	taskName, found := pod.Annotations[batch.TaskSpecKey]
	if !found {
		return
	}

	calMutex.Lock()
	defer calMutex.Unlock()
	if _, ok := taskStatusCount[taskName]; !ok {
		taskStatusCount[taskName] = batch.TaskState{
			Phase: make(map[v1.PodPhase]int32),
		}
	}

	switch pod.Status.Phase {
	case v1.PodPending:
		taskStatusCount[taskName].Phase[v1.PodPending]++
	case v1.PodRunning:
		taskStatusCount[taskName].Phase[v1.PodRunning]++
	case v1.PodSucceeded:
		taskStatusCount[taskName].Phase[v1.PodSucceeded]++
	case v1.PodFailed:
		taskStatusCount[taskName].Phase[v1.PodFailed]++
	default:
		taskStatusCount[taskName].Phase[v1.PodUnknown]++
	}
}

func isInitiated(job *batch.Job) bool {
	if job.Status.State.Phase == "" || job.Status.State.Phase == batch.Pending {
		return false
	}

	return true
}

func newCondition(status batch.JobPhase, lastTransitionTime *metav1.Time) batch.JobCondition {
	return batch.JobCondition{
		Status:             status,
		LastTransitionTime: lastTransitionTime,
	}
}
