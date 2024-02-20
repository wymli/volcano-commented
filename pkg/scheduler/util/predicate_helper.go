package util

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
)

type PredicateHelper interface {
	PredicateNodes(task *api.TaskInfo, nodes []*api.NodeInfo, fn api.PredicateFn, enableErrorCache bool) ([]*api.NodeInfo, *api.FitErrors)
}

type predicateHelper struct {
	taskPredicateErrorCache map[string]map[string]error
}

// PredicateNodes returns the specified number of nodes that fit a task
func (ph *predicateHelper) PredicateNodes(task *api.TaskInfo, nodes []*api.NodeInfo, fn api.PredicateFn, enableErrorCache bool) ([]*api.NodeInfo, *api.FitErrors) {
	var errorLock sync.RWMutex
	fe := api.NewFitErrors()

	// 牛逼，allNodes 是个数量，写成 numNodes 会死？
	allNodes := len(nodes)
	if allNodes == 0 {
		return make([]*api.NodeInfo, 0), fe
	}

	// numNodesToFind 表示期望的通过 predicate 后的候选节点的最大数量，如果候选节点过大，丢弃掉
	numNodesToFind := CalculateNumOfFeasibleNodesToFind(int32(allNodes))

	// 这个注释🐂，当别人小学生呢
	//allocate enough space to avoid growing it
	predicateNodes := make([]*api.NodeInfo, numNodesToFind)

	numFoundNodes := int32(0)
	processedNodes := int32(0)

	taskGroupid := taskGroupID(task)
	nodeErrorCache, taskFailedBefore := ph.taskPredicateErrorCache[taskGroupid]
	if nodeErrorCache == nil {
		nodeErrorCache = map[string]error{}
	}

	//create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	checkNode := func(index int) {
		// Check the nodes starting from where is left off in the previous scheduling cycle,
		// to make sure all nodes have the same chance of being examined across pods.

		// 牛逼，lastProcessedNodeIndex 是个全局变量，作者的意思应该是想尽可能地遍历所有节点，下次分配从上次分配结束的节点开始
		node := nodes[(lastProcessedNodeIndex+index)%allNodes]
		atomic.AddInt32(&processedNodes, 1)
		klog.V(4).Infof("Considering Task <%v/%v> on node <%v>: <%v> vs. <%v>",
			task.Namespace, task.Name, node.Name, task.Resreq, node.Idle)

		// Check if the task had "predicate" failure before.
		// And then check if the task failed to predict on this node before.
		if enableErrorCache && taskFailedBefore {
			errorLock.RLock()
			errC, ok := nodeErrorCache[node.Name]
			errorLock.RUnlock()

			if ok {
				errorLock.Lock()
				fe.SetNodeError(node.Name, errC)
				errorLock.Unlock()
				return
			}
		}

		// TODO (k82cn): Enable eCache for performance improvement.
		if _, err := fn(task, node); err != nil {
			klog.V(3).Infof("Predicates failed: %v", err)
			errorLock.Lock()
			nodeErrorCache[node.Name] = err
			ph.taskPredicateErrorCache[taskGroupid] = nodeErrorCache
			fe.SetNodeError(node.Name, err)
			errorLock.Unlock()
			return
		}

		//check if the number of found nodes is more than the numNodesTofind
		// 这里是在并发安全的增加 idx，很蠢，直接 mutx+append 就行了
		length := atomic.AddInt32(&numFoundNodes, 1)
		if length > numNodesToFind {
			cancel()
			atomic.AddInt32(&numFoundNodes, -1) // 又加又减的，🐂🍺
		} else {
			// 如果没有什么特殊原因，不要用 length-1 这种东西，该是啥就是啥，这里就应该是 predicateNodes[i] = node
			predicateNodes[length-1] = node
		}
	}

	//workqueue.ParallelizeUntil(context.TODO(), 16, len(nodes), checkNode)
	workqueue.ParallelizeUntil(ctx, 16, allNodes, checkNode)

	//processedNodes := int(numFoundNodes) + len(filteredNodesStatuses) + len(failedPredicateMap)
	lastProcessedNodeIndex = (lastProcessedNodeIndex + int(processedNodes)) % allNodes
	predicateNodes = predicateNodes[:numFoundNodes] // numFoundNodes 可能不足 capacity, 再截断一下
	return predicateNodes, fe
}

func taskGroupID(task *api.TaskInfo) string {
	return fmt.Sprintf("%s/%s", task.Job, task.GetTaskSpecKey())
}

func NewPredicateHelper() PredicateHelper {
	return &predicateHelper{taskPredicateErrorCache: map[string]map[string]error{}}
}

type StatusSets []*api.Status

func (s StatusSets) ContainsUnschedulable() bool {
	for _, status := range s {
		if status == nil {
			continue
		}
		if status.Code == api.Unschedulable {
			return true
		}
	}
	return false
}

func (s StatusSets) ContainsUnschedulableAndUnresolvable() bool {
	for _, status := range s {
		if status == nil {
			continue
		}
		if status.Code == api.UnschedulableAndUnresolvable {
			return true
		}
	}
	return false
}

func (s StatusSets) ContainsErrorSkipOrWait() bool {
	for _, status := range s {
		if status == nil {
			continue
		}
		if status.Code == api.Error || status.Code == api.Skip || status.Code == api.Wait {
			return true
		}
	}
	return false
}

// Message return the message generated from StatusSets
func (s StatusSets) Message() string {
	if s == nil {
		return ""
	}
	all := make([]string, 0, len(s))
	for _, status := range s {
		if status.Reason == "" {
			continue
		}
		all = append(all, status.Reason)
	}
	return strings.Join(all, ",")
}

// Reasons return the reasons list
func (s StatusSets) Reasons() []string {
	if s == nil {
		return nil
	}
	all := make([]string, 0, len(s))
	for _, status := range s {
		if status.Reason == "" {
			continue
		}
		all = append(all, status.Reason)
	}
	return all
}
