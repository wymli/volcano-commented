/*
Copyright 2017 The Volcano Authors.

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

package state

import (
	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/bus/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/apis"
)

type pendingState struct {
	job *apis.JobInfo
}

func (ps *pendingState) Execute(action v1alpha1.Action) error {
	switch action {
	case v1alpha1.RestartJobAction:
		// 杀掉所有 pod，将 job 状态置为 Restarting
		return KillJob(ps.job, PodRetainPhaseNone, func(status *vcbatch.JobStatus) bool {
			status.RetryCount++
			status.State.Phase = vcbatch.Restarting
			return true
		})

	case v1alpha1.AbortJobAction:
		// 杀掉除了 Succeeded 和 Failed 的 pod，将 job 状态置为 Aborting
		return KillJob(ps.job, PodRetainPhaseSoft, func(status *vcbatch.JobStatus) bool {
			status.State.Phase = vcbatch.Aborting
			return true
		})
	case v1alpha1.CompleteJobAction:
		// 杀掉除了 Succeeded 和 Failed 的 pod，将 job 状态置为 Completing
		return KillJob(ps.job, PodRetainPhaseSoft, func(status *vcbatch.JobStatus) bool {
			status.State.Phase = vcbatch.Completing
			return true
		})
	case v1alpha1.TerminateJobAction:
		// 杀掉除了 Succeeded 和 Failed 的 pod，将 job 状态置为 Terminating
		return KillJob(ps.job, PodRetainPhaseSoft, func(status *vcbatch.JobStatus) bool {
			status.State.Phase = vcbatch.Terminating
			return true
		})
	default:
		return SyncJob(ps.job, func(status *vcbatch.JobStatus) bool {
			// 如果拉起的 pod >= MinAvailable 了 (由于事件有延迟，有些 pod 可能已经成功/失败了)，将 job 状态置为 Running
			if ps.job.Job.Spec.MinAvailable <= status.Running+status.Succeeded+status.Failed {
				status.State.Phase = vcbatch.Running
				return true
			}
			return false
		})
	}
}
