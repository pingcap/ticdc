// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatchermanager

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/threadpool"
)

// HeartbeatTask is a perioic task to collect the heartbeat status from event dispatcher manager and push to heartbeatRequestQueue
type HeartBeatTask struct {
	taskHandle *threadpool.TaskHandle
	manager    *EventDispatcherManager
	// Used to determine when to collect complete status
	statusTick int
}

func newHeartBeatTask(manager *EventDispatcherManager) *HeartBeatTask {
	taskScheduler := GetHeartBeatTaskScheduler()
	t := &HeartBeatTask{
		manager:    manager,
		statusTick: 0,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *HeartBeatTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}
	executeInterval := time.Millisecond * 200
	// 10s / 200ms = 50
	completeStatusInterval := int(time.Second * 10 / executeInterval)
	t.statusTick++
	needCompleteStatus := (t.statusTick)%completeStatusInterval == 0
	message := t.manager.aggregateDispatcherHeartbeats(needCompleteStatus)
	t.manager.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: t.manager.GetMaintainerID(), Request: message})
	return time.Now().Add(executeInterval)
}

func (t *HeartBeatTask) Cancel() {
	t.taskHandle.Cancel()
}

var (
	heartBeatTaskSchedulerOnce sync.Once
	heartBeatTaskScheduler     threadpool.ThreadPool
)

func GetHeartBeatTaskScheduler() threadpool.ThreadPool {
	heartBeatTaskSchedulerOnce.Do(func() {
		heartBeatTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return heartBeatTaskScheduler
}

func SetHeartBeatTaskScheduler(taskScheduler threadpool.ThreadPool) {
	heartBeatTaskScheduler = taskScheduler
}

var (
	mergeCheckTaskSchedulerOnce sync.Once
	mergeCheckTaskScheduler     threadpool.ThreadPool
)

func GetMergeCheckTaskScheduler() threadpool.ThreadPool {
	mergeCheckTaskSchedulerOnce.Do(func() {
		mergeCheckTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return mergeCheckTaskScheduler
}

func SetMergeCheckTaskScheduler(taskScheduler threadpool.ThreadPool) {
	mergeCheckTaskScheduler = taskScheduler
}

// MergeCheckTask is a task to check the status of the merged dispatcher.
type MergeCheckTask struct {
	taskHandle       *threadpool.TaskHandle
	manager          *EventDispatcherManager
	mergedDispatcher *dispatcher.Dispatcher
	dispatcherIDs    []common.DispatcherID // the ids of dispatchers to be merged
}

func newMergeCheckTask(manager *EventDispatcherManager, mergedDispatcher *dispatcher.Dispatcher, dispatcherIDs []common.DispatcherID) *MergeCheckTask {
	taskScheduler := GetMergeCheckTaskScheduler()
	t := &MergeCheckTask{
		manager:          manager,
		mergedDispatcher: mergedDispatcher,
		dispatcherIDs:    dispatcherIDs,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(time.Second*1))
	return t
}

func (t *MergeCheckTask) Execute() time.Time {
	if t.manager.closed.Load() {
		return time.Time{}
	}

	if t.mergedDispatcher.GetComponentStatus() != heartbeatpb.ComponentState_MergeReady {
		return time.Now().Add(time.Second * 1)
	}

	t.manager.DoMerge(t)
	return time.Now().Add(time.Second * 1)
}

func (t *MergeCheckTask) Cancel() {
	t.taskHandle.Cancel()
}
