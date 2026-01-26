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
	"math"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

// HeartbeatTask is a perioic task to collect the heartbeat status from event dispatcher manager and push to heartbeatRequestQueue
type HeartBeatTask struct {
	taskHandle *threadpool.TaskHandle
	manager    *DispatcherManager
	// Used to determine when to collect complete status
	statusTick int
}

func newHeartBeatTask(manager *DispatcherManager) *HeartBeatTask {
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
	manager          *DispatcherManager
	mergedDispatcher dispatcher.Dispatcher
	dispatcherIDs    []common.DispatcherID // the ids of dispatchers to be merged
}

func newMergeCheckTask(manager *DispatcherManager, mergedDispatcher dispatcher.Dispatcher, dispatcherIDs []common.DispatcherID) *MergeCheckTask {
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

	sinkType := getSinkType(t.mergedDispatcher, t.manager)
	if common.IsRedoMode(t.mergedDispatcher.GetMode()) {
		if needAbort, abortReason := shouldAbortMerge(t, t.manager.redoDispatcherMap); needAbort {
			abortMerge(t, t.manager.redoDispatcherMap, sinkType, abortReason)
			return time.Time{}
		}
	} else {
		if needAbort, abortReason := shouldAbortMerge(t, t.manager.dispatcherMap); needAbort {
			abortMerge(t, t.manager.dispatcherMap, sinkType, abortReason)
			return time.Time{}
		}
	}

	if t.mergedDispatcher.GetComponentStatus() != heartbeatpb.ComponentState_MergeReady {
		return time.Now().Add(time.Second * 1)
	}

	if common.IsRedoMode(t.mergedDispatcher.GetMode()) {
		doMerge(t, t.manager.redoDispatcherMap)
	} else {
		doMerge(t, t.manager.dispatcherMap)
	}
	return time.Now().Add(time.Second * 1)
}

func (t *MergeCheckTask) Cancel() {
	t.taskHandle.Cancel()
}

func shouldAbortMerge[T dispatcher.Dispatcher](t *MergeCheckTask, dispatcherMap *DispatcherMap[T]) (bool, string) {
	mergedID := t.mergedDispatcher.GetId()
	mergedDispatcher, ok := dispatcherMap.Get(mergedID)
	if !ok {
		return true, "merged_dispatcher_missing"
	}
	if mergedDispatcher.GetTryRemoving() || mergedDispatcher.GetRemovingStatus() {
		return true, "merged_dispatcher_removing"
	}

	for _, id := range t.dispatcherIDs {
		dispatcherItem, ok := dispatcherMap.Get(id)
		if !ok {
			return true, "source_dispatcher_missing"
		}
		if dispatcherItem.GetTryRemoving() || dispatcherItem.GetRemovingStatus() {
			return true, "source_dispatcher_removing"
		}
	}
	return false, ""
}

func abortMerge[T dispatcher.Dispatcher](t *MergeCheckTask, dispatcherMap *DispatcherMap[T], sinkType common.SinkType, reason string) {
	log.Info("abort merge",
		zap.Stringer("changefeedID", t.manager.changefeedID),
		zap.String("reason", reason),
		zap.Int64("mode", t.mergedDispatcher.GetMode()),
		zap.Any("dispatcherIDs", t.dispatcherIDs),
		zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
	)

	// Stop retrying and cleanup best-effort.
	t.Cancel()

	eventCollector := appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector)
	memoryQuota := t.manager.sinkQuota
	if common.IsRedoMode(t.mergedDispatcher.GetMode()) {
		memoryQuota = t.manager.redoQuota
	}

	for _, id := range t.dispatcherIDs {
		dispatcherItem, ok := dispatcherMap.Get(id)
		if !ok {
			continue
		}
		if dispatcherItem.GetTryRemoving() || dispatcherItem.GetRemovingStatus() {
			continue
		}
		if dispatcherItem.GetComponentStatus() == heartbeatpb.ComponentState_WaitingMerge {
			dispatcherItem.SetComponentStatus(heartbeatpb.ComponentState_Working)
		}
		// Merge closes and detaches source dispatchers from event collector. If merge aborts,
		// re-register the dispatchers so they can resume receiving events.
		if !eventCollector.HasDispatcher(id) {
			eventCollector.AddDispatcher(dispatcherItem, memoryQuota)
		}
	}

	removeDispatcher(t.manager, t.mergedDispatcher.GetId(), dispatcherMap, sinkType)
}

func doMerge[T dispatcher.Dispatcher](t *MergeCheckTask, dispatcherMap *DispatcherMap[T]) {
	log.Info("do merge",
		zap.Stringer("changefeedID", t.manager.changefeedID),
		zap.Int64("mode", t.mergedDispatcher.GetMode()),
		zap.Any("dispatcherIDs", t.dispatcherIDs),
		zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
	)
	sinkType := getSinkType(t.mergedDispatcher, t.manager)

	// Step1: close all dispatchers to be merged, calculate the min checkpointTs of the merged dispatcher
	minCheckpointTs := uint64(math.MaxUint64)
	closedList := make([]bool, len(t.dispatcherIDs)) // record whether the dispatcher is closed successfully
	pendingStates := make([]*heartbeatpb.State, len(t.dispatcherIDs))
	closedCount := 0
	count := 0
	for closedCount < len(t.dispatcherIDs) {
		if needAbort, abortReason := shouldAbortMerge(t, dispatcherMap); needAbort {
			abortMerge(t, dispatcherMap, sinkType, abortReason)
			return
		}
		for idx, id := range t.dispatcherIDs {
			if closedList[idx] {
				continue
			}
			dispatcher, ok := dispatcherMap.Get(id)
			if !ok {
				break
			}
			if count == 0 {
				appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)
			}

			watermark, ok := dispatcher.TryClose()
			if ok {
				if watermark.CheckpointTs < minCheckpointTs {
					minCheckpointTs = watermark.CheckpointTs
				}
				// Record pending block event status when the source dispatcher is closed successfully.
				// This is used to derive a safe startTs for merged dispatcher when all source dispatchers
				// are waiting for the same block event (DDL or syncpoint).
				pendingStates[idx] = dispatcher.GetBlockEventStatus()
				closedList[idx] = true
				closedCount++
			} else {
				log.Info("dispatcher is still not closed", zap.Stringer("dispatcherID", id))
			}
		}
		time.Sleep(10 * time.Millisecond)
		count += 1
		log.Info("event dispatcher manager is doing merge, waiting for dispatchers to be closed",
			zap.Int("closedCount", closedCount),
			zap.Int("total", len(t.dispatcherIDs)),
			zap.Int("count", count),
			zap.Int64("mode", t.mergedDispatcher.GetMode()),
			zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
		)
	}

	// Step2: resolve startTs and skip flags for the merged dispatcher,
	//        set the pd clock currentTs as the currentPDTs of the merged dispatcher,
	//        change the component status of the merged dispatcher to Initializing
	//        set dispatcher into dispatcherMap and related field
	//        notify eventCollector to update the merged dispatcher startTs
	startTs, skipSyncpointAtStartTs, skipDMLAsStartTs := resolveMergedDispatcherStartTs(t, minCheckpointTs, pendingStates)
	t.mergedDispatcher.SetStartTs(startTs)
	t.mergedDispatcher.SetSkipSyncpointAtStartTs(skipSyncpointAtStartTs)
	t.mergedDispatcher.SetSkipDMLAsStartTs(skipDMLAsStartTs)

	t.mergedDispatcher.SetCurrentPDTs(t.manager.pdClock.CurrentTS())
	t.mergedDispatcher.SetComponentStatus(heartbeatpb.ComponentState_Initializing)
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).CommitAddDispatcher(t.mergedDispatcher, startTs)
	log.Info("merge dispatcher commit",
		zap.Stringer("changefeedID", t.manager.changefeedID),
		zap.Stringer("dispatcherID", t.mergedDispatcher.GetId()),
		zap.Int64("mode", t.mergedDispatcher.GetMode()),
		zap.Any("tableSpan", common.FormatTableSpan(t.mergedDispatcher.GetTableSpan())),
		zap.Uint64("startTs", startTs),
	)

	// Step3: cancel the merge task
	t.Cancel()

	// Step4: remove all the dispatchers to be merged
	// we set dispatcher removing status to true after we set the merged dispatcher into dispatcherMap and change its status to Initializing.
	// so that we can ensure the calculate of checkpointTs of the event dispatcher manager will include the merged dispatcher of the dispatchers to be merged
	// to avoid the fallback of the checkpointTs
	for _, id := range t.dispatcherIDs {
		dispatcher, ok := dispatcherMap.Get(id)
		if !ok {
			log.Warn("dispatcher not found when do merge, skip remove",
				zap.Stringer("dispatcherID", id),
				zap.Stringer("changefeedID", t.manager.changefeedID))
			continue
		}
		dispatcher.Remove()
	}
}

type mergedStartTsCandidate struct {
	startTs                uint64
	skipSyncpointAtStartTs bool
	skipDMLAsStartTs       bool

	allSamePending     bool
	pendingCommitTs    uint64
	pendingIsSyncPoint bool
}

func buildMergedStartTsCandidate(minCheckpointTs uint64, pendingStates []*heartbeatpb.State) mergedStartTsCandidate {
	candidate := mergedStartTsCandidate{
		startTs:                minCheckpointTs,
		skipSyncpointAtStartTs: false,
		skipDMLAsStartTs:       false,
		allSamePending:         true,
	}

	if len(pendingStates) == 0 {
		candidate.allSamePending = false
		return candidate
	}

	var pendingCommitTs uint64
	var pendingIsSyncPoint bool
	for idx, state := range pendingStates {
		if state == nil {
			candidate.allSamePending = false
			break
		}
		if idx == 0 {
			pendingCommitTs = state.BlockTs
			pendingIsSyncPoint = state.IsSyncPoint
			continue
		}
		if state.BlockTs != pendingCommitTs || state.IsSyncPoint != pendingIsSyncPoint {
			candidate.allSamePending = false
			break
		}
	}
	candidate.pendingCommitTs = pendingCommitTs
	candidate.pendingIsSyncPoint = pendingIsSyncPoint

	if candidate.allSamePending {
		if pendingIsSyncPoint {
			candidate.startTs = pendingCommitTs
		} else if pendingCommitTs > 0 {
			candidate.startTs = pendingCommitTs - 1
			candidate.skipDMLAsStartTs = true
		}
	}
	return candidate
}

func mergeMergedStartTsCandidateWithMySQLRecovery(t *MergeCheckTask, candidate mergedStartTsCandidate) (uint64, bool, bool) {
	finalStartTs := candidate.startTs
	finalSkipSyncpointAtStartTs := candidate.skipSyncpointAtStartTs
	finalSkipDMLAsStartTs := candidate.skipDMLAsStartTs

	if !common.IsDefaultMode(t.mergedDispatcher.GetMode()) || t.manager.sink.SinkType() != common.MysqlSinkType {
		return finalStartTs, finalSkipSyncpointAtStartTs, finalSkipDMLAsStartTs
	}

	newStartTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, err := t.manager.sink.(*mysql.Sink).GetTableRecoveryInfo(
		[]int64{t.mergedDispatcher.GetTableSpan().TableID},
		[]int64{int64(candidate.startTs)},
		false,
	)
	if err != nil {
		log.Error("get table recovery info for merge dispatcher failed",
			zap.Stringer("dispatcherID", t.mergedDispatcher.GetId()),
			zap.Stringer("changefeedID", t.manager.changefeedID),
			zap.Error(err),
		)
		t.mergedDispatcher.HandleError(err)
		return finalStartTs, finalSkipSyncpointAtStartTs, finalSkipDMLAsStartTs
	}

	recoveryStartTs := uint64(newStartTsList[0])
	recoverySkipSyncpointAtStartTs := skipSyncpointAtStartTsList[0]
	recoverySkipDMLAsStartTs := skipDMLAsStartTsList[0]
	if recoveryStartTs > candidate.startTs {
		finalStartTs = recoveryStartTs
		finalSkipSyncpointAtStartTs = recoverySkipSyncpointAtStartTs
		finalSkipDMLAsStartTs = recoverySkipDMLAsStartTs
	} else if recoveryStartTs == candidate.startTs {
		finalSkipSyncpointAtStartTs = candidate.skipSyncpointAtStartTs || recoverySkipSyncpointAtStartTs
		finalSkipDMLAsStartTs = candidate.skipDMLAsStartTs || recoverySkipDMLAsStartTs
	}

	log.Info("get table recovery info for merge dispatcher",
		zap.Stringer("changefeedID", t.manager.changefeedID),
		zap.Uint64("mergeStartTsCandidate", candidate.startTs),
		zap.Any("recoveryStartTs", newStartTsList),
		zap.Any("recoverySkipSyncpointAtStartTsList", skipSyncpointAtStartTsList),
		zap.Any("recoverySkipDMLAsStartTsList", skipDMLAsStartTsList),
		zap.Uint64("finalStartTs", finalStartTs),
		zap.Bool("finalSkipSyncpointAtStartTs", finalSkipSyncpointAtStartTs),
		zap.Bool("finalSkipDMLAsStartTs", finalSkipDMLAsStartTs),
	)

	return finalStartTs, finalSkipSyncpointAtStartTs, finalSkipDMLAsStartTs
}

// resolveMergedDispatcherStartTs returns the effective startTs and skip flags for the merged dispatcher.
//
// Inputs:
// - minCheckpointTs: min checkpointTs among all source dispatchers, collected after they are closed.
// - pendingStates: per-source block state from GetBlockEventStatus() captured at close time.
//
// Algorithm:
//  1. Build a merge candidate from minCheckpointTs.
//     If all source dispatchers have a non-nil pending block state and they refer to the same (commitTs, isSyncPoint),
//     adjust the merge candidate so the merged dispatcher can replay that block event safely:
//     - DDL: startTs = commitTs - 1, skipDMLAsStartTs = true.
//     - SyncPoint: startTs = commitTs.
//     The merge candidate always uses skipSyncpointAtStartTs = false.
//  2. If the sink is MySQL, query downstream ddl_ts recovery info using the merge candidate startTs and merge the results:
//     - If recoveryStartTs > mergeStartTsCandidate: use recoveryStartTs and its skip flags.
//     - If recoveryStartTs == mergeStartTsCandidate: OR the skip flags.
//     - If recoveryStartTs < mergeStartTsCandidate: keep the merge candidate.
//     If the query fails, the error is reported via mergedDispatcher.HandleError and the merge candidate is returned.
//
// For non-MySQL and redo, the merge candidate is the final result.
func resolveMergedDispatcherStartTs(t *MergeCheckTask, minCheckpointTs uint64, pendingStates []*heartbeatpb.State) (uint64, bool, bool) {
	candidate := buildMergedStartTsCandidate(minCheckpointTs, pendingStates)
	if candidate.allSamePending {
		if !candidate.pendingIsSyncPoint && candidate.pendingCommitTs == 0 {
			log.Warn("pending ddl has zero commit ts, fallback to min checkpoint ts",
				zap.Stringer("changefeedID", t.manager.changefeedID),
				zap.Uint64("minCheckpointTs", minCheckpointTs),
				zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()))
		}
		log.Info("merge dispatcher uses pending block event to calculate start ts",
			zap.Stringer("changefeedID", t.manager.changefeedID),
			zap.Any("mergedDispatcher", t.mergedDispatcher.GetId()),
			zap.Uint64("pendingCommitTs", candidate.pendingCommitTs),
			zap.Bool("pendingIsSyncPoint", candidate.pendingIsSyncPoint),
			zap.Uint64("startTs", candidate.startTs),
			zap.Bool("skipSyncpointAtStartTs", candidate.skipSyncpointAtStartTs),
			zap.Bool("skipDMLAsStartTs", candidate.skipDMLAsStartTs),
		)
	}

	return mergeMergedStartTsCandidateWithMySQLRecovery(t, candidate)
}

var (
	removeDispatcherTaskSchedulerOnce sync.Once
	removeDispatcherTaskScheduler     threadpool.ThreadPool
)

func GetRemoveDispatcherTaskScheduler() threadpool.ThreadPool {
	removeDispatcherTaskSchedulerOnce.Do(func() {
		removeDispatcherTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return removeDispatcherTaskScheduler
}

// RemoveDispatcherTask is a task to asynchronously remove a dispatcher until the dispatcher can be closed
type RemoveDispatcherTask struct {
	manager        *DispatcherManager
	dispatcherItem dispatcher.Dispatcher
	retryCount     int
}

func (t *RemoveDispatcherTask) Execute() time.Time {
	// Check if manager is closed
	if t.manager.closed.Load() {
		return time.Time{}
	}
	// If the dispatcher is removing, we don't need to remove it again
	if t.dispatcherItem.GetRemovingStatus() {
		return time.Time{}
	}

	// Try to close the dispatcher
	_, ok := t.dispatcherItem.TryClose()

	if ok {
		// Successfully closed, execute Remove
		t.dispatcherItem.Remove()
		log.Info("dispatcher removed successfully",
			zap.Stringer("dispatcherID", t.dispatcherItem.GetId()),
			zap.Int("retryCount", t.retryCount))
		return time.Time{} // Task completed
	}

	// Not successfully closed, retry
	t.retryCount++

	// Log periodically
	if t.retryCount%50 == 0 {
		log.Info("still retrying dispatcher close",
			zap.Stringer("dispatcherID", t.dispatcherItem.GetId()),
			zap.Int("retryCount", t.retryCount))
	}

	// Retry after 10ms, never give up
	return time.Now().Add(10 * time.Millisecond)
}
