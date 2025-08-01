// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatcher

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/ticdc/utils/threadpool"
	"go.uber.org/zap"
)

type ResendTaskMap struct {
	mutex sync.Mutex
	m     map[BlockEventIdentifier]*ResendTask
}

func newResendTaskMap() *ResendTaskMap {
	return &ResendTaskMap{
		m: make(map[BlockEventIdentifier]*ResendTask),
	}
}

func (r *ResendTaskMap) Get(identifier BlockEventIdentifier) *ResendTask {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.m[identifier]
}

func (r *ResendTaskMap) Set(identifier BlockEventIdentifier, task *ResendTask) {
	log.Info("set resend task", zap.Any("identifier", identifier), zap.Any("taskDispatcherID", task.dispatcher.id))
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.m[identifier] = task
}

func (r *ResendTaskMap) Delete(identifier BlockEventIdentifier) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.m, identifier)
}

func (r *ResendTaskMap) Len() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.m)
}

// Considering the sync point event and ddl event may have the same commitTs,
// we need to distinguish them.
type BlockEventIdentifier struct {
	CommitTs    uint64
	IsSyncPoint bool
}

type BlockEventStatus struct {
	mutex             sync.Mutex
	blockPendingEvent commonEvent.BlockEvent
	blockStage        heartbeatpb.BlockStage
	// record all the commitTs of this pending event
	// mainly for the batch sync point event
	blockCommitTsMap map[uint64]struct{}
}

func (b *BlockEventStatus) clear() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = nil
	b.blockStage = heartbeatpb.BlockStage_NONE
	b.blockCommitTsMap = make(map[uint64]struct{})
}

func (b *BlockEventStatus) setBlockEvent(event commonEvent.BlockEvent, blockStage heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = event
	b.blockStage = blockStage
	b.blockCommitTsMap = make(map[uint64]struct{})

	if event.GetType() == commonEvent.TypeSyncPointEvent {
		for _, ts := range event.(*commonEvent.SyncPointEvent).GetCommitTsList() {
			b.blockCommitTsMap[ts] = struct{}{}
		}
	} else {
		b.blockCommitTsMap[event.GetCommitTs()] = struct{}{}
	}
}

func (b *BlockEventStatus) updateBlockStage(blockStage heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.blockStage = blockStage
}

func (b *BlockEventStatus) getEvent() commonEvent.BlockEvent {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.blockPendingEvent
}

func (b *BlockEventStatus) getEventAndStage() (commonEvent.BlockEvent, heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.blockPendingEvent, b.blockStage
}

// actionMatchs checks whether the action is for the current pending ddl event.
// Most of time, the pending event only have one commitTs, so when the commitTs of the action meets the pending event's commitTs, it is enough.
// While if the pending event is a sync point event with multiple commitTs, we only can do the action
// when all the commitTs have been received.
func (b *BlockEventStatus) actionMatchs(action *heartbeatpb.DispatcherAction) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.blockPendingEvent == nil {
		return false
	}

	if b.blockStage != heartbeatpb.BlockStage_WAITING {
		return false
	}

	_, ok := b.blockCommitTsMap[action.CommitTs]
	if ok {
		delete(b.blockCommitTsMap, action.CommitTs)
	}

	if len(b.blockCommitTsMap) == 0 {
		return true
	}
	return false
}

type SchemaIDToDispatchers struct {
	mutex sync.RWMutex
	m     map[int64]map[common.DispatcherID]interface{}
}

func NewSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return &SchemaIDToDispatchers{
		m: make(map[int64]map[common.DispatcherID]interface{}),
	}
}

func (s *SchemaIDToDispatchers) Set(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; !ok {
		s.m[schemaID] = make(map[common.DispatcherID]interface{})
	}
	s.m[schemaID][dispatcherID] = struct{}{}
}

func (s *SchemaIDToDispatchers) Delete(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; ok {
		delete(s.m[schemaID], dispatcherID)
	}
}

func (s *SchemaIDToDispatchers) Update(oldSchemaID int64, newSchemaID int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[oldSchemaID]; ok {
		s.m[newSchemaID] = s.m[oldSchemaID]
		delete(s.m, oldSchemaID)
	} else {
		log.Error("schemaID not found", zap.Any("schemaID", oldSchemaID))
	}
}

func (s *SchemaIDToDispatchers) GetDispatcherIDs(schemaID int64) []common.DispatcherID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ids, ok := s.m[schemaID]; ok {
		dispatcherIDs := make([]common.DispatcherID, 0, len(ids))
		for id := range ids {
			dispatcherIDs = append(dispatcherIDs, id)
		}
		return dispatcherIDs
	}
	return nil
}

type ComponentStateWithMutex struct {
	mutex           sync.Mutex
	componentStatus heartbeatpb.ComponentState
}

func newComponentStateWithMutex(status heartbeatpb.ComponentState) *ComponentStateWithMutex {
	return &ComponentStateWithMutex{
		componentStatus: status,
	}
}

func (s *ComponentStateWithMutex) Set(status heartbeatpb.ComponentState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.componentStatus = status
}

func (s *ComponentStateWithMutex) Get() heartbeatpb.ComponentState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.componentStatus
}

type TableSpanStatusWithSeq struct {
	*heartbeatpb.TableSpanStatus
	CheckpointTs uint64
	ResolvedTs   uint64
	Seq          uint64
}

/*
HeartBeatInfo is used to collect the message for HeartBeatRequest for each dispatcher.
Mainly about the progress of each dispatcher:
1. The checkpointTs of the dispatcher, shows that all the events whose ts <= checkpointTs are flushed to downstream successfully.
*/
type HeartBeatInfo struct {
	heartbeatpb.Watermark
	Id              common.DispatcherID
	ComponentStatus heartbeatpb.ComponentState
	IsRemoving      bool
}

// Resend Task is reponsible for resending the TableSpanBlockStatus message with ddl info to maintainer each 50ms.
// The task will be cancelled when the the dispatcher received the ack message from the maintainer
type ResendTask struct {
	message    *heartbeatpb.TableSpanBlockStatus
	dispatcher *Dispatcher
	callback   func() // function need to be called when the task is cancelled
	taskHandle *threadpool.TaskHandle
}

func newResendTask(message *heartbeatpb.TableSpanBlockStatus, dispatcher *Dispatcher, callback func()) *ResendTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &ResendTask{
		message:    message,
		dispatcher: dispatcher,
		callback:   callback,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(50*time.Millisecond))
	return t
}

func (t *ResendTask) Execute() time.Time {
	log.Debug("resend task", zap.Any("message", t.message), zap.Any("dispatcherID", t.dispatcher.GetId()))
	t.dispatcher.blockStatusesChan <- t.message
	return time.Now().Add(200 * time.Millisecond)
}

func (t *ResendTask) Cancel() {
	if t.callback != nil {
		t.callback()
	}
	t.taskHandle.Cancel()
}

var (
	DispatcherTaskScheduler     threadpool.ThreadPool
	dispatcherTaskSchedulerOnce sync.Once
)

func GetDispatcherTaskScheduler() threadpool.ThreadPool {
	dispatcherTaskSchedulerOnce.Do(func() {
		DispatcherTaskScheduler = threadpool.NewThreadPoolDefault()
	})
	return DispatcherTaskScheduler
}

func SetDispatcherTaskScheduler(taskScheduler threadpool.ThreadPool) {
	DispatcherTaskScheduler = taskScheduler
}

type DispatcherEvent struct {
	From *node.ID
	commonEvent.Event
}

func (d DispatcherEvent) GetSize() int64 {
	return d.From.GetSize() + d.Event.GetSize()
}

func NewDispatcherEvent(from *node.ID, event commonEvent.Event) DispatcherEvent {
	return DispatcherEvent{
		From:  from,
		Event: event,
	}
}

type DispatcherStatusWithID struct {
	id     common.DispatcherID
	status *heartbeatpb.DispatcherStatus
}

func NewDispatcherStatusWithID(dispatcherStatus *heartbeatpb.DispatcherStatus, dispatcherID common.DispatcherID) DispatcherStatusWithID {
	return DispatcherStatusWithID{
		status: dispatcherStatus,
		id:     dispatcherID,
	}
}

func (d *DispatcherStatusWithID) GetDispatcherStatus() *heartbeatpb.DispatcherStatus {
	return d.status
}

func (d *DispatcherStatusWithID) GetDispatcherID() common.DispatcherID {
	return d.id
}

// DispatcherStatusHandler is used to handle the DispatcherStatus event.
// Each dispatcher status may contain a ACK info or a dispatcher action or both.
// If we get a ack info, we need to check whether the ack is for the current pending ddl event.
// If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event.
// If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and
// wake the dispatcherEventsHandler to handle the event.
type DispatcherStatusHandler struct{}

func (h *DispatcherStatusHandler) Path(event DispatcherStatusWithID) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *DispatcherStatusHandler) Handle(dispatcher *Dispatcher, events ...DispatcherStatusWithID) (await bool) {
	for _, event := range events {
		dispatcher.HandleDispatcherStatus(event.GetDispatcherStatus())
	}
	return false
}

func (h *DispatcherStatusHandler) GetSize(event DispatcherStatusWithID) int   { return 0 }
func (h *DispatcherStatusHandler) IsPaused(event DispatcherStatusWithID) bool { return false }
func (h *DispatcherStatusHandler) GetArea(path common.DispatcherID, dest *Dispatcher) common.GID {
	return dest.GetChangefeedID().ID()
}

func (h *DispatcherStatusHandler) GetTimestamp(event DispatcherStatusWithID) dynstream.Timestamp {
	if event.GetDispatcherStatus().Action != nil {
		return dynstream.Timestamp(event.GetDispatcherStatus().Action.CommitTs)
	} else if event.GetDispatcherStatus().Ack != nil {
		return dynstream.Timestamp(event.GetDispatcherStatus().Ack.CommitTs)
	}
	return 0
}

func (h *DispatcherStatusHandler) GetType(event DispatcherStatusWithID) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *DispatcherStatusHandler) OnDrop(event DispatcherStatusWithID) interface{} {
	return nil
}

// dispatcherStatusDS is the dynamic stream for dispatcher status.
// It's a server level singleton, so we use a sync.Once to ensure the instance is created only once.
var (
	dispatcherStatusDS     dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, *Dispatcher, *DispatcherStatusHandler]
	dispatcherStatusDSOnce sync.Once
)

func GetDispatcherStatusDynamicStream() dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, *Dispatcher, *DispatcherStatusHandler] {
	dispatcherStatusDSOnce.Do(func() {
		dispatcherStatusDS = dynstream.NewParallelDynamicStream(func(id common.DispatcherID) uint64 { return common.GID(id).FastHash() }, &DispatcherStatusHandler{})
		dispatcherStatusDS.Start()
	})
	return dispatcherStatusDS
}

// bootstrapState used to check if send bootstrap event after changefeed created
type bootstrapState int32

const (
	BootstrapNotStarted bootstrapState = iota
	BootstrapInProgress
	BootstrapFinished
)

func storeBootstrapState(addr *bootstrapState, state bootstrapState) {
	atomic.StoreInt32((*int32)(addr), int32(state))
}

func loadBootstrapState(addr *bootstrapState) bootstrapState {
	return bootstrapState(atomic.LoadInt32((*int32)(addr)))
}
