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
	"container/list"
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
	log.Info("set resend task", zap.Any("identifier", identifier), zap.Any("taskDispatcherID", task.dispatcher.GetId()))
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

func (r *ResendTaskMap) Keys() []BlockEventIdentifier {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	keys := make([]BlockEventIdentifier, 0, len(r.m))
	for key := range r.m {
		keys = append(keys, key)
	}
	return keys
}

// Considering the sync point event and ddl event may have the same commitTs,
// we need to distinguish them.
type BlockEventIdentifier struct {
	CommitTs    uint64
	IsSyncPoint bool
}

// addTableCheckpointBlocker caps the table-trigger checkpoint while add-table
// DDLs are waiting for maintainer ACK. The list keeps add order, and the map
// gives O(1) removal when ACKs arrive out of order.
type addTableCheckpointBlocker struct {
	mutex       sync.Mutex
	pending     map[BlockEventIdentifier]*list.Element
	queue       *list.List
	minCommitTs atomic.Uint64
}

func newAddTableCheckpointBlocker() *addTableCheckpointBlocker {
	return &addTableCheckpointBlocker{
		pending: make(map[BlockEventIdentifier]*list.Element),
		queue:   list.New(),
	}
}

func (b *addTableCheckpointBlocker) add(identifier BlockEventIdentifier) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, ok := b.pending[identifier]; ok {
		return
	}
	if len(b.pending) == 0 {
		// Checkpoint readers only look at minCommitTs, so publish the cap
		// before returning the newly pending blocker to the caller.
		b.minCommitTs.Store(identifier.CommitTs)
	}
	b.pending[identifier] = b.queue.PushBack(identifier)
}

func (b *addTableCheckpointBlocker) remove(identifier BlockEventIdentifier) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	elem, ok := b.pending[identifier]
	if !ok {
		return
	}
	delete(b.pending, identifier)
	wasMin := b.queue.Front() == elem
	b.queue.Remove(elem)
	if wasMin {
		b.storeMinCommitTsLocked()
	}
}

func (b *addTableCheckpointBlocker) empty() bool {
	return b.minCommitTs.Load() == 0
}

func (b *addTableCheckpointBlocker) len() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return len(b.pending)
}

func (b *addTableCheckpointBlocker) capCheckpointTs(checkpointTs uint64) uint64 {
	minCommitTs := b.minCommitTs.Load()
	if minCommitTs == 0 {
		return checkpointTs
	}
	return min(checkpointTs, minCommitTs-1)
}

func (b *addTableCheckpointBlocker) storeMinCommitTsLocked() {
	front := b.queue.Front()
	if front == nil {
		b.minCommitTs.Store(0)
		return
	}
	b.minCommitTs.Store(front.Value.(BlockEventIdentifier).CommitTs)
}

type BlockEventStatus struct {
	mutex             sync.Mutex
	blockPendingEvent commonEvent.BlockEvent
	blockStage        heartbeatpb.BlockStage
	blockCommitTs     uint64
}

func (b *BlockEventStatus) clear() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = nil
	b.blockStage = heartbeatpb.BlockStage_NONE
	b.blockCommitTs = 0
}

func (b *BlockEventStatus) setBlockEvent(event commonEvent.BlockEvent, blockStage heartbeatpb.BlockStage) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.blockPendingEvent = event
	b.blockStage = blockStage
	b.blockCommitTs = event.GetCommitTs()
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

// actionMatchs checks whether the action is for the current pending ddl/sync point event.
func (b *BlockEventStatus) actionMatchs(action *heartbeatpb.DispatcherAction) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.blockPendingEvent == nil {
		return false
	}

	if b.blockStage != heartbeatpb.BlockStage_WAITING {
		return false
	}

	return b.blockCommitTs == action.CommitTs
}

// ignoredStatusMatches checks whether the ignored status is for the current pending ddl/sync point event.
func (b *BlockEventStatus) ignoredStatusMatches(ignored *heartbeatpb.IgnoredBlockStatus) bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.blockPendingEvent == nil {
		return false
	}

	if b.blockStage != heartbeatpb.BlockStage_WAITING {
		return false
	}

	pendingIsSyncPoint := b.blockPendingEvent.GetType() == commonEvent.TypeSyncPointEvent
	return b.blockCommitTs == ignored.CommitTs && pendingIsSyncPoint == ignored.IsSyncPoint
}

func (b *BlockEventStatus) getEventCommitTs() (uint64, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if b.blockPendingEvent == nil {
		return 0, false
	}
	return b.blockCommitTs, true
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
	Seq uint64
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

// blockStatusOfferer is the minimal dispatcher surface needed by resend tasks.
// Keeping it narrow avoids reintroducing a broader unified block-status API.
type blockStatusOfferer interface {
	GetId() common.DispatcherID
	offerBlockStatus(status *heartbeatpb.TableSpanBlockStatus)
}

// ResendTask is responsible for periodically resending the block status to the maintainer.
// The task will be cancelled when the dispatcher receives the ACK message from the maintainer.
type ResendTask struct {
	dispatcher   blockStatusOfferer
	message      *heartbeatpb.TableSpanBlockStatus
	callback     func() // function need to be called when the task is cancelled
	taskHandle   *threadpool.TaskHandle
	executeCount uint64
}

const resendTimeInterval = 5 * time.Second

// newResendTask registers a periodic resend immediately so the first retry is
// scheduled with the same immutable protobuf object the initial send used.
func newResendTask(dispatcher blockStatusOfferer, message *heartbeatpb.TableSpanBlockStatus, callback func()) *ResendTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &ResendTask{
		dispatcher: dispatcher,
		message:    message,
		callback:   callback,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(resendTimeInterval))
	return t
}

// Execute resends the original protobuf object without rebuilding payload so
// WAITING/NONE retries stay allocation-light and byte-for-byte consistent.
func (t *ResendTask) Execute() time.Time {
	log.Debug("resend task", zap.Any("dispatcherID", t.dispatcher.GetId()))
	t.dispatcher.offerBlockStatus(t.message)

	executeCount := atomic.AddUint64(&t.executeCount, 1)
	if executeCount%10 == 0 {
		log.Info("resend task periodic resend",
			zap.Any("dispatcherID", t.dispatcher.GetId()),
			zap.Uint64("executeCount", executeCount))
	}
	return time.Now().Add(resendTimeInterval)
}

// Cancel stops future retries and runs the optional completion callback once.
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
// 1. If the action is a write, flush prior DML and write the block event to sink(async).
// 2. If the action is a pass, flush prior DML and pass the event in tableProgress(async).
type DispatcherStatusHandler struct{}

func (h *DispatcherStatusHandler) Path(event DispatcherStatusWithID) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *DispatcherStatusHandler) Handle(dispatcher Dispatcher, events ...DispatcherStatusWithID) (await bool) {
	if len(events) != 1 {
		panic("invalid event count")
	}
	return dispatcher.HandleDispatcherStatus(events[0].GetDispatcherStatus())
}

func (h *DispatcherStatusHandler) GetSize(event DispatcherStatusWithID) int   { return 0 }
func (h *DispatcherStatusHandler) IsPaused(event DispatcherStatusWithID) bool { return false }
func (h *DispatcherStatusHandler) GetArea(path common.DispatcherID, dest Dispatcher) common.GID {
	return dest.GetChangefeedID().ID()
}

func (h *DispatcherStatusHandler) GetMetricLabel(dest Dispatcher) string {
	return dest.GetChangefeedID().String()
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
	// DispatcherStatus may trigger downstream IO when handling action write/pass
	// because we flush prior DML before completing the block action.
	// Make it non-batchable to ensure we can safely return await=true for a single event.
	return dynstream.EventType{DataGroup: 0, Property: dynstream.NonBatchable}
}

func (h *DispatcherStatusHandler) OnDrop(event DispatcherStatusWithID) interface{} {
	return nil
}

// dispatcherStatusDS is the dynamic stream for dispatcher status.
// It's a server level singleton, so we use a sync.Once to ensure the instance is created only once.
var (
	dispatcherStatusDS     dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, Dispatcher, *DispatcherStatusHandler]
	dispatcherStatusDSOnce sync.Once
)

func GetDispatcherStatusDynamicStream() dynstream.DynamicStream[common.GID, common.DispatcherID, DispatcherStatusWithID, Dispatcher, *DispatcherStatusHandler] {
	dispatcherStatusDSOnce.Do(func() {
		dispatcherStatusDS = dynstream.NewParallelDynamicStream("dispatcher-status", &DispatcherStatusHandler{})
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

// addToDynamicStream add self to dynamic stream
func addToStatusDynamicStream(d Dispatcher) {
	dispatcherStatusDS := GetDispatcherStatusDynamicStream()
	err := dispatcherStatusDS.AddPath(d.GetId(), d)
	if err != nil {
		log.Error("add dispatcher to dynamic stream failed",
			zap.Stringer("changefeedID", d.GetChangefeedID()),
			zap.Stringer("dispatcher", d.GetId()),
			zap.Error(err))
	}
}
