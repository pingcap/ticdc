// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// If the dispatcher doesn't send heartbeat to the event service for a long time,
	// we consider it is in-active and remove it.
	heartbeatTimeout = time.Second * 180
)

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	id common.DispatcherID
	// reverse pointer to the changefeed status this dispatcher belongs to.
	changefeedStat *changefeedStatus
	// scanWorkerIndex is the index of the worker that this dispatcher belongs to.
	scanWorkerIndex int
	// messageWorkerIndex is the index of the worker that this dispatcher belongs to.
	messageWorkerIndex int
	info               DispatcherInfo
	// startTableInfo is the table info of the dispatcher when it is registered or reset.
	startTableInfo atomic.Pointer[common.TableInfo]
	filter         filter.Filter
	// The reset ts send by the dispatcher.
	// It is also the start ts of the dispatcher.
	resetTs atomic.Uint64
	// The max resolved ts received from event store.
	eventStoreResolvedTs atomic.Uint64
	// The max latest commit ts received from event store.
	latestCommitTs atomic.Uint64
	// The sentResolvedTs of the events that have been sent to the dispatcher.
	// We use this value to generate data range for the next scan task.
	// Note: Please don't changed this value directly, use updateSentResolvedTs instead.
	sentResolvedTs atomic.Uint64
	// checkpointTs is the ts that reported by the downstream dispatcher.
	// events <= checkpointTs will not needed anymore, so we can inform eventStore to GC them.
	// TODO: maintain it
	checkpointTs atomic.Uint64

	// The seq of the events that have been sent to the downstream dispatcher.
	// It start from 1, and increase by 1 for each event.
	// If the dispatcher is reset, the seq should be set to 1.
	seq atomic.Uint64

	// isRunning is used to indicate whether the dispatcher is running.
	// It will be set to false, after it receives the pause event from the dispatcher.
	// It will be set to true, after it receives the register/resume/reset event from the dispatcher.
	isRunning atomic.Bool
	// isHandshaked is used to indicate whether the dispatcher is ready to send data.
	// It will be set to true, after it sends the handshake event to the dispatcher.
	// It will be set to false, after it receives the reset event from the dispatcher.
	isHandshaked atomic.Bool

	// lastReceivedHeartbeatTime is the time when the dispatcher last received the heartbeat from the event service.
	lastReceivedHeartbeatTime atomic.Int64

	// syncpoint related
	enableSyncPoint   bool
	nextSyncPoint     uint64
	syncPointInterval time.Duration

	// Scan task related
	// taskScanning is used to indicate whether the scan task is running.
	// If so, we should wait until it is done before we send next resolvedTs event of
	// this dispatcher.

	isTaskScanning atomic.Bool

	// isRemoved is used to indicate whether the dispatcher is removed.
	// If so, we should ignore the errors related to this dispatcher.
	isRemoved atomic.Bool

	// scanRateLimiter *rate.Limiter

	isReceivedFirstResolvedTs atomic.Bool
}

func newDispatcherStat(
	startTs uint64,
	info DispatcherInfo,
	filter filter.Filter,
	scanWorkerIndex int,
	messageWorkerIndex int,
	changefeedStatus *changefeedStatus,
) *dispatcherStat {
	dispStat := &dispatcherStat{
		id:                 info.GetID(),
		changefeedStat:     changefeedStatus,
		scanWorkerIndex:    scanWorkerIndex,
		messageWorkerIndex: messageWorkerIndex,
		info:               info,
		filter:             filter,
		// scanRateLimiter:    rate.NewLimiter(rate.Limit(25), 25),
	}
	changefeedStatus.addDispatcher()

	if info.SyncPointEnabled() {
		dispStat.enableSyncPoint = true
		dispStat.nextSyncPoint = info.GetSyncPointTs()
		dispStat.syncPointInterval = info.GetSyncPointInterval()
	}
	dispStat.eventStoreResolvedTs.Store(startTs)
	dispStat.checkpointTs.Store(startTs)
	dispStat.sentResolvedTs.Store(startTs)
	dispStat.isRunning.Store(true)
	dispStat.lastReceivedHeartbeatTime.Store(time.Now().UnixNano())
	return dispStat
}

func (a *dispatcherStat) getEventSenderState() pevent.EventSenderState {
	if a.IsRunning() {
		return pevent.EventSenderStateNormal
	}
	return pevent.EventSenderStatePaused
}

func (a *dispatcherStat) updateTableInfo(tableInfo *common.TableInfo) {
	a.startTableInfo.Store(tableInfo)
}

func (a *dispatcherStat) updateSentResolvedTs(resolvedTs uint64) {
	// Only update the sentResolvedTs when the dispatcher is handshaked.
	if a.isHandshaked.Load() {
		a.sentResolvedTs.Store(resolvedTs)
	}
}

// resetState is used to reset the state of the dispatcher.
func (a *dispatcherStat) resetState(resetTs uint64) {
	// Do this first to prevent the dispatcher's sentResolvedTs being updated by other goroutines.
	a.isHandshaked.Store(false)
	// Reset the sentResolvedTs to the resetTs.
	// Because when the dispatcher is reset, the downstream want to resend the events from the resetTs.
	a.sentResolvedTs.Store(resetTs)
	a.resetTs.Store(resetTs)
	a.seq.Store(0)

	a.isTaskScanning.Store(false)

	a.isRunning.Store(true)
	a.lastReceivedHeartbeatTime.Store(time.Now().UnixNano())
}

// onResolvedTs try to update the resolved ts of the dispatcher.
func (a *dispatcherStat) onResolvedTs(resolvedTs uint64) bool {
	if resolvedTs < a.eventStoreResolvedTs.Load() {
		log.Panic("resolved ts should not fallback")
	}
	if !a.isReceivedFirstResolvedTs.Load() {
		log.Info("received first resolved ts from event service", zap.Uint64("resolvedTs", resolvedTs), zap.Stringer("dispatcherID", a.id))
		a.isReceivedFirstResolvedTs.Store(true)
	}
	return util.CompareAndMonotonicIncrease(&a.eventStoreResolvedTs, resolvedTs)
}

func (a *dispatcherStat) onLatestCommitTs(latestCommitTs uint64) bool {
	return util.CompareAndMonotonicIncrease(&a.latestCommitTs, latestCommitTs)
}

// getDataRange returns the the data range that the dispatcher needs to scan.
func (a *dispatcherStat) getDataRange() (common.DataRange, bool) {
	startTs := a.sentResolvedTs.Load()
	if startTs < a.resetTs.Load() {
		startTs = a.resetTs.Load()
	}

	if startTs >= a.eventStoreResolvedTs.Load() {
		return common.DataRange{}, false
	}
	// Range: (startTs, EndTs],
	// since the startTs(and the data before startTs) is already sent to the dispatcher.
	r := common.DataRange{
		Span:    a.info.GetTableSpan(),
		StartTs: startTs,
		EndTs:   a.eventStoreResolvedTs.Load(),
	}
	return r, true
}

func (a *dispatcherStat) IsRunning() bool {
	return a.isRunning.Load() && a.changefeedStat.isRunning.Load()
}

type scanTask = *dispatcherStat

func (t scanTask) GetKey() common.DispatcherID {
	return t.id
}

var wrapEventPool = sync.Pool{
	New: func() interface{} {
		return &wrapEvent{}
	},
}

func getWrapEvent() *wrapEvent {
	return wrapEventPool.Get().(*wrapEvent)
}

var zeroResolvedEvent = pevent.ResolvedEvent{}

type wrapEvent struct {
	serverID        node.ID
	resolvedTsEvent pevent.ResolvedEvent

	e       messaging.IOTypeT
	msgType int
	// postSendFunc should be called after the message is sent to message center
	postSendFunc func()
}

func newWrapDMLEvent(serverID node.ID, e *pevent.DMLEvent, state pevent.EventSenderState) *wrapEvent {
	e.State = state
	w := getWrapEvent()
	w.serverID = serverID
	w.e = e
	w.msgType = pevent.TypeDMLEvent
	return w
}

func (w *wrapEvent) reset() {
	w.e = nil
	w.postSendFunc = nil
	w.resolvedTsEvent = zeroResolvedEvent
	w.serverID = ""
	w.msgType = -1
	wrapEventPool.Put(w)
}

func (w wrapEvent) getDispatcherID() common.DispatcherID {
	e, ok := w.e.(pevent.Event)
	if !ok {
		log.Panic("cast event failed", zap.Any("event", w.e))
	}
	return e.GetDispatcherID()
}

func newWrapReadyEvent(serverID node.ID, e pevent.ReadyEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.e = &e
	w.msgType = pevent.TypeReadyEvent
	return w
}

func newWrapNotReusableEvent(serverID node.ID, e pevent.NotReusableEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.e = &e
	w.msgType = pevent.TypeNotReusableEvent
	return w
}

func newWrapResolvedEvent(serverID node.ID, e pevent.ResolvedEvent, state pevent.EventSenderState) *wrapEvent {
	e.State = state
	w := getWrapEvent()
	w.serverID = serverID
	w.resolvedTsEvent = e
	w.msgType = pevent.TypeResolvedEvent
	return w
}

func newWrapDDLEvent(serverID node.ID, e *pevent.DDLEvent, state pevent.EventSenderState) *wrapEvent {
	e.State = state
	w := getWrapEvent()
	w.serverID = serverID
	w.e = e
	w.msgType = pevent.TypeDDLEvent
	return w
}

func newWrapSyncPointEvent(serverID node.ID, e *pevent.SyncPointEvent, state pevent.EventSenderState) *wrapEvent {
	e.State = state
	w := getWrapEvent()
	w.serverID = serverID
	w.e = e
	w.msgType = pevent.TypeSyncPointEvent
	return w
}

// resolvedTsCache is used to cache the resolvedTs events.
// We use it instead of a primitive slice to reduce the allocation
// of the memory and reduce the GC pressure.
type resolvedTsCache struct {
	cache []pevent.ResolvedEvent
	// len is the number of the events in the cache.
	len int
	// limit is the max number of the events that the cache can store.
	limit int
}

func newResolvedTsCache(limit int) *resolvedTsCache {
	return &resolvedTsCache{
		cache: make([]pevent.ResolvedEvent, limit),
		limit: limit,
	}
}

func (c *resolvedTsCache) add(e pevent.ResolvedEvent) {
	c.cache[c.len] = e
	c.len++
}

func (c *resolvedTsCache) isFull() bool {
	return c.len >= c.limit
}

func (c *resolvedTsCache) getAll() []pevent.ResolvedEvent {
	res := c.cache[:c.len]
	c.reset()
	return res
}

func (c *resolvedTsCache) reset() {
	c.len = 0
}

type changefeedStatus struct {
	changefeedID common.ChangeFeedID
	// isRunning is used to indicate whether the changefeed is running.
	// It will be set to false, after it receives the pause event from the dispatcher.
	// It will be set to true, after it receives the register/resume/reset event from the dispatcher.
	isRunning atomic.Bool
	// dispatcherCount is the number of the dispatchers that belong to this changefeed.
	dispatcherCount atomic.Uint64

	dispatcherStatMap sync.Map // nodeID -> dispatcherID -> dispatcherStat
}

func newChangefeedStatus(changefeedID common.ChangeFeedID) *changefeedStatus {
	stat := &changefeedStatus{
		changefeedID: changefeedID,
		isRunning:    atomic.Bool{},
	}
	stat.isRunning.Store(true)
	return stat
}

func (c *changefeedStatus) addDispatcher() {
	c.dispatcherCount.Inc()
}

func (c *changefeedStatus) removeDispatcher() {
	c.dispatcherCount.Dec()
}
