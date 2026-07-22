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
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
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
	heartbeatTimeout = time.Second * 3600

	minScanLimitInBytes     = 1024        // 1KB
	maxScanLimitInBytes     = 1024 * 1024 // 1MB
	updateScanLimitInterval = time.Second * 10
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
	filter             filter.Filter
	// the start ts of the dispatcher
	startTs uint64
	// startTableInfo is the table info at the `startTs` of the dispatcher
	startTableInfo *common.TableInfo
	// The epoch of the dispatcher.
	// It should not be changed after the dispatcher is created.
	epoch uint64

	// The seq of the events that have been sent to the downstream dispatcher.
	// It starts from 1, and increase by 1 for each event.
	// If the dispatcher is reset, the seq should be set to 1.
	seq atomic.Uint64
	// This lock should only be used to protect the seq when setting handshake.
	handshakeLock sync.Mutex

	// syncpoint related
	enableSyncPoint   bool
	nextSyncPoint     atomic.Uint64
	syncPointInterval time.Duration
	txnAtomicity      config.AtomicityLevel

	// =============================================================================
	// ================== below are fields need copied when reset ==================

	// The max resolved ts received from event store/schema store.
	receivedResolvedTs atomic.Uint64

	// The max commit ts of dml events received from event store.
	// it's updated after the event store receive DML events.
	eventStoreCommitTs atomic.Uint64

	// checkpointTs is the ts that reported by the downstream dispatcher.
	// events <= checkpointTs will not needed anymore, so we can inform eventStore to GC them.
	checkpointTs atomic.Uint64

	hasReceivedFirstResolvedTs atomic.Bool

	currentScanLimitInBytes atomic.Int64
	maxScanLimitInBytes     atomic.Int64
	lastUpdateScanLimitTime atomic.Time
	lastScanBytes           atomic.Int64
	availableMemoryQuota    atomic.Uint64

	lastReceivedResolvedTsTime atomic.Time
	lastSentResolvedTsTime     atomic.Time

	// ================== above are fields need copied when reset ==================
	// =============================================================================

	// The sentResolvedTs of the events that have been sent to the dispatcher.
	// Note: Please don't changed this value directly, use updateSentResolvedTs instead.
	sentResolvedTs atomic.Uint64

	// lastScanProgress is published as one immutable value so the next scan
	// cannot observe a cursor assembled from different scan results.
	lastScanProgress atomic.Pointer[scanProgress]

	largeTxnStateMu sync.Mutex
	largeTxnState   *largeTxnScanState

	// bigTxnMetricState is updated only by the serialized scan task of this dispatcher.
	bigTxnMetricState *bigTxnMetricState

	// isRemoved is used to indicate whether the dispatcher is removed.
	// it is set to true in the following two cases:
	//   1) the dispatcher is removed
	//   2) the dispatcherStat is replaced by a new one with larger epoch.
	// If so, we should ignore the errors related to this dispatcher.
	isRemoved atomic.Bool
	// lastReadySendTime is the time when the ready event was last sent to dispatcher.
	lastReadySendTime atomic.Int64
	// readyInterval is the interval between two ready events in seconds.
	// it will double the interval for next time, but cap at maxReadyEventInterval.
	readyInterval atomic.Int64

	// lastReceivedHeartbeatTime is the time when the dispatcher last received the heartbeat from the event service.
	lastReceivedHeartbeatTime atomic.Int64

	// Scan task related
	// taskScanning is used to indicate whether the scan task is running.
	// If so, we should wait until it is done before we send next resolvedTs event of
	// this dispatcher.
	isTaskScanning atomic.Bool

	activeScanMu sync.Mutex
	activeScan   *activeDispatcherScan
}

type activeDispatcherScan struct {
	cancel context.CancelFunc
}

func newDispatcherStat(
	info DispatcherInfo,
	scanWorkerCount uint64,
	messageWorkerCount uint64,
	startTableInfo *common.TableInfo,
	changefeedStatus *changefeedStatus,
) *dispatcherStat {
	id := info.GetID()
	dispStat := &dispatcherStat{
		id:                 id,
		changefeedStat:     changefeedStatus,
		scanWorkerIndex:    (common.GID)(id).Hash(scanWorkerCount),
		messageWorkerIndex: (common.GID)(id).Hash(messageWorkerCount),
		info:               info,
		filter:             changefeedStatus.filter,
		startTs:            info.GetStartTs(),
		epoch:              info.GetEpoch(),
		startTableInfo:     startTableInfo,
		txnAtomicity:       info.GetTxnAtomicity(),
	}

	// A small value to avoid too many scan tasks at the first place.
	dispStat.lastScanBytes.Store(minScanLimitInBytes)
	dispStat.availableMemoryQuota.Store(minScanLimitInBytes)

	if info.SyncPointEnabled() {
		dispStat.enableSyncPoint = true
		dispStat.nextSyncPoint.Store(info.GetSyncPointTs())
		dispStat.syncPointInterval = info.GetSyncPointInterval()
	}
	startTs := info.GetStartTs()
	dispStat.receivedResolvedTs.Store(startTs)
	dispStat.checkpointTs.Store(startTs)

	dispStat.sentResolvedTs.Store(startTs)

	dispStat.storeScanProgress(newTxnScanProgress(startTs, 0))
	dispStat.lastReadySendTime.Store(0)
	dispStat.readyInterval.Store(1)
	dispStat.resetScanLimit()

	now := time.Now()
	dispStat.lastReceivedResolvedTsTime.Store(now)
	dispStat.lastSentResolvedTsTime.Store(now)
	dispStat.lastReceivedHeartbeatTime.Store(now.Unix())
	return dispStat
}

// copyStatistics copies statistical data that reflects the dynamic state of the dispatcher.
func (a *dispatcherStat) copyStatistics(src *dispatcherStat) {
	a.receivedResolvedTs.Store(src.receivedResolvedTs.Load())
	a.eventStoreCommitTs.Store(src.eventStoreCommitTs.Load())
	a.checkpointTs.Store(src.checkpointTs.Load())
	a.hasReceivedFirstResolvedTs.Store(src.hasReceivedFirstResolvedTs.Load())

	a.currentScanLimitInBytes.Store(src.currentScanLimitInBytes.Load())
	a.maxScanLimitInBytes.Store(src.maxScanLimitInBytes.Load())
	a.lastUpdateScanLimitTime.Store(src.lastUpdateScanLimitTime.Load())
	a.lastScanBytes.Store(src.lastScanBytes.Load())

	a.lastReceivedResolvedTsTime.Store(src.lastReceivedResolvedTsTime.Load())
	a.lastSentResolvedTsTime.Store(src.lastSentResolvedTsTime.Load())
}

func (a *dispatcherStat) isHandshaked() bool {
	return a.seq.Load() > 0
}

func (a *dispatcherStat) beginScan(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	activeScan := &activeDispatcherScan{cancel: cancel}

	a.activeScanMu.Lock()
	if a.isRemoved.Load() {
		cancel()
	} else {
		a.activeScan = activeScan
	}
	a.activeScanMu.Unlock()

	return ctx, func() {
		cancel()
		a.activeScanMu.Lock()
		if a.activeScan == activeScan {
			a.activeScan = nil
		}
		a.activeScanMu.Unlock()
	}
}

func (a *dispatcherStat) markRemoved() {
	a.activeScanMu.Lock()
	a.isRemoved.Store(true)
	activeScan := a.activeScan
	a.activeScanMu.Unlock()
	if activeScan != nil {
		activeScan.cancel()
	}
}

func (a *dispatcherStat) setHandshaked() {
	a.seq.Store(1)
}

func (a *dispatcherStat) updateSentResolvedTs(resolvedTs uint64) {
	a.sentResolvedTs.Store(resolvedTs)
	a.lastSentResolvedTsTime.Store(time.Now())
	a.updateScanRange(resolvedTs, 0)
}

func (a *dispatcherStat) updateScanRange(txnCommitTs, txnStartTs uint64) {
	a.updateScanRangeWithPosition(txnCommitTs, txnStartTs, nil)
}

func (a *dispatcherStat) updateScanRangeWithPosition(
	txnCommitTs uint64,
	txnStartTs uint64,
	position eventstore.ScanPosition,
) {
	a.storeScanProgress(newRowLevelScanProgress(txnCommitTs, txnStartTs, position))
}

func (a *dispatcherStat) storeScanProgress(progress scanProgress) {
	progress.rowLevelScanPosition = cloneScanPosition(progress.rowLevelScanPosition)
	a.lastScanProgress.Store(&progress)
}

func (a *dispatcherStat) loadScanProgress() scanProgress {
	progress := a.lastScanProgress.Load()
	if progress == nil {
		return scanProgress{}
	}
	result := *progress
	result.rowLevelScanPosition = cloneScanPosition(result.rowLevelScanPosition)
	return result
}

type bigTxnMetricState struct {
	startTs                  uint64
	commitTs                 uint64
	rawKVBytes               int64
	largeTxnThresholdInBytes int64
}

func (a *dispatcherStat) addBigTxnMetricFragment(
	startTs uint64,
	commitTs uint64,
	rawKVBytes int64,
	largeTxnThresholdInBytes int64,
) {
	if rawKVBytes <= largeTxnThresholdInBytes {
		return
	}
	if a.bigTxnMetricState == nil ||
		a.bigTxnMetricState.startTs != startTs ||
		a.bigTxnMetricState.commitTs != commitTs {
		a.finishPendingBigTxnMetricBefore(startTs, commitTs)
		a.bigTxnMetricState = &bigTxnMetricState{
			startTs:                  startTs,
			commitTs:                 commitTs,
			largeTxnThresholdInBytes: largeTxnThresholdInBytes,
		}
	}
	a.bigTxnMetricState.rawKVBytes += rawKVBytes
}

func (a *dispatcherStat) finishBigTxnMetric(
	startTs uint64,
	commitTs uint64,
	rawKVBytes int64,
	largeTxnThresholdInBytes int64,
) {
	totalRawKVBytes := rawKVBytes
	if a.bigTxnMetricState != nil {
		if a.bigTxnMetricState.startTs == startTs && a.bigTxnMetricState.commitTs == commitTs {
			totalRawKVBytes += a.bigTxnMetricState.rawKVBytes
		} else if a.bigTxnMetricState.rawKVBytes > a.bigTxnMetricState.largeTxnThresholdInBytes {
			updateMetricEventServiceBigTxn(a.bigTxnMetricState.rawKVBytes)
		}
		a.bigTxnMetricState = nil
	}
	if totalRawKVBytes > largeTxnThresholdInBytes {
		updateMetricEventServiceBigTxn(totalRawKVBytes)
	}
}

func (a *dispatcherStat) finishPendingBigTxnMetric() {
	if a.bigTxnMetricState == nil {
		return
	}
	state := a.bigTxnMetricState
	a.bigTxnMetricState = nil
	if state.rawKVBytes > state.largeTxnThresholdInBytes {
		updateMetricEventServiceBigTxn(state.rawKVBytes)
	}
}

func (a *dispatcherStat) finishPendingBigTxnMetricBefore(startTs uint64, commitTs uint64) {
	if a.bigTxnMetricState == nil ||
		(a.bigTxnMetricState.startTs == startTs && a.bigTxnMetricState.commitTs == commitTs) {
		return
	}
	a.finishPendingBigTxnMetric()
}

// onResolvedTs try to update the resolved ts of the dispatcher.
func (a *dispatcherStat) onResolvedTs(resolvedTs uint64) bool {
	if resolvedTs <= a.receivedResolvedTs.Load() {
		return false
	}
	if !a.hasReceivedFirstResolvedTs.Load() {
		log.Info("received first resolved ts from event store",
			zap.Stringer("changefeedID", a.changefeedStat.changefeedID),
			zap.Stringer("dispatcherID", a.id),
			zap.Uint64("resolvedTs", resolvedTs))
		a.lastUpdateScanLimitTime.Store(time.Now())
		a.hasReceivedFirstResolvedTs.Store(true)
	}
	return util.CompareAndMonotonicIncrease(&a.receivedResolvedTs, resolvedTs)
}

func (a *dispatcherStat) onLatestCommitTs(latestCommitTs uint64) bool {
	return util.CompareAndMonotonicIncrease(&a.eventStoreCommitTs, latestCommitTs)
}

// getScanRequest returns the range and cursor that the dispatcher needs to scan.
func (a *dispatcherStat) getScanRequest() (eventstore.ScanRequest, bool) {
	progress := a.loadScanProgress()
	lastTxnCommitTs := progress.txnCommitTs
	lastTxnStartTs := progress.txnStartTs
	lastPosition := progress.rowLevelScanPosition
	hasPendingLargeTxn := a.hasPendingLargeTxnState()

	// the data not received by the event store yet, so just skip it.
	resolvedTs := a.receivedResolvedTs.Load()
	if lastTxnCommitTs > resolvedTs {
		return eventstore.ScanRequest{}, false
	}
	if lastTxnCommitTs == resolvedTs && lastTxnStartTs == 0 &&
		len(lastPosition) == 0 && !hasPendingLargeTxn {
		return eventstore.ScanRequest{}, false
	}
	// Range is (CommitTsStart, CommitTsEnd], with Cursor identifying any
	// unfinished transaction or row at CommitTsStart.
	r := eventstore.ScanRequest{
		Range: common.DataRange{
			Span:          a.info.GetTableSpan(),
			CommitTsStart: lastTxnCommitTs,
			CommitTsEnd:   resolvedTs,
		},
		Cursor: eventstore.ScanCursor{
			TxnStartTs: lastTxnStartTs,
			Position:   lastPosition,
		},
	}
	return r, true
}

// getCurrentScanLimitInBytes returns the current scan limit in bytes.
// It will double the current scan limit in bytes every 10 seconds,
// and cap the scan limit in bytes to the max scan limit in bytes.
func (a *dispatcherStat) getCurrentScanLimitInBytes() int64 {
	res := a.currentScanLimitInBytes.Load()
	if time.Since(a.lastUpdateScanLimitTime.Load()) > updateScanLimitInterval {
		maxScanLimit := a.maxScanLimitInBytes.Load()
		if res > maxScanLimit {
			return maxScanLimit
		}
		newLimit := res * 2
		if newLimit > maxScanLimit {
			newLimit = maxScanLimit
		}
		a.currentScanLimitInBytes.Store(newLimit)
		a.lastUpdateScanLimitTime.Store(time.Now())
	}
	return res
}

func (a *dispatcherStat) resetScanLimit() {
	a.currentScanLimitInBytes.Store(minScanLimitInBytes)
	a.maxScanLimitInBytes.Store(maxScanLimitInBytes)
	a.lastUpdateScanLimitTime.Store(time.Now())
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

func newWrapBatchDMLEvent(serverID node.ID, e *pevent.BatchDMLEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.e = e
	w.msgType = e.GetType()
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

func newWrapHandshakeEvent(serverID node.ID, e pevent.HandshakeEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.e = &e
	w.msgType = pevent.TypeHandshakeEvent
	return w
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

func newWrapResolvedEvent(serverID node.ID, e pevent.ResolvedEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.resolvedTsEvent = e
	w.msgType = pevent.TypeResolvedEvent
	return w
}

func newWrapDDLEvent(serverID node.ID, e *pevent.DDLEvent) *wrapEvent {
	w := getWrapEvent()
	w.serverID = serverID
	w.e = e
	w.msgType = pevent.TypeDDLEvent
	return w
}

func newWrapSyncPointEvent(serverID node.ID, e *pevent.SyncPointEvent) *wrapEvent {
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
	res := make([]pevent.ResolvedEvent, c.len)
	copy(res, c.cache[:c.len]) // must explicitly copy the slice to avoid data being covered by the next add operation
	c.reset()
	return res
}

func (c *resolvedTsCache) reset() {
	c.len = 0
}

type changefeedStatus struct {
	changefeedID common.ChangeFeedID
	filter       filter.Filter

	dispatchers sync.Map // common.DispatcherID -> *atomic.Pointer[dispatcherStat]

	availableMemoryQuota sync.Map // nodeID -> atomic.Uint64 (memory quota in bytes)
	minSentTs            atomic.Uint64
	scanInterval         atomic.Int64
	reportBandState      atomic.Int32
	fastBandState        atomic.Int32
	slowBandState        atomic.Int32

	scanWindowController *adaptiveScanWindowController
	syncPointInterval    time.Duration
}

func newChangefeedStatus(changefeedID common.ChangeFeedID, syncPointInterval time.Duration) *changefeedStatus {
	return newChangefeedStatusWithScanWindow(
		changefeedID,
		syncPointInterval,
		isScanWindowEnabled(),
	)
}

func isScanWindowEnabled() bool {
	cfg := config.GetGlobalServerConfig()
	if cfg == nil || cfg.Debug == nil || cfg.Debug.EventService == nil {
		return true
	}
	return cfg.Debug.EventService.EnableScanWindow
}

func newChangefeedStatusWithScanWindow(
	changefeedID common.ChangeFeedID,
	syncPointInterval time.Duration,
	enableScanWindow bool,
) *changefeedStatus {
	status := &changefeedStatus{
		changefeedID:      changefeedID,
		syncPointInterval: syncPointInterval,
	}
	if enableScanWindow {
		status.scanWindowController = newAdaptiveScanWindowController(time.Now())
	}
	status.scanInterval.Store(int64(defaultScanInterval))

	return status
}

func (c *changefeedStatus) addDispatcher(id common.DispatcherID, dispatcher *atomic.Pointer[dispatcherStat]) {
	c.dispatchers.Store(id, dispatcher)
}

func (c *changefeedStatus) removeDispatcher(id common.DispatcherID) {
	c.dispatchers.Delete(id)
}

func (c *changefeedStatus) isEmpty() bool {
	empty := true
	c.dispatchers.Range(func(key, value any) bool {
		empty = false
		return false // stop iteration
	})
	return empty
}

func (c *changefeedStatus) isSyncpointEnabled() bool {
	return c.syncPointInterval > 0
}
