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
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	resolvedTsCacheSize = 512
	basicChannelSize    = 2048

	defaultMaxBatchSize            = 128
	defaultFlushResolvedTsInterval = 25 * time.Millisecond

	defaultReportDispatcherStatToStoreInterval = time.Second * 10

	maxReadyEventIntervalSeconds = 10
	// defaultSendResolvedTsInterval use to control whether to send a resolvedTs event to the dispatcher when its scan is skipped.
	defaultSendResolvedTsInterval = time.Second * 2
)

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventStore is the source of the events, eventBroker get the events from the eventStore.
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	mounter     event.Mounter
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender
	pdClock   pdutil.Clock

	// changefeedMap is used to track the changefeed status.
	changefeedMap sync.Map // common.ChangeFeedID -> *changefeedStatus

	// All the dispatchers that register to the eventBroker.
	dispatchers sync.Map

	// dispatcherID -> dispatcherStat map, track all table trigger dispatchers.
	tableTriggerDispatchers sync.Map

	// taskChan is used to send the scan tasks to the scan workers.
	taskChan []chan scanTask

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh     []chan *wrapEvent
	redoMessageCh []chan *wrapEvent

	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc
	g      *errgroup.Group

	// metricsCollector handles all metrics collection and reporting
	metricsCollector *metricsCollector

	scanRateLimiter  *rate.Limiter
	scanLimitInBytes uint64
}

func newEventBroker(
	ctx context.Context,
	id uint64,
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mc messaging.MessageSender,
	tz *time.Location,
	integrity *integrity.Config,
) *eventBroker {
	// These numbers are define by real test result.
	// We noted that:
	// 1. When the number of send message workers is too small, the lag of the resolvedTs keep in a high level.
	// 2. When the number of send message workers is too large, the lag of the resolvedTs has spikes.
	// And when the number of send message workers is x, the lag of the resolvedTs is stable.
	sendMessageWorkerCount := config.DefaultBasicEventHandlerConcurrency
	scanWorkerCount := config.DefaultBasicEventHandlerConcurrency * 4

	scanTaskQueueSize := config.GetGlobalServerConfig().Debug.EventService.ScanTaskQueueSize / scanWorkerCount
	sendMessageQueueSize := basicChannelSize * 4

	scanLimitInBytes := config.GetGlobalServerConfig().Debug.EventService.ScanLimitInBytes

	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)

	// TODO: Retrieve the correct pdClock from the context once multiple upstreams are supported.
	// For now, since there is only one upstream, using the default pdClock is sufficient.
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	c := &eventBroker{
		tidbClusterID:           id,
		eventStore:              eventStore,
		pdClock:                 pdClock,
		mounter:                 event.NewMounter(tz, integrity),
		schemaStore:             schemaStore,
		changefeedMap:           sync.Map{},
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		msgSender:               mc,
		taskChan:                make([]chan scanTask, scanWorkerCount),
		messageCh:               make([]chan *wrapEvent, sendMessageWorkerCount),
		redoMessageCh:           make([]chan *wrapEvent, sendMessageWorkerCount),
		cancel:                  cancel,
		g:                       g,
		scanRateLimiter:         rate.NewLimiter(rate.Limit(scanLimitInBytes), scanLimitInBytes),
		scanLimitInBytes:        uint64(scanLimitInBytes),
	}

	// Initialize metrics collector
	c.metricsCollector = newMetricsCollector(c)

	for i := 0; i < sendMessageWorkerCount; i++ {
		c.messageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i, messaging.EventCollectorTopic)
		})
		c.redoMessageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i, messaging.RedoEventCollectorTopic)
		})
	}

	for i := 0; i < scanWorkerCount; i++ {
		taskChan := make(chan scanTask, scanTaskQueueSize)
		c.taskChan[i] = taskChan
		g.Go(func() error {
			return c.runScanWorker(ctx, taskChan)
		})
	}

	g.Go(func() error {
		return c.tickTableTriggerDispatchers(ctx)
	})

	g.Go(func() error {
		return c.logUninitializedDispatchers(ctx)
	})

	g.Go(func() error {
		return c.reportDispatcherStatToStore(ctx, defaultReportDispatcherStatToStoreInterval)
	})

	g.Go(func() error {
		return c.metricsCollector.Run(ctx)
	})

	log.Info("new event broker created", zap.Uint64("id", id), zap.Uint64("scanLimitInBytes", c.scanLimitInBytes))
	return c
}

func (c *eventBroker) sendDML(remoteID node.ID, batchEvent *event.BatchDMLEvent, d *dispatcherStat) {
	doSendDML := func(e *event.BatchDMLEvent) {
		// Send the DML event
		if e != nil && len(e.DMLEvents) > 0 {
			c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- newWrapBatchDMLEvent(remoteID, e)
			updateMetricEventServiceSendKvCount(d.info.GetMode(), float64(e.Len()))
		}
	}

	var (
		idx          int
		lastStartTs  uint64
		lastCommitTs uint64
	)
	for {
		if idx >= len(batchEvent.DMLEvents) {
			break
		}
		dml := batchEvent.DMLEvents[idx]
		if c.hasSyncPointEventsBeforeTs(dml.GetCommitTs(), d) {
			events := batchEvent.PopHeadDMLEvents(idx)
			doSendDML(events)
			// Reset the index to 1 to process the next event after `dml` in next loop
			idx = 1
			// Emit sync point event if needed
			c.emitSyncPointEventIfNeeded(dml.GetCommitTs(), d, remoteID)
		} else {
			idx++
		}
		// Set sequence number for the event
		dml.Seq = d.seq.Add(1)
		dml.Epoch = d.epoch

		lastStartTs = dml.GetStartTs()
		lastCommitTs = dml.GetCommitTs()
		log.Debug("send dml event to dispatcher",
			zap.Stringer("changefeedID", d.changefeedStat.changefeedID),
			zap.Stringer("dispatcherID", d.id), zap.Int64("tableID", d.info.GetTableSpan().GetTableID()),
			zap.Uint64("seq", dml.Seq),
			zap.Uint64("lastCommitTs", lastCommitTs), zap.Uint64("lastStartTs", lastStartTs))
	}
	if lastCommitTs != 0 {
		d.updateScanRange(lastCommitTs, lastStartTs)
	}
	doSendDML(batchEvent)
}

func (c *eventBroker) sendDDL(ctx context.Context, remoteID node.ID, e *event.DDLEvent, d *dispatcherStat) {
	c.emitSyncPointEventIfNeeded(e.FinishedTs, d, remoteID)
	e.DispatcherID = d.id
	e.Seq = d.seq.Add(1)
	e.Epoch = d.epoch
	ddlEvent := newWrapDDLEvent(remoteID, e)
	select {
	case <-ctx.Done():
		log.Error("send ddl event failed", zap.Error(ctx.Err()))
		return
	case c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- ddlEvent:
		updateMetricEventServiceSendDDLCount(d.info.GetMode())
	}

	log.Info("send ddl event to dispatcher",
		zap.Stringer("changefeedID", d.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", d.id),
		zap.Int64("DDLSpanTableID", d.info.GetTableSpan().TableID),
		zap.Int64("EventTableID", e.GetTableID()),
		zap.String("query", e.Query), zap.Uint64("commitTs", e.FinishedTs),
		zap.Uint64("seq", e.Seq), zap.Int64("mode", d.info.GetMode()))
}

func (c *eventBroker) sendSignalResolvedTs(d *dispatcherStat) {
	// Can't send resolvedTs if there was a interrupted scan task happened before.
	// d.lastScannedStartTs.Load() != 0 indicates that there was a interrupted scan task happened before.
	if time.Since(d.lastSentResolvedTsTime.Load()) < defaultSendResolvedTsInterval || d.lastScannedStartTs.Load() != 0 {
		return
	}
	watermark := d.sentResolvedTs.Load()
	c.sendResolvedTs(d, watermark)
}

func (c *eventBroker) sendResolvedTs(d *dispatcherStat, watermark uint64) {
	remoteID := node.ID(d.info.GetServerID())
	c.emitSyncPointEventIfNeeded(watermark, d, remoteID)
	re := event.NewResolvedEvent(watermark, d.id, d.epoch)
	re.Seq = d.seq.Load()
	resolvedEvent := newWrapResolvedEvent(remoteID, re)
	c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- resolvedEvent
	d.updateSentResolvedTs(watermark)
	updateMetricEventServiceSendResolvedTsCount(d.info.GetMode())
}

func (c *eventBroker) sendNotReusableEvent(
	server node.ID,
	d *dispatcherStat,
) {
	event := event.NewNotReusableEvent(d.info.GetID())
	wrapEvent := newWrapNotReusableEvent(server, event)

	// must success unless we can do retry later
	c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- wrapEvent
	updateMetricEventServiceSendCommandCount(d.info.GetMode())
}

func (c *eventBroker) getMessageCh(workerIndex int, isRedo bool) chan *wrapEvent {
	if isRedo {
		return c.redoMessageCh[workerIndex]
	}
	return c.messageCh[workerIndex]
}

func (c *eventBroker) runScanWorker(ctx context.Context, taskChan chan scanTask) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case task := <-taskChan:
			c.doScan(ctx, task)
		}
	}
}

// TODO: maybe event driven model is better. It is coupled with the detail implementation of
// the schemaStore, we will refactor it later.
func (c *eventBroker) tickTableTriggerDispatchers(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				stat := value.(*atomic.Pointer[dispatcherStat]).Load()
				if !c.checkAndSendReady(stat) {
					return true
				}
				c.sendHandshakeIfNeed(stat)
				startTs := stat.sentResolvedTs.Load()
				remoteID := node.ID(stat.info.GetServerID())
				keyspaceMeta := common.KeyspaceMeta{
					ID:   stat.info.GetTableSpan().KeyspaceID,
					Name: stat.info.GetChangefeedID().Keyspace(),
				}
				ddlEvents, endTs, err := c.schemaStore.FetchTableTriggerDDLEvents(keyspaceMeta, key.(common.DispatcherID), stat.filter, startTs, 100)
				if err != nil {
					log.Error("table trigger ddl events fetch failed", zap.Uint32("keyspaceID", stat.info.GetTableSpan().KeyspaceID), zap.Stringer("dispatcherID", stat.id), zap.Error(err))
					return true
				}
				stat.receivedResolvedTs.Store(endTs)
				for _, e := range ddlEvents {
					ep := &e
					c.sendDDL(ctx, remoteID, ep, stat)
				}
				if endTs > startTs {
					// After all the events are sent, we send the watermark to the dispatcher.
					c.sendResolvedTs(stat, endTs)
				}
				return true
			})
		}
	}
}

func (c *eventBroker) logUninitializedDispatchers(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	isUninitialized := func(d *dispatcherStat) bool {
		return !d.isRemoved.Load() && d.seq.Load() == 0
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isUninitialized(dispatcher) {
					log.Info("dispatcher not reset",
						zap.Stringer("changefeedID", dispatcher.changefeedStat.changefeedID),
						zap.Any("dispatcherID", dispatcher.id))
				}
				return true
			})
			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isUninitialized(dispatcher) {
					log.Info("table trigger dispatcher not reset",
						zap.Stringer("changefeedID", dispatcher.changefeedStat.changefeedID),
						zap.Any("dispatcherID", dispatcher.id))
				}
				return true
			})
		}
	}
}

// getScanTaskDataRange determines the valid data range for scanning a given task.
// It checks various conditions (dispatcher status, DDL state, max commit ts of dml event)
// to decide whether scanning is needed and returns the appropriate time range.
// If no valid range is found, it returns an empty DataRange.
func (c *eventBroker) getScanTaskDataRange(task scanTask) (bool, common.DataRange) {
	// 1. Get the data range of the dispatcher.
	dataRange, needScan := task.getDataRange()
	if !needScan {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		return false, common.DataRange{}
	}

	keyspaceMeta := common.KeyspaceMeta{
		ID:   task.info.GetTableSpan().KeyspaceID,
		Name: task.changefeedStat.changefeedID.Keyspace(),
	}

	// 2. Constrain the data range by the ddl state of the table.
	ddlState, err := c.schemaStore.GetTableDDLEventState(keyspaceMeta, task.info.GetTableSpan().TableID)
	if err != nil {
		log.Error("GetTableDDLEventState failed", zap.Uint32("keyspaceID", task.info.GetTableSpan().KeyspaceID), zap.Int64("tableID", task.info.GetTableSpan().TableID), zap.Error(err))
		return false, common.DataRange{}
	}
	dataRange.CommitTsEnd = min(dataRange.CommitTsEnd, ddlState.ResolvedTs)

	if dataRange.CommitTsEnd <= dataRange.CommitTsStart {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		return false, common.DataRange{}
	}

	// 3. Check whether there is any events in the data range
	// Note: target range is (dataRange.CommitTsStart-dataRange.LastScannedTxnStartTs, dataRange.CommitTsEnd]
	// when `dataRange.CommitTsStart` equals `task.eventStoreCommitTs.Load()`,
	// it is difficult to determine whether any txn events with a commitTs of `dataRange.CommitTsStart` remain unscanned.
	// because multiple transactions may have the same commit ts.
	// so we take the risk to do a useless scan.
	noDMLEvent := dataRange.CommitTsStart > task.eventStoreCommitTs.Load()
	noDDLEvent := dataRange.CommitTsStart >= ddlState.MaxEventCommitTs
	if noDMLEvent && noDDLEvent {
		// The dispatcher has no new events. In such case, we don't need to scan the event store.
		// We just send the watermark to the dispatcher.
		c.sendResolvedTs(task, dataRange.CommitTsEnd)
		return false, common.DataRange{}
	}
	return true, dataRange
}

// scanReady checks if the dispatcher needs to scan the event store/schema store.
// If the dispatcher needs to scan the event store/schema store, it returns true.
// If the dispatcher does not need to scan the event store, it send the watermark to the dispatcher.
//
// Note: A true return value only indicates potential scanning need,
// final determination occurs when the scanTask is actully processed.
func (c *eventBroker) scanReady(task scanTask) bool {
	if task.isRemoved.Load() {
		return false
	}

	if task.isTaskScanning.Load() {
		return false
	}

	// If the dispatcher is not ready, we don't need do the scan.
	if !c.checkAndSendReady(task) {
		return false
	}

	c.sendHandshakeIfNeed(task)

	ok, _ := c.getScanTaskDataRange(task)
	return ok
}

func (c *eventBroker) checkAndSendReady(task scanTask) bool {
	// only dispatcher with epoch 0 need send ready event.
	if task.epoch == 0 {
		now := time.Now().Unix()
		lastSendTime := task.lastReadySendTime.Load()
		currentInterval := task.readyInterval.Load()
		if now-lastSendTime < currentInterval {
			return false
		}
		remoteID := node.ID(task.info.GetServerID())
		event := event.NewReadyEvent(task.info.GetID())
		wrapEvent := newWrapReadyEvent(remoteID, event)
		c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- wrapEvent
		log.Debug("send ready event to dispatcher",
			zap.Stringer("changefeedID", task.changefeedStat.changefeedID), zap.Stringer("dispatcherID", task.id))
		task.lastReadySendTime.Store(now)
		newInterval := currentInterval * 2
		if newInterval > maxReadyEventIntervalSeconds {
			newInterval = maxReadyEventIntervalSeconds
		}
		task.readyInterval.Store(newInterval)
		updateMetricEventServiceSendCommandCount(task.info.GetMode())
		return false
	}
	return true
}

func (c *eventBroker) sendHandshakeIfNeed(task scanTask) {
	// Fast path.
	if task.isHandshaked() {
		return
	}

	task.handshakeLock.Lock()
	defer task.handshakeLock.Unlock()

	if task.isHandshaked() {
		return
	}

	remoteID := node.ID(task.info.GetServerID())
	event := event.NewHandshakeEvent(task.id, task.startTs, task.epoch, task.startTableInfo)
	log.Info("send handshake event to dispatcher",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", task.id),
		zap.Int64("tableID", task.info.GetTableSpan().GetTableID()),
		zap.Uint64("commitTs", event.GetCommitTs()),
		zap.Uint64("epoch", event.GetEpoch()),
		zap.Uint64("seq", event.GetSeq()))
	wrapEvent := newWrapHandshakeEvent(remoteID, event)
	c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- wrapEvent
	updateMetricEventServiceSendCommandCount(task.info.GetMode())
	// Send handshake event to channel before calling `setHandshaked`
	// This ensures the handshake event precedes any subsequent data events.
	task.setHandshaked()
}

// hasSyncPointEventBeforeTs checks if there is any sync point events before the given ts.
func (c *eventBroker) hasSyncPointEventsBeforeTs(ts uint64, d *dispatcherStat) bool {
	return d.enableSyncPoint && ts > d.nextSyncPoint.Load()
}

// emitSyncPointEventIfNeeded emits a sync point event if the current ts is greater than the next sync point, and updates the next sync point.
// We need call this function every time we send a event(whether dml/ddl/resolvedTs),
// thus to ensure the sync point event is in correct order for each dispatcher.
func (c *eventBroker) emitSyncPointEventIfNeeded(ts uint64, d *dispatcherStat, remoteID node.ID) {
	for d.enableSyncPoint && ts > d.nextSyncPoint.Load() {
		commitTs := d.nextSyncPoint.Load()
		d.nextSyncPoint.Store(oracle.GoTimeToTS(oracle.GetTimeFromTS(commitTs).Add(d.syncPointInterval)))

		e := event.NewSyncPointEvent(d.id, commitTs, d.seq.Add(1), d.epoch)
		log.Debug("send syncpoint event to dispatcher",
			zap.Stringer("changefeedID", d.changefeedStat.changefeedID),
			zap.Stringer("dispatcherID", d.id), zap.Int64("tableID", d.info.GetTableSpan().GetTableID()),
			zap.Uint64("commitTs", e.GetCommitTs()), zap.Uint64("seq", e.GetSeq()))

		syncPointEvent := newWrapSyncPointEvent(remoteID, e)
		c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- syncPointEvent
	}
}

func (c *eventBroker) calculateScanLimit(task scanTask) scanLimit {
	return scanLimit{
		maxDMLBytes: task.getCurrentScanLimitInBytes(),
	}
}

func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	var interrupted bool
	defer func() {
		task.isTaskScanning.Store(false)
		if interrupted {
			c.pushTask(task, false)
		}
	}()

	var (
		remoteID     = node.ID(task.info.GetServerID())
		changefeedID = task.info.GetChangefeedID()
	)
	if task.isRemoved.Load() {
		return
	}
	// If the target is not ready to send, we don't need to scan the event store.
	// To avoid the useless scan task.
	if !c.msgSender.IsReadyToSend(remoteID) {
		log.Info("The remote target is not ready, skip scan",
			zap.Stringer("changefeed", changefeedID),
			zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()),
			zap.String("remote", remoteID.String()))
		return
	}

	needScan, dataRange := c.getScanTaskDataRange(task)
	if !needScan {
		return
	}

	// TODO: Currently, this rate limit does not take into account the priority of each task, which may lead to situations where certain tasks are starved and cannot be scheduled for a long time.
	// For example, there are 10 dispatchers in the incremental scanning phase, with a large amount of traffic and a continuous stream of tasks, which occupy all the rate limits.
	// At this time, a dispatcher with very little traffic comes in. It cannot apply for the rate limit, resulting in it being starved and unable to be scheduled for a long time.
	// Therefore, we need to consider the priority of each task in the future and allocate rate limits based on priority.
	// My current idea is to divide rate limits into 3 different levels, and decide which rate limit to use according to lastScanBytes.
	if !c.scanRateLimiter.AllowN(time.Now(), int(task.lastScanBytes.Load())) {
		log.Debug("scan rate limit exceeded",
			zap.Stringer("dispatcher", task.id),
			zap.Int64("lastScanBytes", task.lastScanBytes.Load()),
			zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()))
		return
	}

	item, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		log.Panic("cannot found the changefeed status", zap.Any("changefeed", changefeedID.String()))
	}

	status := item.(*changefeedStatus)
	item, ok = status.availableMemoryQuota.Load(remoteID)
	if !ok {
		log.Info("available memory quota is not set, skip scan",
			zap.String("changefeed", changefeedID.String()), zap.String("remote", remoteID.String()))
		return
	}

	available := item.(*atomic.Uint64)
	if available.Load() < c.scanLimitInBytes {
		task.resetScanLimit()
	}

	sl := c.calculateScanLimit(task)
	ok = allocQuota(available, uint64(sl.maxDMLBytes))
	if !ok {
		log.Debug("changefeed available memory quota is not enough, skip scan",
			zap.String("changefeed", changefeedID.String()),
			zap.String("remote", remoteID.String()),
			zap.Uint64("available", available.Load()),
			zap.Uint64("required", uint64(sl.maxDMLBytes)))
		c.sendSignalResolvedTs(task)
		metrics.EventServiceSkipScanCount.WithLabelValues("changefeed_quota").Inc()
		return
	}

	if uint64(sl.maxDMLBytes) > task.availableMemoryQuota.Load() {
		log.Debug("dispatcher available memory quota is not enough, skip scan", zap.Stringer("dispatcher", task.id), zap.Uint64("available", task.availableMemoryQuota.Load()), zap.Int64("required", int64(sl.maxDMLBytes)))
		c.sendSignalResolvedTs(task)
		metrics.EventServiceSkipScanCount.WithLabelValues("dispatcher_quota").Inc()
		return
	}

	scanner := newEventScanner(c.eventStore, c.schemaStore, c.mounter, task.info.GetMode())
	scannedBytes, events, interrupted, err := scanner.scan(ctx, task, dataRange, sl)
	if scannedBytes < 0 {
		releaseQuota(available, uint64(sl.maxDMLBytes))
	} else if scannedBytes >= 0 && scannedBytes < sl.maxDMLBytes {
		releaseQuota(available, uint64(sl.maxDMLBytes-scannedBytes))
	}

	if interrupted {
		metrics.EventServiceInterruptScanCount.Inc()
	}

	if err != nil {
		log.Error("scan events failed",
			zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
			zap.Stringer("dispatcherID", task.id), zap.Int64("tableID", task.info.GetTableSpan().GetTableID()),
			zap.Any("dataRange", dataRange), zap.Uint64("receivedResolvedTs", task.receivedResolvedTs.Load()),
			zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()), zap.Error(err))
		return
	}

	if scannedBytes > int64(c.scanLimitInBytes) {
		log.Info("scan bytes exceeded the limit, there must be a big transaction", zap.Stringer("dispatcher", task.id), zap.Int64("scannedBytes", scannedBytes), zap.Int64("limit", int64(c.scanLimitInBytes)))
		scannedBytes = int64(c.scanLimitInBytes)
	}
	task.lastScanBytes.Store(scannedBytes)

	for _, e := range events {
		if task.isRemoved.Load() {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		switch e.GetType() {
		case event.TypeBatchDMLEvent:
			dmls, ok := e.(*event.BatchDMLEvent)
			if !ok {
				log.Panic("expect a DMLEvent, but got", zap.Any("event", e))
			}
			c.sendDML(remoteID, dmls, task)
		case event.TypeDDLEvent:
			ddl, ok := e.(*event.DDLEvent)
			if !ok {
				log.Panic("expect a DDLEvent, but got", zap.Any("event", e))
			}
			c.sendDDL(ctx, remoteID, ddl, task)
		case event.TypeResolvedEvent:
			re, ok := e.(event.ResolvedEvent)
			if !ok {
				log.Panic("expect a ResolvedEvent, but got", zap.Any("event", e))
			}
			c.sendResolvedTs(task, re.ResolvedTs)
		default:
			log.Panic("unknown event type", zap.Any("event", e))
		}
	}
	task.info.GetMode()
	// Update metrics
	metricEventBrokerScanTaskCount.Inc()
}

func allocQuota(quota *atomic.Uint64, nBytes uint64) bool {
	for {
		available := quota.Load()
		if available < nBytes {
			return false
		}
		if quota.CompareAndSwap(available, available-nBytes) {
			return true
		}
	}
}

func releaseQuota(quota *atomic.Uint64, nBytes uint64) {
	quota.Add(nBytes)
}

func (c *eventBroker) runSendMessageWorker(ctx context.Context, workerIndex int, topic string) error {
	ticker := time.NewTicker(defaultFlushResolvedTsInterval)
	defer ticker.Stop()

	resolvedTsCacheMap := make(map[node.ID]*resolvedTsCache)
	messageCh := c.getMessageCh(workerIndex, topic == messaging.RedoEventCollectorTopic)
	batchM := make([]*wrapEvent, 0, defaultMaxBatchSize)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case m := <-messageCh:
			batchM = append(batchM, m)
		LOOP:
			for {
				select {
				case moreM := <-messageCh:
					batchM = append(batchM, moreM)
					if len(batchM) > defaultMaxBatchSize {
						break LOOP
					}
				default:
					break LOOP
				}
			}
			for _, m = range batchM {
				if m.msgType == event.TypeResolvedEvent {
					c.handleResolvedTs(ctx, resolvedTsCacheMap, m, workerIndex, topic)
					continue
				}
				tMsg := messaging.NewSingleTargetMessage(
					m.serverID,
					topic,
					m.e,
					uint64(workerIndex),
				)
				// Note: we need to flush the resolvedTs cache before sending the message
				// to keep the order of the resolvedTs and the message.
				c.flushResolvedTs(ctx, resolvedTsCacheMap[m.serverID], m.serverID, workerIndex, topic)
				c.sendMsg(ctx, tMsg, m.postSendFunc)
				m.reset()
			}
			batchM = batchM[:0]

		case <-ticker.C:
			for serverID, cache := range resolvedTsCacheMap {
				c.flushResolvedTs(ctx, cache, serverID, workerIndex, topic)
			}
		}
	}
}

func (c *eventBroker) handleResolvedTs(ctx context.Context, cacheMap map[node.ID]*resolvedTsCache, m *wrapEvent, workerIndex int, topic string) {
	defer m.reset()
	cache, ok := cacheMap[m.serverID]
	if !ok {
		cache = newResolvedTsCache(resolvedTsCacheSize)
		cacheMap[m.serverID] = cache
	}
	cache.add(m.resolvedTsEvent)
	if cache.isFull() {
		c.flushResolvedTs(ctx, cache, m.serverID, workerIndex, topic)
	}
}

func (c *eventBroker) flushResolvedTs(ctx context.Context, cache *resolvedTsCache, serverID node.ID, workerIndex int, topic string) {
	if cache == nil || cache.len == 0 {
		return
	}
	msg := event.NewBatchResolvedEvent(cache.getAll())
	if len(msg.Events) == 0 {
		return
	}
	tMsg := messaging.NewSingleTargetMessage(
		serverID,
		topic,
		msg,
		uint64(workerIndex),
	)
	c.sendMsg(ctx, tMsg, nil)
}

func (c *eventBroker) sendMsg(ctx context.Context, tMsg *messaging.TargetMessage, postSendMsg func()) {
	start := time.Now()
	congestedRetryInterval := time.Millisecond * 10
	// Send the message to messageCenter. Retry if to send failed.
	for {
		select {
		case <-ctx.Done():
			log.Error("send message failed", zap.Error(ctx.Err()))
			return
		default:
		}
		// Send the message to the dispatcher.
		err := c.msgSender.SendEvent(tMsg)
		if err != nil {
			_, ok := err.(errors.AppError)
			log.Debug("send msg failed, retry it later", zap.Error(err), zap.Stringer("tMsg", tMsg), zap.Bool("castOk", ok))
			if strings.Contains(err.Error(), "congested") {
				log.Debug("send message failed since the message is congested, retry it laster", zap.Error(err))
				// Wait for a while and retry to avoid the dropped message flood.
				time.Sleep(congestedRetryInterval)
				continue
			} else {
				log.Info("send message failed, drop it", zap.Error(err), zap.Stringer("tMsg", tMsg))
				// Drop the message, and return.
				// If the dispatcher finds the events are not continuous, it will send a reset message.
				// And the broker will send the missed events to the dispatcher again.
				return
			}
		}
		if postSendMsg != nil {
			postSendMsg()
		}
		metricEventServiceSendEventDuration.Observe(time.Since(start).Seconds())
		return
	}
}

func (c *eventBroker) reportDispatcherStatToStore(ctx context.Context, tickInterval time.Duration) error {
	ticker := time.NewTicker(tickInterval)
	log.Info("update dispatcher send ts goroutine is started")
	isInactiveDispatcher := func(d *dispatcherStat) bool {
		return d.isHandshaked() && time.Since(time.Unix(d.lastReceivedHeartbeatTime.Load(), 0)) > heartbeatTimeout
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			inActiveDispatchers := make([]*dispatcherStat, 0)
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				checkpointTs := dispatcher.checkpointTs.Load()
				if checkpointTs > 0 && checkpointTs < dispatcher.sentResolvedTs.Load() {
					c.eventStore.UpdateDispatcherCheckpointTs(dispatcher.id, checkpointTs)
				}
				if isInactiveDispatcher(dispatcher) {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isInactiveDispatcher(dispatcher) {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			for _, d := range inActiveDispatchers {
				log.Warn("remove in-active dispatcher",
					zap.Stringer("changefeedID", d.changefeedStat.changefeedID),
					zap.Stringer("dispatcherID", d.id), zap.Time("lastReceivedHeartbeatTime", time.Unix(d.lastReceivedHeartbeatTime.Load(), 0)))
				c.removeDispatcher(d.info)
			}
		}
	}
}

func (c *eventBroker) close() {
	c.cancel()
	_ = c.g.Wait()
}

func (c *eventBroker) onNotify(d *dispatcherStat, resolvedTs uint64, commitTs uint64) {
	if d.onResolvedTs(resolvedTs) {
		d.lastReceivedResolvedTsTime.Store(time.Now())
		updateMetricEventStoreOutputResolved(d.info.GetMode())
		d.onLatestCommitTs(commitTs)
		if c.scanReady(d) {
			c.pushTask(d, true)
		}
	}
}

func (c *eventBroker) pushTask(d *dispatcherStat, force bool) {
	if d.isRemoved.Load() {
		return
	}

	// make sure only one scan task can run at the same time.
	if !d.isTaskScanning.CompareAndSwap(false, true) {
		return
	}

	if force {
		c.taskChan[d.scanWorkerIndex] <- d
	} else {
		timer := time.NewTimer(time.Millisecond * 10)
		select {
		case c.taskChan[d.scanWorkerIndex] <- d:
		case <-timer.C:
			d.isTaskScanning.Store(false)
		}
	}
}

func (c *eventBroker) getDispatcher(id common.DispatcherID) *atomic.Pointer[dispatcherStat] {
	stat, ok := c.dispatchers.Load(id)
	if ok {
		return stat.(*atomic.Pointer[dispatcherStat])
	}
	stat, ok = c.tableTriggerDispatchers.Load(id)
	if ok {
		return stat.(*atomic.Pointer[dispatcherStat])
	}
	return nil
}

func (c *eventBroker) addDispatcher(info DispatcherInfo) error {
	id := info.GetID()
	span := info.GetTableSpan()
	changefeedID := info.GetChangefeedID()

	status := c.getOrSetChangefeedStatus(changefeedID)
	dispatcher := newDispatcherStat(info, uint64(len(c.taskChan)), uint64(len(c.messageCh)), nil, status)
	dispatcherPtr := &atomic.Pointer[dispatcherStat]{}
	dispatcherPtr.Store(dispatcher)
	status.addDispatcher(id, dispatcherPtr)
	if span.Equal(common.KeyspaceDDLSpan(span.KeyspaceID)) {
		c.tableTriggerDispatchers.Store(id, dispatcherPtr)
		log.Info("table trigger dispatcher register dispatcher",
			zap.Uint64("clusterID", c.tidbClusterID),
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("startTs", info.GetStartTs()))
		return nil
	}

	start := time.Now()
	success := c.eventStore.RegisterDispatcher(
		changefeedID,
		id,
		span,
		info.GetStartTs(),
		func(resolvedTs uint64, latestCommitTs uint64) {
			d := dispatcherPtr.Load()
			// If the dispatcher is removed, just ignore the notification.
			if d.isRemoved.Load() {
				return
			}
			c.onNotify(d, resolvedTs, latestCommitTs)
		},
		info.IsOnlyReuse(),
		info.GetBdrMode(),
	)

	if !success {
		if !info.IsOnlyReuse() {
			log.Error("register dispatcher to eventStore failed",
				zap.Stringer("changefeedID", changefeedID),
				zap.Stringer("dispatcherID", id), zap.Int64("tableID", span.GetTableID()),
				zap.Uint64("startTs", info.GetStartTs()), zap.String("span", common.FormatTableSpan(span)))
		}
		status.removeDispatcher(id)
		if status.isEmpty() {
			c.changefeedMap.Delete(changefeedID)
		}
		c.sendNotReusableEvent(node.ID(info.GetServerID()), dispatcher)
		return nil
	}

	keyspaceMeta := common.KeyspaceMeta{
		ID:   span.KeyspaceID,
		Name: changefeedID.Keyspace(),
	}
	err := c.schemaStore.RegisterTable(keyspaceMeta, span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Error("register table to schemaStore failed",
			zap.Uint32("keyspaceID", span.KeyspaceID),
			zap.Stringer("dispatcherID", id), zap.Int64("tableID", span.GetTableID()),
			zap.Uint64("startTs", info.GetStartTs()), zap.String("span", common.FormatTableSpan(span)),
			zap.Error(err),
		)
		status.removeDispatcher(id)
		if status.isEmpty() {
			c.changefeedMap.Delete(changefeedID)
		}
		return err
	}
	c.dispatchers.Store(id, dispatcherPtr)
	c.metricsCollector.metricDispatcherCount.Inc()
	log.Info("register dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID),
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("dispatcherID", id),
		zap.Int64("mode", info.GetMode()),
		zap.Int64("tableID", span.GetTableID()),
		zap.String("span", common.FormatTableSpan(span)),
		zap.Uint64("startTs", info.GetStartTs()),
		zap.String("txnAtomocity", string(info.GetTxnAtomicity())),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (c *eventBroker) removeDispatcher(dispatcherInfo DispatcherInfo) {
	id := dispatcherInfo.GetID()

	statPtr, ok := c.dispatchers.Load(id)
	if !ok {
		statPtr, ok = c.tableTriggerDispatchers.Load(id)
		if !ok {
			return
		}
		c.tableTriggerDispatchers.Delete(id)
	}
	stat := statPtr.(*atomic.Pointer[dispatcherStat]).Load()

	stat.changefeedStat.removeDispatcher(id)
	c.metricsCollector.metricDispatcherCount.Dec()
	changefeedID := dispatcherInfo.GetChangefeedID()

	if stat.changefeedStat.isEmpty() {
		log.Info("All dispatchers for the changefeed are removed, remove the changefeed status",
			zap.Stringer("changefeedID", changefeedID),
		)
		c.changefeedMap.Delete(changefeedID)
		metrics.EventServiceAvailableMemoryQuotaGaugeVec.DeleteLabelValues(changefeedID.String())
	}

	stat.isRemoved.Store(true)
	c.eventStore.UnregisterDispatcher(changefeedID, id)

	span := dispatcherInfo.GetTableSpan()
	keyspaceMeta := common.KeyspaceMeta{
		ID:   span.KeyspaceID,
		Name: changefeedID.Keyspace(),
	}
	c.schemaStore.UnregisterTable(keyspaceMeta, span.TableID)
	c.dispatchers.Delete(id)

	log.Info("remove dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID), zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("dispatcherID", id), zap.Int64("tableID", dispatcherInfo.GetTableSpan().GetTableID()),
		zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
	)
}

func (c *eventBroker) resetDispatcher(dispatcherInfo DispatcherInfo) error {
	dispatcherID := dispatcherInfo.GetID()
	start := time.Now()
	statPtr := c.getDispatcher(dispatcherID)
	if statPtr == nil {
		// The dispatcher is not registered, ignore it.
		log.Warn("reset a non-exist dispatcher, ignore it",
			zap.Stringer("changefeedID", dispatcherInfo.GetChangefeedID()),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", dispatcherInfo.GetTableSpan().GetTableID()),
			zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
			zap.Uint64("startTs", dispatcherInfo.GetStartTs()))
		return nil
	}
	metrics.EventServiceResetDispatcherCount.Inc()

	oldStat := statPtr.Load()
	// stale reset request, ignore it.
	if oldStat.epoch >= dispatcherInfo.GetEpoch() {
		return nil
	}

	// Mark the old dispatcher as removed.
	// No need to worry that the old dispatcher is still scanning,
	// because its data will be filtered by event collector because of stale epoch.
	oldStat.isRemoved.Store(true)

	// Create a new dispatcherStat and replace the old one.
	// The new dispatcherStat will be used for all future operations.
	changefeedID := dispatcherInfo.GetChangefeedID()
	span := dispatcherInfo.GetTableSpan()
	var tableInfo *common.TableInfo
	if !span.Equal(common.KeyspaceDDLSpan(span.KeyspaceID)) {
		var err error
		keyspaceMeta := common.KeyspaceMeta{
			ID:   span.KeyspaceID,
			Name: changefeedID.Keyspace(),
		}
		tableInfo, err = c.schemaStore.GetTableInfo(keyspaceMeta, span.GetTableID(), dispatcherInfo.GetStartTs())
		if err != nil {
			log.Error("get table info from schemaStore failed",
				zap.Stringer("changefeedID", changefeedID),
				zap.Stringer("dispatcherID", dispatcherID),
				zap.Int64("tableID", span.GetTableID()),
				zap.Uint64("startTs", dispatcherInfo.GetStartTs()),
				zap.String("span", common.FormatTableSpan(span)),
				zap.Error(err))
			return err
		}
	}
	status := c.getOrSetChangefeedStatus(changefeedID)
	newStat := newDispatcherStat(dispatcherInfo, uint64(len(c.taskChan)), uint64(len(c.messageCh)), tableInfo, status)
	newStat.copyStatistics(oldStat)

	for {
		if statPtr.CompareAndSwap(oldStat, newStat) {
			status.addDispatcher(dispatcherID, statPtr)
			break
		}
		log.Warn("reset dispatcher failed since the dispatcher is changed concurrently",
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", span.GetTableID()),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("oldStartTs", oldStat.info.GetStartTs()),
			zap.Uint64("newStartTs", dispatcherInfo.GetStartTs()),
			zap.Uint64("oldEpoch", oldStat.epoch),
			zap.Uint64("newEpoch", newStat.epoch))
		// The dispatcher is changed concurrently, retry it.
		oldStat = statPtr.Load()
		// stale reset request, ignore it.
		if oldStat.epoch >= dispatcherInfo.GetEpoch() {
			return nil
		}
		oldStat.isRemoved.Store(true)
	}

	log.Info("reset dispatcher",
		zap.Stringer("changefeedID", newStat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", newStat.id), zap.Int64("tableID", newStat.info.GetTableSpan().GetTableID()),
		zap.String("span", common.FormatTableSpan(newStat.info.GetTableSpan())),
		zap.Uint64("originStartTs", oldStat.info.GetStartTs()),
		zap.Uint64("newStartTs", dispatcherInfo.GetStartTs()),
		zap.Uint64("newEpoch", newStat.epoch),
		zap.Duration("resetTime", time.Since(start)))

	return nil
}

func (c *eventBroker) getOrSetChangefeedStatus(changefeedID common.ChangeFeedID) *changefeedStatus {
	stat, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		stat = newChangefeedStatus(changefeedID)
		log.Info("new changefeed status", zap.Stringer("changefeedID", changefeedID))
		c.changefeedMap.Store(changefeedID, stat)
	}
	return stat.(*changefeedStatus)
}

func (c *eventBroker) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	responseMap := make(map[string]*event.DispatcherHeartbeatResponse)
	for _, dp := range heartbeat.heartbeat.DispatcherProgresses {
		dispatcherPtr := c.getDispatcher(dp.DispatcherID)
		// Can't find the dispatcher, it means the dispatcher is removed.
		if dispatcherPtr == nil {
			response, ok := responseMap[heartbeat.serverID]
			if !ok {
				response = event.NewDispatcherHeartbeatResponse()
				responseMap[heartbeat.serverID] = response
			}
			response.Append(event.NewDispatcherState(dp.DispatcherID, event.DSStateRemoved))
			continue
		}
		dispatcher := dispatcherPtr.Load()
		// TODO: Should we check if the dispatcher's serverID is the same as the heartbeat's serverID?
		if dispatcher.checkpointTs.Load() < dp.CheckpointTs {
			dispatcher.checkpointTs.Store(dp.CheckpointTs)
		}
		// Update the last received heartbeat time to the current time.
		dispatcher.lastReceivedHeartbeatTime.Store(time.Now().Unix())
	}
	c.sendDispatcherResponse(responseMap)
}

func (c *eventBroker) handleCongestionControl(from node.ID, m *event.CongestionControl) {
	availables := m.GetAvailables()
	if len(availables) == 0 {
		return
	}

	holder := make(map[common.GID]uint64, len(availables))
	dispatcherAvailable := make(map[common.DispatcherID]uint64, len(availables))
	for _, item := range availables {
		holder[item.Gid] = item.Available
		for dispatcherID, available := range item.DispatcherAvailable {
			dispatcherAvailable[dispatcherID] = available
		}
	}

	c.changefeedMap.Range(func(k, v interface{}) bool {
		changefeedID := k.(common.ChangeFeedID)
		changefeed := v.(*changefeedStatus)
		available, ok := holder[changefeedID.ID()]
		if ok {
			changefeed.availableMemoryQuota.Store(from, atomic.NewUint64(available))
			metrics.EventServiceAvailableMemoryQuotaGaugeVec.WithLabelValues(changefeedID.String()).Set(float64(available))
		}
		return true
	})

	c.dispatchers.Range(func(k, v interface{}) bool {
		dispatcherID := k.(common.DispatcherID)
		dispatcher := v.(*atomic.Pointer[dispatcherStat]).Load()
		available, ok := dispatcherAvailable[dispatcherID]
		if ok {
			dispatcher.availableMemoryQuota.Store(available)
		}
		return true
	})
}

func (c *eventBroker) sendDispatcherResponse(responseMap map[string]*event.DispatcherHeartbeatResponse) {
	for serverID, response := range responseMap {
		msg := messaging.NewSingleTargetMessage(node.ID(serverID), messaging.EventCollectorTopic, response)
		c.msgSender.SendCommand(msg)
	}
}
