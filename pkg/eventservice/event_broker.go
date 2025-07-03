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
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	resolvedTsCacheSize = 512
	basicChannelSize    = 2048

	defaultMaxBatchSize            = 128
	defaultFlushResolvedTsInterval = 25 * time.Millisecond
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
	mounter     pevent.Mounter
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender
	pdClock   pdutil.Clock

	// changefeedMap is used to track the changefeed status.
	changefeedMap sync.Map
	// All the dispatchers that register to the eventBroker.
	dispatchers sync.Map
	// dispatcherID -> dispatcherStat map, track all table trigger dispatchers.
	tableTriggerDispatchers sync.Map

	// taskChan is used to send the scan tasks to the scan workers.
	taskChan []chan scanTask

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh []chan *wrapEvent

	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc
	g      *errgroup.Group

	// metricsCollector handles all metrics collection and reporting
	metricsCollector *metricsCollector
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

	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)

	// TODO: Retrieve the correct pdClock from the context once multiple upstreams are supported.
	// For now, since there is only one upstream, using the default pdClock is sufficient.
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)

	c := &eventBroker{
		tidbClusterID:           id,
		eventStore:              eventStore,
		pdClock:                 pdClock,
		mounter:                 pevent.NewMounter(tz, integrity),
		schemaStore:             schemaStore,
		changefeedMap:           sync.Map{},
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		msgSender:               mc,
		taskChan:                make([]chan scanTask, scanWorkerCount),
		messageCh:               make([]chan *wrapEvent, sendMessageWorkerCount),
		cancel:                  cancel,
		g:                       g,
	}
	// Initialize metrics collector
	c.metricsCollector = newMetricsCollector(c)

	for i := 0; i < sendMessageWorkerCount; i++ {
		c.messageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i)
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
		return c.logUnresetDispatchers(ctx)
	})

	g.Go(func() error {
		return c.reportDispatcherStatToStore(ctx)
	})

	g.Go(func() error {
		return c.metricsCollector.Run(ctx)
	})

	log.Info("new event broker created", zap.Uint64("id", id))
	return c
}

func (c *eventBroker) sendDML(remoteID node.ID, batchEvent *pevent.BatchDMLEvent, d *dispatcherStat) {
	doSendDML := func(e *pevent.BatchDMLEvent) {
		// Send the DML event
		if e != nil && len(e.DMLEvents) > 0 {
			c.getMessageCh(d.messageWorkerIndex) <- newWrapBatchDMLEvent(remoteID, e, d.getEventSenderState())
			metricEventServiceSendKvCount.Add(float64(e.Len()))
		}
	}

	i := 0
	for {
		if i >= len(batchEvent.DMLEvents) {
			break
		}
		dml := batchEvent.DMLEvents[i]
		// Set sequence number for the event
		dml.Seq = d.seq.Add(1)
		dml.Epoch = d.epoch.Load()
		if c.hasSyncPointEventsBeforeTs(dml.GetCommitTs(), d) {
			events := batchEvent.PopHeadDMLEvents(i)
			doSendDML(events)
			// Reset the index to 1 to process the next event after `dml` in next loop
			i = 1
			// Emit sync point event if needed
			c.emitSyncPointEventIfNeeded(dml.GetCommitTs(), d, remoteID)
		} else {
			i++
		}
	}
	doSendDML(batchEvent)
}

func (c *eventBroker) sendDDL(ctx context.Context, remoteID node.ID, e *pevent.DDLEvent, d *dispatcherStat) {
	c.emitSyncPointEventIfNeeded(e.FinishedTs, d, remoteID)
	e.DispatcherID = d.id
	e.Seq = d.seq.Add(1)
	e.Epoch = d.epoch.Load()
	log.Info("send ddl event to dispatcher",
		zap.Stringer("dispatcher", d.id),
		zap.Int64("dispatcherTableID", d.info.GetTableSpan().TableID),
		zap.String("query", e.Query),
		zap.Int64("eventTableID", e.TableID),
		zap.Uint64("commitTs", e.FinishedTs),
		zap.Uint64("seq", e.Seq))
	ddlEvent := newWrapDDLEvent(remoteID, e, d.getEventSenderState())
	select {
	case <-ctx.Done():
		return
	case c.getMessageCh(d.messageWorkerIndex) <- ddlEvent:
		metricEventServiceSendDDLCount.Inc()
	}
}

func (c *eventBroker) sendResolvedTs(d *dispatcherStat, watermark uint64) {
	remoteID := node.ID(d.info.GetServerID())
	c.emitSyncPointEventIfNeeded(watermark, d, remoteID)
	re := pevent.NewResolvedEvent(watermark, d.id, d.epoch.Load())
	resolvedEvent := newWrapResolvedEvent(
		remoteID,
		re,
		d.getEventSenderState(),
	)
	c.getMessageCh(d.messageWorkerIndex) <- resolvedEvent
	d.updateSentResolvedTs(watermark)
	metricEventServiceSendResolvedTsCount.Inc()
}

func (c *eventBroker) sendNotReusableEvent(
	server node.ID,
	d *dispatcherStat,
) {
	event := pevent.NewNotReusableEvent(d.info.GetID())
	wrapEvent := newWrapNotReusableEvent(server, event)

	// must success unless we can do retry later
	c.getMessageCh(d.messageWorkerIndex) <- wrapEvent
	metricEventServiceSendCommandCount.Inc()
}

func (c *eventBroker) getMessageCh(workerIndex int) chan *wrapEvent {
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
				dispatcherStat := value.(*dispatcherStat)
				if !c.checkAndSendReady(dispatcherStat) {
					return true
				}
				c.sendHandshakeIfNeed(dispatcherStat)
				startTs := dispatcherStat.sentResolvedTs.Load()
				remoteID := node.ID(dispatcherStat.info.GetServerID())
				// TODO: maybe limit 1 is enough.
				ddlEvents, endTs, err := c.schemaStore.FetchTableTriggerDDLEvents(dispatcherStat.filter, startTs, 100)
				if err != nil {
					log.Error("table trigger ddl events fetch failed", zap.Stringer("dispatcher", dispatcherStat.id), zap.Error(err))
					return true
				}
				for _, e := range ddlEvents {
					ep := &e
					c.sendDDL(ctx, remoteID, ep, dispatcherStat)
				}
				if endTs > startTs {
					// After all the events are sent, we send the watermark to the dispatcher.
					c.sendResolvedTs(dispatcherStat, endTs)
				}
				return true
			})
		}
	}
}

func (c *eventBroker) logUnresetDispatchers(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*dispatcherStat)
				if dispatcher.resetTs.Load() == 0 {
					log.Info("dispatcher not reset", zap.Any("dispatcher", dispatcher.id))
				}
				return true
			})
			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*dispatcherStat)
				if dispatcher.resetTs.Load() == 0 {
					log.Info("table trigger dispatcher not reset", zap.Any("dispatcher", dispatcher.id))
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
	// Only do scan when the dispatcher is ready to receive data.
	if !task.IsReadyRecevingData() {
		return false, common.DataRange{}
	}

	// 1. Get the data range of the dispatcher.
	dataRange, needScan := task.getDataRange()
	if !needScan {
		metricEventServiceSkipResolvedTsCount.Inc()
		return false, common.DataRange{}
	}

	// 2. Constrain the data range by the ddl state of the table.
	ddlState := c.schemaStore.GetTableDDLEventState(task.info.GetTableSpan().TableID)
	dataRange.EndTs = min(dataRange.EndTs, ddlState.ResolvedTs)

	if dataRange.EndTs <= dataRange.StartTs {
		metricEventServiceSkipResolvedTsCount.Inc()
		return false, common.DataRange{}
	}

	// 3. Check whether there is any events in the data range
	// Note: target range is (dataRange.StartTs, dataRange.EndTs]
	if dataRange.StartTs >= task.latestCommitTs.Load() &&
		dataRange.StartTs >= ddlState.MaxEventCommitTs {
		// The dispatcher has no new events. In such case, we don't need to scan the event store.
		// We just send the watermark to the dispatcher.
		c.sendResolvedTs(task, dataRange.EndTs)
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
	// If there is already a scan task running, skip this one.
	if task.isTaskScanning.Load() {
		return false
	}

	// If the dispatcher is not ready, we don't need do the scan.
	if !c.checkAndSendReady(task) {
		return false
	}

	c.sendHandshakeIfNeed(task)

	// Only check scan when the dispatcher is ready to receive data event.
	if !task.IsReadyRecevingData() {
		// If the dispatcher is not ready to receive data event,
		// we still need to send the last resolvedTs to the dispatcher.
		resolvedTs := task.sentResolvedTs.Load()
		c.sendResolvedTs(task, resolvedTs)
		return false
	}

	ok, _ := c.getScanTaskDataRange(task)
	return ok
}

func (c *eventBroker) checkAndSendReady(task scanTask) bool {
	// the dispatcher is not reset yet.
	if task.resetTs.Load() == 0 {
		remoteID := node.ID(task.info.GetServerID())
		event := pevent.NewReadyEvent(task.info.GetID())
		wrapEvent := newWrapReadyEvent(remoteID, event)
		c.getMessageCh(task.messageWorkerIndex) <- wrapEvent
		metricEventServiceSendCommandCount.Inc()
		return false
	}
	return true
}

func (c *eventBroker) sendHandshakeIfNeed(task scanTask) {
	if task.isHandshaked.Load() {
		return
	}
	if !task.isHandshaked.CompareAndSwap(false, true) {
		log.Panic("should not happen: sendHandshakeIfNeed should not be called concurrently")
		return
	}
	// Always reset the seq of the dispatcher to 0 before sending a handshake event.
	task.seq.Store(0)
	remoteID := node.ID(task.info.GetServerID())
	event := pevent.NewHandshakeEvent(
		task.id,
		task.resetTs.Load(),
		task.seq.Add(1),
		task.epoch.Load(),
		task.startTableInfo.Load())
	wrapEvent := newWrapHandshakeEvent(remoteID, event)
	c.getMessageCh(task.messageWorkerIndex) <- wrapEvent
	metricEventServiceSendCommandCount.Inc()
}

// hasSyncPointEventBeforeTs checks if there is any sync point events before the given ts.
func (c *eventBroker) hasSyncPointEventsBeforeTs(ts uint64, d *dispatcherStat) bool {
	return d.enableSyncPoint && ts > d.nextSyncPoint
}

// emitSyncPointEventIfNeeded emits a sync point event if the current ts is greater than the next sync point, and updates the next sync point.
// We need call this function every time we send a event(whether dml/ddl/resolvedTs),
// thus to ensure the sync point event is in correct order for each dispatcher.
// When a period of time, there is no other dml and ddls, we will batch multiple sync point commit ts in one sync point event to enhance the speed.
func (c *eventBroker) emitSyncPointEventIfNeeded(ts uint64, d *dispatcherStat, remoteID node.ID) {
	commitTsList := make([]uint64, 0)
	for d.enableSyncPoint && ts > d.nextSyncPoint {
		commitTsList = append(commitTsList, d.nextSyncPoint)
		d.nextSyncPoint = oracle.GoTimeToTS(oracle.GetTimeFromTS(d.nextSyncPoint).Add(d.syncPointInterval))
	}

	for len(commitTsList) > 0 {
		// we limit a sync point event to contain at most 16 commit ts, to avoid a too large event.
		newCommitTsList := commitTsList
		if len(commitTsList) > 16 {
			newCommitTsList = commitTsList[:16]
		}
		e := &pevent.SyncPointEvent{
			DispatcherID: d.id,
			CommitTsList: newCommitTsList,
			Seq:          d.seq.Add(1),
			Epoch:        d.epoch.Load(),
		}
		syncPointEvent := newWrapSyncPointEvent(remoteID, e, d.getEventSenderState())
		c.getMessageCh(d.messageWorkerIndex) <- syncPointEvent

		if len(commitTsList) > 16 {
			commitTsList = commitTsList[16:]
		} else {
			break
		}
	}
}

func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	var interrupted bool
	defer func() {
		if interrupted {
			c.pushTask(task, false)
		} else {
			task.isTaskScanning.Store(false)
		}
	}()

	remoteID := node.ID(task.info.GetServerID())
	// If the target is not ready to send, we don't need to scan the event store.
	// To avoid the useless scan task.
	if !c.msgSender.IsReadyToSend(remoteID) {
		log.Info("The remote target is not ready, skip scan",
			zap.String("changefeed", task.info.GetChangefeedID().String()),
			zap.String("dispatcher", task.id.String()),
			zap.String("remote", remoteID.String()))
		return
	}

	needScan, dataRange := c.getScanTaskDataRange(task)
	if !needScan {
		return
	}

	scanner := newEventScanner(c.eventStore, c.schemaStore, c.mounter, task.epoch.Load())
	sl := scanLimit{
		maxScannedBytes: task.getCurrentScanLimitInBytes(),
		timeout:         time.Millisecond * 1000, // 1 Second
	}

	events, interrupted, err := scanner.scan(ctx, task, dataRange, sl)
	if err != nil {
		log.Error("scan events failed", zap.Stringer("dispatcher", task.id), zap.Any("dataRange", dataRange), zap.Uint64("receivedResolvedTs", task.eventStoreResolvedTs.Load()), zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()), zap.Error(err))
		return
	}

	// Check whether the task is ready to receive data events again before sending events.
	if !task.isReadyRecevingData.Load() {
		return
	}

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
		case pevent.TypeBatchDMLEvent:
			dmls, ok := e.(*pevent.BatchDMLEvent)
			if !ok {
				log.Panic("expect a DMLEvent, but got", zap.Any("event", e))
			}
			c.sendDML(remoteID, dmls, task)
		case pevent.TypeDDLEvent:
			ddl, ok := e.(*pevent.DDLEvent)
			if !ok {
				log.Panic("expect a DDLEvent, but got", zap.Any("event", e))
			}
			c.sendDDL(ctx, remoteID, ddl, task)
		case pevent.TypeResolvedEvent:
			re, ok := e.(pevent.ResolvedEvent)
			if !ok {
				log.Panic("expect a ResolvedEvent, but got", zap.Any("event", e))
			}
			c.sendResolvedTs(task, re.ResolvedTs)
		default:
			log.Panic("unknown event type", zap.Any("event", e))
		}
	}
	// Update metrics
	metricEventBrokerScanTaskCount.Inc()
}

func (c *eventBroker) runSendMessageWorker(ctx context.Context, workerIndex int) error {
	ticker := time.NewTicker(defaultFlushResolvedTsInterval)
	defer ticker.Stop()

	resolvedTsCacheMap := make(map[node.ID]*resolvedTsCache)
	messageCh := c.messageCh[workerIndex]
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
				if m.msgType == pevent.TypeResolvedEvent {
					c.handleResolvedTs(ctx, resolvedTsCacheMap, m, workerIndex)
					continue
				}
				tMsg := messaging.NewSingleTargetMessage(
					m.serverID,
					messaging.EventCollectorTopic,
					m.e,
					uint64(workerIndex),
				)
				// Note: we need to flush the resolvedTs cache before sending the message
				// to keep the order of the resolvedTs and the message.
				c.flushResolvedTs(ctx, resolvedTsCacheMap[m.serverID], m.serverID, workerIndex)
				c.sendMsg(ctx, tMsg, m.postSendFunc)
				m.reset()
			}
			batchM = batchM[:0]

		case <-ticker.C:
			for serverID, cache := range resolvedTsCacheMap {
				c.flushResolvedTs(ctx, cache, serverID, workerIndex)
			}
		}
	}
}

func (c *eventBroker) handleResolvedTs(ctx context.Context, cacheMap map[node.ID]*resolvedTsCache, m *wrapEvent, workerIndex int) {
	defer m.reset()
	cache, ok := cacheMap[m.serverID]
	if !ok {
		cache = newResolvedTsCache(resolvedTsCacheSize)
		cacheMap[m.serverID] = cache
	}
	cache.add(m.resolvedTsEvent)
	if cache.isFull() {
		c.flushResolvedTs(ctx, cache, m.serverID, workerIndex)
	}
}

func (c *eventBroker) flushResolvedTs(ctx context.Context, cache *resolvedTsCache, serverID node.ID, workerIndex int) {
	if cache == nil || cache.len == 0 {
		return
	}
	msg := &pevent.BatchResolvedEvent{}
	msg.Events = append(msg.Events, cache.getAll()...)
	tMsg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventCollectorTopic,
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
			return
		default:
		}
		// Send the message to the dispatcher.
		err := c.msgSender.SendEvent(tMsg)
		if err != nil {
			_, ok := err.(apperror.AppError)
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

func (c *eventBroker) reportDispatcherStatToStore(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 10)
	log.Info("update dispatcher send ts goroutine is started")
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			inActiveDispatchers := make([]*dispatcherStat, 0)
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*dispatcherStat)
				checkpointTs := dispatcher.checkpointTs.Load()
				if checkpointTs > 0 && checkpointTs < dispatcher.sentResolvedTs.Load() {
					c.eventStore.UpdateDispatcherCheckpointTs(dispatcher.id, checkpointTs)
				}
				if time.Since(time.Unix(dispatcher.lastReceivedHeartbeatTime.Load(), 0)) > heartbeatTimeout {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*dispatcherStat)
				if time.Since(time.Unix(dispatcher.lastReceivedHeartbeatTime.Load(), 0)) > heartbeatTimeout {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			for _, d := range inActiveDispatchers {
				log.Info("remove in-active dispatcher", zap.Stringer("dispatcherID", d.id), zap.Time("lastReceivedHeartbeatTime", time.Unix(d.lastReceivedHeartbeatTime.Load(), 0)))
				c.removeDispatcher(d.info)
			}
		}
	}
}

func (c *eventBroker) close() {
	c.cancel()
	_ = c.g.Wait()
}

func (c *eventBroker) onNotify(d *dispatcherStat, resolvedTs uint64, latestCommitTs uint64) {
	if d.onResolvedTs(resolvedTs) {
		d.lastReceivedResolvedTsTime.Store(time.Now())
		metricEventStoreOutputResolved.Inc()
		d.onLatestCommitTs(latestCommitTs)
		if c.scanReady(d) {
			c.pushTask(d, true)
		}
	}
}

func (c *eventBroker) pushTask(d *dispatcherStat, force bool) {
	if force {
		d.isTaskScanning.Store(true)
		c.taskChan[d.scanWorkerIndex] <- d
	} else {
		timer := time.NewTimer(time.Millisecond * 10)
		select {
		case c.taskChan[d.scanWorkerIndex] <- d:
			d.isTaskScanning.Store(true)
		case <-timer.C:
			d.isTaskScanning.Store(false)
		}
	}
}

func (c *eventBroker) getDispatcher(id common.DispatcherID) (*dispatcherStat, bool) {
	stat, ok := c.dispatchers.Load(id)
	if ok {
		return stat.(*dispatcherStat), true
	}
	stat, ok = c.tableTriggerDispatchers.Load(id)
	if ok {
		return stat.(*dispatcherStat), true
	}
	return nil, false
}

func (c *eventBroker) addDispatcher(info DispatcherInfo) error {
	defer c.metricsCollector.metricDispatcherCount.Inc()
	filter := info.GetFilter()

	start := time.Now()
	id := info.GetID()
	span := info.GetTableSpan()
	startTs := info.GetStartTs()
	changefeedID := info.GetChangefeedID()
	workerIndex := (common.GID)(id).Hash(uint64(len(c.messageCh)))
	scanWorkerIndex := (common.GID)(id).Hash(uint64(len(c.taskChan)))

	dispatcher := newDispatcherStat(startTs, info, filter, scanWorkerIndex, workerIndex, c.getOrSetChangefeedStatus(changefeedID))
	if span.Equal(common.DDLSpan) {
		c.tableTriggerDispatchers.Store(id, dispatcher)
		log.Info("table trigger dispatcher register dispatcher",
			zap.Uint64("clusterID", c.tidbClusterID),
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("startTs", startTs),
			zap.Duration("brokerRegisterDuration", time.Since(start)),
		)
		return nil
	}

	success := c.eventStore.RegisterDispatcher(
		id,
		span,
		info.GetStartTs(),
		func(resolvedTs uint64, latestCommitTs uint64) { c.onNotify(dispatcher, resolvedTs, latestCommitTs) },
		info.IsOnlyReuse(),
		info.GetBdrMode(),
	)

	if !success {
		if !info.IsOnlyReuse() {
			log.Error("register dispatcher to eventStore failed",
				zap.Stringer("dispatcherID", id),
				zap.String("span", common.FormatTableSpan(span)),
				zap.Uint64("startTs", info.GetStartTs()),
			)
		}
		c.sendNotReusableEvent(node.ID(info.GetServerID()), dispatcher)
		return nil
	}

	err := c.schemaStore.RegisterTable(span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Error("register table to schemaStore failed",
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("startTs", info.GetStartTs()),
			zap.Error(err),
		)
		return err
	}
	tableInfo, err := c.schemaStore.GetTableInfo(span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Error("get table info from schemaStore failed",
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("startTs", info.GetStartTs()),
			zap.Error(err),
		)
		return err
	}
	dispatcher.updateTableInfo(tableInfo)
	c.dispatchers.Store(id, dispatcher)
	log.Info("register dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID),
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("dispatcherID", id),
		zap.String("span", common.FormatTableSpan(span)),
		zap.Uint64("startTs", startTs),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (c *eventBroker) removeDispatcher(dispatcherInfo DispatcherInfo) {
	defer c.metricsCollector.metricDispatcherCount.Dec()
	id := dispatcherInfo.GetID()

	stat, ok := c.dispatchers.Load(id)
	if !ok {
		stat, ok = c.tableTriggerDispatchers.Load(id)
		if !ok {
			return
		}
		c.tableTriggerDispatchers.Delete(id)
	}

	stat.(*dispatcherStat).changefeedStat.removeDispatcher()

	if stat.(*dispatcherStat).changefeedStat.dispatcherCount.Load() == 0 {
		log.Info("All dispatchers for the changefeed are removed, remove the changefeed status",
			zap.Stringer("changefeedID", dispatcherInfo.GetChangefeedID()),
		)
		c.changefeedMap.Delete(dispatcherInfo.GetChangefeedID())
	}

	stat.(*dispatcherStat).isRemoved.Store(true)
	stat.(*dispatcherStat).isReadyRecevingData.Store(false)
	c.eventStore.UnregisterDispatcher(id)
	// todo: how to handle this error?
	_ = c.schemaStore.UnregisterTable(dispatcherInfo.GetTableSpan().TableID)
	c.dispatchers.Delete(id)

	log.Info("remove dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID),
		zap.Stringer("changefeedID", dispatcherInfo.GetChangefeedID()),
		zap.Stringer("dispatcherID", id),
		zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
	)
}

func (c *eventBroker) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		return
	}
	log.Info("pause dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID),
		zap.Stringer("changefeedID", stat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", stat.id),
		zap.String("span", common.FormatTableSpan(stat.info.GetTableSpan())),
		zap.Uint64("sentResolvedTs", stat.sentResolvedTs.Load()),
		zap.Uint64("seq", stat.seq.Load()))
	stat.isReadyRecevingData.Store(false)
	stat.resetScanLimit()
}

func (c *eventBroker) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		return
	}
	log.Info("resume dispatcher",
		zap.Stringer("changefeedID", stat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", stat.id),
		zap.String("span", common.FormatTableSpan(stat.info.GetTableSpan())),
		zap.Uint64("sentResolvedTs", stat.sentResolvedTs.Load()),
		zap.Uint64("seq", stat.seq.Load()))
	stat.isReadyRecevingData.Store(true)
}

func (c *eventBroker) resetDispatcher(dispatcherInfo DispatcherInfo) {
	stat, ok := c.getDispatcher(dispatcherInfo.GetID())
	if !ok {
		// The dispatcher is not registered, register it.
		// FIXME: Handle the error.
		_ = c.addDispatcher(dispatcherInfo)
		return
	}

	// Must set the isRunning to false before reset the dispatcher.
	// Otherwise, the scan task goroutine will not return before reset the dispatcher.
	stat.isReadyRecevingData.Store(false)

	// Wait until the scan task goroutine return before reset the dispatcher.
	for {
		if !stat.isTaskScanning.Load() {
			break
		}
		// Give other goroutines a chance to acquire the write lock
		time.Sleep(10 * time.Millisecond)
	}

	oldSeq := stat.seq.Load()
	oldSentResolvedTs := stat.sentResolvedTs.Load()
	stat.resetState(dispatcherInfo.GetStartTs())
	newEpoch := dispatcherInfo.GetEpoch()
	for {
		oldEpoch := stat.epoch.Load()
		if oldEpoch >= newEpoch {
			break
		}
		if stat.epoch.CompareAndSwap(oldEpoch, newEpoch) {
			break
		}
	}

	log.Info("reset dispatcher",
		zap.Stringer("changefeedID", stat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", stat.id),
		zap.String("span", common.FormatTableSpan(stat.info.GetTableSpan())),
		zap.Uint64("originStartTs", stat.info.GetStartTs()),
		zap.Uint64("oldSeq", oldSeq),
		zap.Uint64("newSeq", stat.seq.Load()),
		zap.Uint64("newEpoch", newEpoch),
		zap.Uint64("oldSentResolvedTs", oldSentResolvedTs),
		zap.Uint64("newSentResolvedTs", stat.sentResolvedTs.Load()))
}

func (c *eventBroker) getOrSetChangefeedStatus(changefeedID common.ChangeFeedID) *changefeedStatus {
	stat, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		stat = newChangefeedStatus(changefeedID)
		log.Info("new changefeed status",
			zap.Stringer("changefeedID", changefeedID),
			zap.Bool("isRunning", stat.(*changefeedStatus).isReadyRecevingData.Load()),
		)
		c.changefeedMap.Store(changefeedID, stat)
	}
	return stat.(*changefeedStatus)
}

func (c *eventBroker) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	responseMap := make(map[string]*event.DispatcherHeartbeatResponse)
	for _, dp := range heartbeat.heartbeat.DispatcherProgresses {
		dispatcher, ok := c.getDispatcher(dp.DispatcherID)
		// Can't find the dispatcher, it means the dispatcher is removed.
		if !ok {
			response, ok := responseMap[heartbeat.serverID]
			if !ok {
				response = event.NewDispatcherHeartbeatResponse()
				responseMap[heartbeat.serverID] = response
			}
			response.Append(event.NewDispatcherState(dp.DispatcherID, event.DSStateRemoved))
			continue
		}
		// TODO: Should we check if the dispatcher's serverID is the same as the heartbeat's serverID?
		if dispatcher.checkpointTs.Load() < dp.CheckpointTs {
			dispatcher.checkpointTs.Store(dp.CheckpointTs)
		}
		// Update the last received heartbeat time to the current time.
		dispatcher.lastReceivedHeartbeatTime.Store(time.Now().UnixNano())
	}
	c.sendDispatcherResponse(responseMap)
}

func (c *eventBroker) sendDispatcherResponse(responseMap map[string]*event.DispatcherHeartbeatResponse) {
	for serverID, response := range responseMap {
		msg := messaging.NewSingleTargetMessage(node.ID(serverID), messaging.EventCollectorTopic, response)
		c.msgSender.SendCommand(msg)
	}
}
