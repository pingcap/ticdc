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

	// sendMessageWorkerCount is the number of the send message workers to spawn.
	sendMessageWorkerCount int
	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

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
		scanWorkerCount:         scanWorkerCount,
		taskChan:                make([]chan scanTask, scanWorkerCount),
		sendMessageWorkerCount:  sendMessageWorkerCount,
		messageCh:               make([]chan *wrapEvent, sendMessageWorkerCount),
		cancel:                  cancel,
		g:                       g,
	}
	// Initialize metrics collector
	c.metricsCollector = newMetricsCollector(c)

	for i := 0; i < c.sendMessageWorkerCount; i++ {
		c.messageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
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

	for i := 0; i < c.sendMessageWorkerCount; i++ {
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i)
		})
	}
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

func (c *eventBroker) sendResolvedTs(
	server node.ID,
	d *dispatcherStat,
	watermark uint64,
) {
	c.emitSyncPointEventIfNeeded(watermark, d, server)
	re := pevent.NewResolvedEvent(watermark, d.id)
	resolvedEvent := newWrapResolvedEvent(
		server,
		re,
		d.getEventSenderState(),
	)
	c.getMessageCh(d.messageWorkerIndex) <- resolvedEvent
	d.updateSentResolvedTs(watermark)
	metricEventServiceSendResolvedTsCount.Inc()
}

func (c *eventBroker) sendReadyEvent(
	server node.ID,
	d *dispatcherStat,
) {
	event := pevent.NewReadyEvent(d.info.GetID())
	wrapEvent := newWrapReadyEvent(server, event)
	c.getMessageCh(d.messageWorkerIndex) <- wrapEvent
	metricEventServiceSendCommandCount.Inc()
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
				if !c.checkAndSendHandshake(dispatcherStat) {
					return true
				}
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
					c.sendResolvedTs(remoteID, dispatcherStat, endTs)
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

// checkNeedScan checks if the dispatcher needs to scan the event store.
// If the dispatcher needs to scan the event store, it returns true.
// If the dispatcher does not need to scan the event store, it send the watermark to the dispatcher
func (c *eventBroker) checkNeedScan(task scanTask, mustCheck bool) (bool, common.DataRange) {
	if !mustCheck && task.isTaskScanning.Load() {
		return false, common.DataRange{}
	}

	// If the dispatcher is not ready, we don't need to scan the event store.
	if !c.checkAndSendReady(task) {
		return false, common.DataRange{}
	}

	c.checkAndSendHandshake(task)

	// Only check scan when the dispatcher is running.
	if !task.IsRunning() {
		// If the dispatcher is not running, we also need to send the watermark to the dispatcher.
		// And the resolvedTs should be the last sent watermark.
		resolvedTs := task.sentResolvedTs.Load()
		remoteID := node.ID(task.info.GetServerID())

		c.sendResolvedTs(remoteID, task, resolvedTs)
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

	if ddlState.ResolvedTs <= dataRange.StartTs {
		metricEventServiceSkipResolvedTsCount.Inc()
		return false, common.DataRange{}
	}

	// Note: Maybe we should still send a resolvedTs to downstream to tell that
	// the dispatcher is alive?
	if dataRange.EndTs <= dataRange.StartTs {
		metricEventServiceSkipResolvedTsCount.Inc()
		return false, common.DataRange{}
	}

	// target ts range: (dataRange.StartTs, dataRange.EndTs]
	if dataRange.StartTs >= task.latestCommitTs.Load() &&
		dataRange.StartTs >= ddlState.MaxEventCommitTs {
		// The dispatcher has no new events. In such case, we don't need to scan the event store.
		// We just send the watermark to the dispatcher.
		remoteID := node.ID(task.info.GetServerID())
		c.sendResolvedTs(remoteID, task, dataRange.EndTs)
		return false, common.DataRange{}
	}

	return true, dataRange
}

func (c *eventBroker) checkAndSendReady(task scanTask) bool {
	if task.resetTs.Load() == 0 {
		remoteID := node.ID(task.info.GetServerID())
		c.sendReadyEvent(remoteID, task)
		return false
	}
	return true
}

func (c *eventBroker) checkAndSendHandshake(task scanTask) bool {
	if task.isHandshaked.Load() {
		return true
	}
	// Always reset the seq of the dispatcher to 0 before sending a handshake event.
	task.seq.Store(0)
	wrapE := &wrapEvent{
		serverID: node.ID(task.info.GetServerID()),
		e: pevent.NewHandshakeEvent(
			task.id,
			task.resetTs.Load(),
			task.seq.Add(1),
			task.startTableInfo.Load()),
		msgType: pevent.TypeHandshakeEvent,
		postSendFunc: func() {
			log.Info("checkAndSendHandshake", zap.String("changefeed", task.info.GetChangefeedID().String()), zap.String("dispatcher", task.id.String()), zap.Int("workerIndex", task.scanWorkerIndex), zap.Bool("isHandshaked", task.isHandshaked.Load()))
			task.isHandshaked.Store(true)
		},
	}
	c.getMessageCh(task.messageWorkerIndex) <- wrapE
	metricEventServiceSendCommandCount.Inc()
	return false
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
		syncPointEvent := newWrapSyncPointEvent(
			remoteID,
			&pevent.SyncPointEvent{
				DispatcherID: d.id,
				CommitTsList: newCommitTsList,
			},
			d.getEventSenderState())
		c.getMessageCh(d.messageWorkerIndex) <- syncPointEvent

		if len(commitTsList) > 16 {
			commitTsList = commitTsList[16:]
		} else {
			break
		}
	}
}

func (c *eventBroker) doScan(ctx context.Context, task scanTask) {
	remoteID := node.ID(task.info.GetServerID())

	isBroken := false
	defer func() {
		if isBroken {
			c.pushTask(task, false)
		} else {
			task.isTaskScanning.Store(false)
		}
	}()

	// If the target is not ready to send, we don't need to scan the event store.
	// To avoid the useless scan task.
	if !c.msgSender.IsReadyToSend(remoteID) {
		log.Info("The remote target is not ready, skip scan",
			zap.String("changefeed", task.info.GetChangefeedID().String()),
			zap.String("dispatcher", task.id.String()),
			zap.String("remote", remoteID.String()))
		return
	}

	needScan, dataRange := c.checkNeedScan(task, true)
	if !needScan {
		return
	}

	scanner := newEventScanner(c.eventStore, c.schemaStore, c.mounter)
	sl := scanLimit{
		maxBytes: task.getCurrentScanLimitInBytes(),
		timeout:  time.Millisecond * 1000, // 1 Second
	}

	events, isBroken, err := scanner.scan(ctx, task, dataRange, sl)
	if err != nil {
		log.Error("scan events failed", zap.Stringer("dispatcher", task.id), zap.Any("dataRange", dataRange), zap.Uint64("receivedResolvedTs", task.eventStoreResolvedTs.Load()), zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()), zap.Error(err))
		return
	}

	// If the task is not running, we don't send the events to the dispatcher.
	if !task.isRunning.Load() {
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
			c.sendResolvedTs(remoteID, task, re.ResolvedTs)
		default:
			log.Panic("unknown event type", zap.Any("event", e))
		}
	}
	// Update metrics
	metricEventBrokerScanTaskCount.Inc()
}

func (c *eventBroker) runSendMessageWorker(ctx context.Context, workerIndex int) error {
	flushResolvedTsTicker := time.NewTicker(defaultFlushResolvedTsInterval)
	defer flushResolvedTsTicker.Stop()

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
				// Check if the dispatcher is initialized, if so, ignore the handshake event.
				if m.msgType == pevent.TypeHandshakeEvent {
					// If the message is a handshake event, we need to reset the dispatcher.
					d, ok := c.getDispatcher(m.getDispatcherID())
					if !ok {
						log.Warn("Get dispatcher failed", zap.Any("dispatcherID", m.getDispatcherID()))
						continue
					}
					if d.isHandshaked.Load() {
						log.Info("Ignore handshake event since the dispatcher already handshaked", zap.Any("dispatcherID", m.getDispatcherID()))
						continue
					}
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

		case <-flushResolvedTsTicker.C:
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
		needScan, _ := c.checkNeedScan(d, false)
		if needScan {
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
	workerIndex := (common.GID)(id).Hash(uint64(c.sendMessageWorkerCount))
	scanWorkerIndex := (common.GID)(id).Hash(uint64(c.scanWorkerCount))

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
	stat.(*dispatcherStat).isRunning.Store(false)
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
	stat.isRunning.Store(false)
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
	stat.isRunning.Store(true)
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
	stat.isRunning.Store(false)

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

	log.Info("reset dispatcher",
		zap.Stringer("changefeedID", stat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", stat.id),
		zap.String("span", common.FormatTableSpan(stat.info.GetTableSpan())),
		zap.Uint64("originStartTs", stat.info.GetStartTs()),
		zap.Uint64("oldSeq", oldSeq),
		zap.Uint64("newSeq", stat.seq.Load()),
		zap.Uint64("oldSentResolvedTs", oldSentResolvedTs),
		zap.Uint64("newSentResolvedTs", stat.sentResolvedTs.Load()))
}

func (c *eventBroker) getOrSetChangefeedStatus(changefeedID common.ChangeFeedID) *changefeedStatus {
	stat, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		stat = newChangefeedStatus(changefeedID)
		log.Info("new changefeed status",
			zap.Stringer("changefeedID", changefeedID),
			zap.Bool("isRunning", stat.(*changefeedStatus).isRunning.Load()),
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
				response = event.NewDispatcherHeartbeatResponse(32)
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
