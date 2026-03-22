// Copyright 2026 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func (c *eventBroker) sendDML(remoteID node.ID, batchEvent *event.BatchDMLEvent, d *dispatcherStat) {
	doSendDML := func(e *event.BatchDMLEvent) {
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
			idx = 1
			c.emitSyncPointEventIfNeeded(dml.GetCommitTs(), d, remoteID)
		} else {
			idx++
		}
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
					c.sendResolvedTs(stat, endTs)
				}
				return true
			})
		}
	}
}

func (c *eventBroker) getScanTaskDataRange(task scanTask) (bool, common.DataRange) {
	dataRange, needScan := task.getDataRange()
	if !needScan {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		return false, common.DataRange{}
	}

	keyspaceMeta := common.KeyspaceMeta{
		ID:   task.info.GetTableSpan().KeyspaceID,
		Name: task.changefeedStat.changefeedID.Keyspace(),
	}

	ddlState, err := c.schemaStore.GetTableDDLEventState(keyspaceMeta, task.info.GetTableSpan().TableID)
	if err != nil {
		log.Error("GetTableDDLEventState failed", zap.Uint32("keyspaceID", task.info.GetTableSpan().KeyspaceID), zap.Int64("tableID", task.info.GetTableSpan().TableID), zap.Error(err))
		return false, common.DataRange{}
	}
	dataRange.CommitTsEnd = min(dataRange.CommitTsEnd, ddlState.ResolvedTs)
	commitTsEndBeforeWindow := dataRange.CommitTsEnd
	scanMaxTs := task.changefeedStat.getScanMaxTs()
	if scanMaxTs > 0 {
		dataRange.CommitTsEnd = min(dataRange.CommitTsEnd, scanMaxTs)
		if dataRange.CommitTsEnd < commitTsEndBeforeWindow {
			log.Debug("scan window capped",
				zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
				zap.Stringer("dispatcherID", task.id),
				zap.Uint64("baseTs", task.changefeedStat.minSentTs.Load()),
				zap.Uint64("scanMaxTs", scanMaxTs),
				zap.Uint64("beforeEndTs", commitTsEndBeforeWindow),
				zap.Uint64("afterEndTs", dataRange.CommitTsEnd),
				zap.Duration("scanInterval", time.Duration(task.changefeedStat.scanInterval.Load())),
			)
		}
	}

	if dataRange.CommitTsEnd <= dataRange.CommitTsStart {
		updateMetricEventServiceSkipResolvedTsCount(task.info.GetMode())
		c.sendSignalResolvedTs(task)
		return false, common.DataRange{}
	}

	noDMLEvent := dataRange.CommitTsStart > task.eventStoreCommitTs.Load()
	noDDLEvent := dataRange.CommitTsStart >= ddlState.MaxEventCommitTs
	if noDMLEvent && noDDLEvent {
		c.sendResolvedTs(task, dataRange.CommitTsEnd)
		return false, common.DataRange{}
	}
	return true, dataRange
}

func (c *eventBroker) hasSyncPointEventsBeforeTs(ts uint64, d *dispatcherStat) bool {
	return d.enableSyncPoint && ts > d.nextSyncPoint.Load()
}

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

	if !c.scanRateLimiter.AllowN(time.Now(), int(task.lastScanBytes.Load())) {
		log.Debug("scan rate limit exceeded",
			zap.Stringer("dispatcher", task.id),
			zap.Int64("lastScanBytes", task.lastScanBytes.Load()),
			zap.Uint64("sentResolvedTs", task.sentResolvedTs.Load()))
		return
	}

	item, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		log.Info("changefeed status is not found, skip scan",
			zap.Stringer("changefeed", changefeedID),
			zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().GetTableID()))
		return
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
	for {
		select {
		case <-ctx.Done():
			log.Error("send message failed", zap.Error(ctx.Err()))
			return
		default:
		}
		err := c.msgSender.SendEvent(tMsg)
		if err != nil {
			_, ok := err.(errors.AppError)
			log.Debug("send msg failed, retry it later", zap.Error(err), zap.Stringer("tMsg", tMsg), zap.Bool("castOk", ok))
			if strings.Contains(err.Error(), "congested") {
				log.Debug("send message failed since the message is congested, retry it laster", zap.Error(err))
				time.Sleep(congestedRetryInterval)
				continue
			}
			log.Info("send message failed, drop it", zap.Error(err), zap.Stringer("tMsg", tMsg))
			return
		}
		if postSendMsg != nil {
			postSendMsg()
		}
		metricEventServiceSendEventDuration.Observe(time.Since(start).Seconds())
		return
	}
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
