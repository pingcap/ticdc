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
	"errors"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	batchDMLEventSize = 1024
	batchInterval     = 2 * time.Second
)

type dispatcherCache struct {
	dml          *event.DMLEvent
	lastSentTime time.Time
	lastCommitTs uint64
}

func (d *dispatcherCache) shouldSendDML() bool {
	if d.dml == nil {
		return false
	}

	if d.dml.Length >= int32(batchDMLEventSize) || time.Since(d.lastSentTime) > batchInterval {
		return true
	}

	return false
}

func (d *dispatcherCache) reset() {
	d.dml = nil
	d.lastSentTime = time.Now()
}

type scanWorker struct {
	// A reverse pointer to the eventBroker.
	eventBroker *eventBroker
	taskChan    chan scanTask
	// A cache of the DML events for the same dispatcher.
	cacheDML map[common.DispatcherID]*dispatcherCache
}

func newScanWorker(eventBroker *eventBroker, taskChan chan scanTask) *scanWorker {
	return &scanWorker{
		eventBroker: eventBroker,
		taskChan:    taskChan,
		cacheDML:    make(map[common.DispatcherID]*dispatcherCache),
	}
}

func (s *scanWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-s.taskChan:
			s.doScan(ctx, task)
		}
	}
}

// TODO: handle error properly.
func (s *scanWorker) doScan(ctx context.Context, task scanTask) {
	start := time.Now()
	remoteID := node.ID(task.info.GetServerID())
	dispatcherID := task.id

	defer func() {
		task.taskScanning.Store(false)
	}()

	// If the target is not ready to send, we don't need to scan the event store.
	// To avoid the useless scan task.
	if !s.eventBroker.msgSender.IsReadyToSend(remoteID) {
		log.Info("The remote target is not ready, skip scan",
			zap.String("changefeed", task.info.GetChangefeedID().String()),
			zap.String("dispatcher", task.id.String()),
			zap.String("remote", remoteID.String()))
		return
	}

	needScan, dataRange := s.eventBroker.checkNeedScan(task, true)
	if !needScan {
		return
	}

	// TODO: distinguish only dml or only ddl scenario
	ddlEvents, err := s.eventBroker.schemaStore.
		FetchTableDDLEvents(
			dataRange.Span.TableID,
			task.filter,
			dataRange.StartTs,
			dataRange.EndTs,
		)
	if err != nil {
		log.Panic("get ddl events failed", zap.Error(err))
	}

	// After all the events are sent, we need to drain the remaining ddlEvents.
	sendRemainingDDLEvents := func() {
		for _, e := range ddlEvents {
			s.eventBroker.sendDDL(ctx, remoteID, e, task)
		}
		s.eventBroker.sendWatermark(remoteID, task, dataRange.EndTs)
		task.updateSentResolvedTs(dataRange.EndTs)
	}

	// 2. Get event iterator from eventStore.
	iter, err := s.eventBroker.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Panic("read events failed", zap.Error(err))
	}

	if iter == nil {
		sendRemainingDDLEvents()
		return
	}

	defer func() {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			metricEventStoreOutputKv.Add(float64(eventCount))
		}
		metricEventBrokerScanTaskCount.Inc()
	}()

	lastSentDMLCommitTs := uint64(0)
	// sendDML is used to send the dml event to the dispatcher.
	// It returns true if the dml event is sent successfully.
	// Otherwise, it returns false.
	sendDML := func(dml *pevent.DMLEvent) bool {
		if dml == nil {
			return true
		}

		// Check if the dispatcher is running.
		// If not, we don't need to send the dml event.
		if !task.IsRunning() {
			if lastSentDMLCommitTs != 0 {
				task.updateSentResolvedTs(lastSentDMLCommitTs)
				s.eventBroker.sendWatermark(remoteID, task, lastSentDMLCommitTs)
				log.Info("The dispatcher is not running, skip the following scan",
					zap.Uint64("clusterID", task.info.GetClusterID()),
					zap.String("changefeed", task.info.GetChangefeedID().String()),
					zap.String("dispatcher", task.id.String()),
					zap.Uint64("taskStartTs", dataRange.StartTs),
					zap.Uint64("taskEndTs", dataRange.EndTs),
					zap.Uint64("lastSentResolvedTs", task.sentResolvedTs.Load()),
					zap.Uint64("lastSentDMLCommitTs", lastSentDMLCommitTs))
			}
			return false
		}

		for len(ddlEvents) > 0 && dml.CommitTs > ddlEvents[0].FinishedTs {
			s.eventBroker.sendDDL(ctx, remoteID, ddlEvents[0], task)
			ddlEvents = ddlEvents[1:]
		}

		dml.Seq = task.seq.Add(1)

		s.eventBroker.emitSyncPointEventIfNeeded(dml.CommitTs, task, remoteID)
		s.eventBroker.getMessageCh(task.workerIndex) <- newWrapDMLEvent(remoteID, dml, task.getEventSenderState())
		metricEventServiceSendKvCount.Add(float64(dml.Len()))
		lastSentDMLCommitTs = dml.CommitTs
		return true
	}

	// 3. Send the events to the dispatcher.
	dmlCache, ok := s.cacheDML[dispatcherID]
	if !ok {
		dmlCache = &dispatcherCache{
			dml:          nil,
			lastSentTime: time.Now(),
		}
		s.cacheDML[dispatcherID] = dmlCache
	}

	for {
		// Note: The first event of the txn must return isNewTxn as true.
		e, isNewTxn, err := iter.Next()
		// Start the memory limiter when the first event is read.
		s.eventBroker.memoryLimiter.Start()
		var eSize int
		if e != nil {
			eSize = int(e.KeyLen + e.ValueLen + e.OldValueLen)
			if eSize > s.eventBroker.memoryLimiter.GetCurrentMemoryLimit() {
				log.Warn("The single event memory limit is exceeded the total memory limit, set it to the current memory limit", zap.Int("eventSize", eSize), zap.Int("currentMemoryLimit", s.eventBroker.memoryLimiter.GetCurrentMemoryLimit()))
				eSize = s.eventBroker.memoryLimiter.GetCurrentMemoryLimit()
			}
			s.eventBroker.memoryLimiter.WaitN(eSize)
		}

		if err != nil {
			log.Panic("read events failed", zap.Error(err))
		}

		if e == nil {
			if dmlCache.shouldSendDML() {
				// Send the last dml to the dispatcher.
				ok := sendDML(dmlCache.dml)
				if !ok {
					return
				}
				dmlCache.reset()
			}

			sendRemainingDDLEvents()
			metrics.EventServiceScanDuration.Observe(time.Since(start).Seconds())
			return
		}

		if e.CRTs < dataRange.StartTs {
			// If the commitTs of the event is less than the startTs of the data range,
			// there are some bugs in the eventStore.
			log.Panic("should never Happen", zap.Uint64("commitTs", e.CRTs), zap.Uint64("dataRangeStartTs", dataRange.StartTs))
		}

		if isNewTxn {
			// Only send the dml event when the dml event is full.
			if dmlCache.shouldSendDML() {
				dmlCache.dml.CommitTs = dmlCache.lastCommitTs
				ok := sendDML(dmlCache.dml)
				if !ok {
					return
				}
				dmlCache.reset()
			}

			tableID := task.info.GetTableSpan().TableID
			tableInfo, err := s.eventBroker.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				if task.isRemoved.Load() {
					log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err))
					return
				}
				if errors.Is(err, &schemastore.TableDeletedError{}) {
					// After a table is truncated, it is possible to receive more dml events, just ignore is ok.
					// TODO: tables may be deleted in many ways, we need to check if it is safe to ignore later dmls in all cases.
					// We must send the remaining ddl events to the dispatcher in this case.
					sendRemainingDDLEvents()
					log.Warn("get table info failed, since the table is deleted", zap.Error(err))
					return
				}
				log.Panic("get table info failed, unknown reason", zap.Error(err))
			}
			if dmlCache.dml == nil {
				dmlCache.dml = pevent.NewDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)
			}
		}

		if err = dmlCache.dml.AppendRow(e, s.eventBroker.mounter.DecodeToChunk); err != nil {
			log.Panic("append row failed", zap.Error(err))
		}

		if e.CRTs > dmlCache.lastCommitTs {
			dmlCache.lastCommitTs = e.CRTs
		}
	}
}
