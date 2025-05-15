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
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

// ScanLimit defines the limits for a scan operation
type scanLimit struct {
	// MaxBytes is the maximum number of bytes to scan
	MaxBytes int64
	// Timeout is the maximum time to spend scanning
	Timeout time.Duration
}

// eventScanner scans events from eventStore and schemaStore
type eventScanner struct {
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	mounter     pevent.Mounter
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mounter pevent.Mounter,
) *eventScanner {
	return &eventScanner{
		eventStore:  eventStore,
		schemaStore: schemaStore,
		mounter:     mounter,
	}
}

// Scan retrieves and processes events from both eventStore and schemaStore based on the provided scanTask and limits.
// The function ensures that events are returned in chronological order, with DDL and DML events sorted by their commit timestamps.
// If there are DML and DDL events with the same commitTs, the DML event will be returned first.
//
// Time-ordered event processing:
//
//	Time/Commit TS -->
//	|
//	|    DML1   DML2      DML3      DML4  DML5
//	|     |      |         |         |     |
//	|     v      v         v         v     v
//	|    TS10   TS20      TS30      TS40  TS40
//	|                       |               |
//	|                       |              DDL2
//	|                      DDL1            TS40
//	|                      TS30
//
// - DML events with TS 10, 20, 30 are processed first
// - At TS30, DDL1 is processed after DML3 (same timestamp)
// - At TS40, DML4 is processed first, then DML5, then DDL2 (same timestamp)
//
// The scan operation may be interrupted when ANY of these limits are reached:
// - Maximum bytes processed (limit.MaxBytes)
// - Timeout duration (limit.Timeout)
//
// A scan interruption is ONLY allowed when both conditions are met:
// 1. The current event's commit timestamp is greater than the lastCommitTs (a commit TS boundary is reached)
// 2. At least one DML event has been successfully scanned
//
// Returns:
// - events: The scanned events in commitTs order
// - isBroken: true if the scan was interrupted due to reaching a limit, false otherwise
// - error: Any error that occurred during the scan operation
func (s *eventScanner) Scan(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit scanLimit,
) ([]event.Event, bool, error) {
	startTime := time.Now()
	var events []event.Event
	var totalBytes int64
	var lastCommitTs uint64

	defer func() {
		metrics.EventServiceScanDuration.Observe(time.Since(startTime).Seconds())
	}()
	dispatcherID := dispatcherStat.id

	ddlEvents, err := s.schemaStore.FetchTableDDLEvents(
		dataRange.Span.TableID,
		dispatcherStat.filter,
		dataRange.StartTs,
		dataRange.EndTs,
	)
	if err != nil {
		log.Error("get ddl events failed", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
		return nil, false, err
	}

	iter, err := s.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Error("read events failed", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
		return nil, false, err
	}

	// appendDML adds a DML event to the results list, maintaining order by commitTs.
	// It ensures:
	// 1. Any DDL events with FinishedTs <= DML's CommitTs are added first
	// 2. The DML event is added after relevant DDLs (enforcing DML priority at same commitTs)
	// 3. lastCommitTs is updated to track the scan boundary for potential interruption
	//
	// Example ordering:
	//    TS10 (DDL1) → TS20 (DML1) → TS30 (DML2) → TS30 (DDL2) → TS40 (DML3)
	appendDML := func(dml *pevent.BatchDMLEvent) {
		if dml == nil || dml.Len() == 0 {
			return
		}
		commitTs := dml.GetCommitTs()
		for len(ddlEvents) > 0 && commitTs > ddlEvents[0].FinishedTs {
			events = append(events, &ddlEvents[0])
			ddlEvents = ddlEvents[1:]
		}
		events = append(events, dml)
		lastCommitTs = commitTs
	}

	// appendDDLs appends all ddl events with finishedTs <= endTs
	appendDDLs := func(endTs uint64) {
		for i := 0; i < len(ddlEvents); i++ {
			ep := &ddlEvents[i]
			if ep.FinishedTs > endTs {
				break
			}
			events = append(events, ep)
		}
		events = append(events, pevent.ResolvedEvent{
			DispatcherID: dispatcherID,
			ResolvedTs:   endTs,
		})
	}

	if iter == nil {
		appendDDLs(dataRange.EndTs)
		return events, false, nil
	}

	defer func() {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			metricEventStoreOutputKv.Add(float64(eventCount))
		}
	}()

	var batchDML *pevent.BatchDMLEvent
	var lastTableInfoUpdateTs uint64
	dmlCount := 0
	tableID := dataRange.Span.TableID
	for {
		select {
		case <-ctx.Done():
			log.Warn("scan exits since context done", zap.Error(ctx.Err()), zap.Stringer("dispatcherID", dispatcherID))
			return events, false, ctx.Err()
		default:
		}

		// If the dispatcher is not running, we don't scan any events.
		if !dispatcherStat.isRunning.Load() {
			return nil, false, nil
		}

		e, isNewTxn, err := iter.Next()
		if err != nil {
			log.Panic("read events failed", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
		}

		if e == nil {
			appendDML(batchDML)
			appendDDLs(dataRange.EndTs)
			return events, false, nil
		}

		eSize := len(e.Key) + len(e.Value) + len(e.OldValue)
		totalBytes += int64(eSize)
		elapsed := time.Since(startTime)

		if isNewTxn {
			if (totalBytes > limit.MaxBytes || elapsed > limit.Timeout) && e.CRTs > lastCommitTs && dmlCount > 0 {
				appendDML(batchDML)
				appendDDLs(lastCommitTs)
				return events, true, nil
			}
			tableInfo, err := s.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				if dispatcherStat.isRemoved.Load() {
					log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
					return events, false, nil
				}

				if errors.Is(err, &schemastore.TableDeletedError{}) {
					// After a table is truncated, it is possible to receive more dml events, just ignore is ok.
					// TODO: tables may be deleted in many ways, we need to check if it is safe to ignore later dmls in all cases.
					// We must send the remaining ddl events to the dispatcher in this case.
					appendDDLs(dataRange.EndTs)
					log.Warn("get table info failed, since the table is deleted", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
					return events, false, nil
				}
				log.Panic("get table info failed, unknown reason", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
			}
			hasDDL := batchDML != nil && len(ddlEvents) > 0 && e.CRTs > ddlEvents[0].FinishedTs
			// updateTs may be less than the previous updateTs
			if tableInfo.UpdateTS() != lastTableInfoUpdateTs || hasDDL {
				appendDML(batchDML)
				lastTableInfoUpdateTs = tableInfo.UpdateTS()
				batchDML = new(pevent.BatchDMLEvent)
			}
			batchDML.AppendDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)

			lastCommitTs = batchDML.GetCommitTs()
			dmlCount++
		}

		if err = batchDML.AppendRow(e, s.mounter.DecodeToChunk); err != nil {
			log.Panic("append row failed", zap.Error(err), zap.Stringer("dispatcherID", dispatcherID))
		}
	}
}
