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
type ScanLimit struct {
	// MaxBytes is the maximum number of bytes to scan
	MaxBytes int64
	// Timeout is the maximum time to spend scanning
	Timeout time.Duration
}

// EventScanner scans events from eventStore and schemaStore
type EventScanner struct {
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	mounter     pevent.Mounter
}

// NewEventScanner creates a new EventScanner
func NewEventScanner(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mounter pevent.Mounter,
) *EventScanner {
	return &EventScanner{
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
func (s *EventScanner) Scan(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit ScanLimit,
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
		log.Error("get ddl events failed", zap.Error(err))
		return nil, false, err
	}

	iter, err := s.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Error("read events failed", zap.Error(err))
		return nil, false, err
	}

	appendDML := func(dml *pevent.DMLEvent) {
		if dml == nil || dml.Length == 0 {
			return
		}
		for len(ddlEvents) > 0 && dml.CommitTs > ddlEvents[0].FinishedTs {
			events = append(events, &ddlEvents[0])
			ddlEvents = ddlEvents[1:]
		}
		events = append(events, dml)
		lastCommitTs = dml.CommitTs
	}

	// appendDDLs appends all ddl events with finishedTs <= endTs
	appendDDLs := func(endTs uint64) {
		for _, e := range ddlEvents {
			ep := &e
			if ep.FinishedTs <= endTs {
				events = append(events, ep)
			}
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

	var dml *pevent.DMLEvent
	dmlCount := 0
	for {
		select {
		case <-ctx.Done():
			log.Warn("scan exits since context done", zap.Error(ctx.Err()))
			return events, false, ctx.Err()
		default:
		}

		// Node: The first event of the txn must return isNewTxn as true.
		e, isNewTxn, err := iter.Next()
		if err != nil {
			log.Panic("read events failed", zap.Error(err))
		}

		// Append last dml
		if e == nil {
			appendDML(dml)
			appendDDLs(dataRange.EndTs)
			return events, false, nil
		}

		eSize := len(e.Key) + len(e.Value) + len(e.OldValue)
		log.Info("fizz event size", zap.Int("size", eSize))
		totalBytes += int64(eSize)
		elapsed := time.Since(startTime)

		if isNewTxn {
			// Must append the dml event before interrupt the scan, otherwise the dml event will be lost if its commitTs is equal to lastCommitTs.
			appendDML(dml)
			if (totalBytes > limit.MaxBytes || elapsed > limit.Timeout) && e.CRTs > lastCommitTs && dmlCount > 0 {
				appendDDLs(lastCommitTs)
				return events, true, nil
			}
			tableID := dataRange.Span.TableID
			tableInfo, err := s.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				if dispatcherStat.isRemoved.Load() {
					log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err))
					return events, false, nil
				}

				if errors.Is(err, &schemastore.TableDeletedError{}) {
					// After a table is truncated, it is possible to receive more dml events, just ignore is ok.
					// TODO: tables may be deleted in many ways, we need to check if it is safe to ignore later dmls in all cases.
					// We must send the remaining ddl events to the dispatcher in this case.
					appendDDLs(dataRange.EndTs)
					log.Warn("get table info failed, since the table is deleted", zap.Error(err))
					return events, false, nil
				}
				log.Panic("get table info failed, unknown reason", zap.Error(err))
			}
			dml = pevent.NewDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)
			dmlCount++
		}

		if err = dml.AppendRow(e, s.mounter.DecodeToChunk); err != nil {
			log.Panic("append row failed", zap.Error(err))
		}
	}
}
