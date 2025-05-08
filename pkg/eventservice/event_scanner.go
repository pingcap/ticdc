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

// Scan scans events from eventStore and schemaStore according to the given scanTask and limits
// It returns events in order (DDL and DML events are sorted by their commit timestamp)
// If scan hits any limit (bytes, rows, timeout), it will try to break at transaction boundary
func (s *EventScanner) Scan(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit ScanLimit,
) ([]event.Event, uint64, error) {
	startTime := time.Now()
	var events []event.Event
	var totalBytes int64
	var lastCommitTs uint64

	defer func() {
		metrics.EventServiceScanDuration.Observe(time.Since(startTime).Seconds())
	}()

	dispatcherID := dispatcherStat.id
	// 1. Fetch DDL events
	ddlEvents, err := s.schemaStore.FetchTableDDLEvents(
		dataRange.Span.TableID,
		dispatcherStat.filter,
		dataRange.StartTs,
		dataRange.EndTs,
	)
	if err != nil {
		log.Error("get ddl events failed", zap.Error(err))
		return nil, 0, err
	}

	// 2. Get event iterator from eventStore
	iter, err := s.eventStore.GetIterator(dispatcherID, dataRange)
	if err != nil {
		log.Error("read events failed", zap.Error(err))
		return nil, 0, err
	}

	// After all the events are sent, we need to drain the remaining ddlEvents.
	appendRemainingDDLEvents := func() {
		for _, e := range ddlEvents {
			ep := &e
			events = append(events, ep)
		}
		lastCommitTs = dataRange.EndTs
	}

	if iter == nil {
		appendRemainingDDLEvents()
		return events, lastCommitTs, nil
	}

	defer func() {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			metricEventStoreOutputKv.Add(float64(eventCount))
		}
	}()

	// sendDML is used to send the dml event to the dispatcher.
	// It returns true if the dml event is sent successfully.
	// Otherwise, it returns false.
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

	appendWaterMark := func(resolvedTs uint64) {
		if resolvedTs == 0 {
			return
		}
		events = append(events, pevent.ResolvedEvent{
			DispatcherID: dispatcherID,
			ResolvedTs:   resolvedTs,
			State:        event.EventSenderStateNormal,
			Version:      event.ResolvedEventVersion,
		})
	}

	// 3. Send the events to the dispatcher.
	var dml *pevent.DMLEvent
	for {
		select {
		case <-ctx.Done():
			log.Warn("scan exits since context done", zap.Error(ctx.Err()))
			return events, lastCommitTs, ctx.Err()
		default:
		}

		// Node: The first event of the txn must return isNewTxn as true.
		e, isNewTxn, err := iter.Next()
		if err != nil {
			log.Panic("read events failed", zap.Error(err))
		}

		// Send the last dml to the dispatcher.
		if e == nil {
			appendDML(dml)
			appendRemainingDDLEvents()
			appendWaterMark(dataRange.EndTs)
			return events, lastCommitTs, nil
		}

		eSize := len(e.Key) + len(e.Value) + len(e.OldValue)
		totalBytes += int64(eSize)
		elapsed := time.Since(startTime)

		if (totalBytes > limit.MaxBytes || elapsed > limit.Timeout) && e.CRTs > lastCommitTs {
			appendWaterMark(lastCommitTs)
			return events, lastCommitTs, nil
		}

		if isNewTxn {
			appendDML(dml)
			tableID := dataRange.Span.TableID
			tableInfo, err := s.schemaStore.GetTableInfo(tableID, e.CRTs-1)
			if err != nil {
				if dispatcherStat.isRemoved.Load() {
					log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err))
					return events, lastCommitTs, nil
				}

				if errors.Is(err, &schemastore.TableDeletedError{}) {
					// After a table is truncated, it is possible to receive more dml events, just ignore is ok.
					// TODO: tables may be deleted in many ways, we need to check if it is safe to ignore later dmls in all cases.
					// We must send the remaining ddl events to the dispatcher in this case.
					appendRemainingDDLEvents()
					log.Warn("get table info failed, since the table is deleted", zap.Error(err))
					return events, lastCommitTs, nil
				}
				log.Panic("get table info failed, unknown reason", zap.Error(err))
			}
			dml = pevent.NewDMLEvent(dispatcherID, tableID, e.StartTs, e.CRTs, tableInfo)
		}

		if err = dml.AppendRow(e, s.mounter.DecodeToChunk); err != nil {
			log.Panic("append row failed", zap.Error(err))
		}
	}
}
