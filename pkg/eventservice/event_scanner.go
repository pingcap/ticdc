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
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

// eventGetter is the interface for getting iterator of events
// The implementation of eventGetter is eventstore.EventStore
type eventGetter interface {
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (eventstore.EventIterator, error)
}

// schemaGetter is the interface for getting schema info and ddl events
// The implementation of schemaGetter is schemastore.SchemaStore
type schemaGetter interface {
	FetchTableDDLEvents(dispatcherID common.DispatcherID, tableID int64, filter filter.Filter, startTs, endTs uint64) ([]event.DDLEvent, error)
	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)
}

// ScanLimit defines the limits for a scan operation
// todo: should consider the bytes of decoded events.
type scanLimit struct {
	// maxDMLBytes is the maximum number of bytes to scan
	maxDMLBytes int64
	// timeout is the maximum time to spend scanning
	timeout time.Duration
}

// eventScanner scans events from eventStore and schemaStore
type eventScanner struct {
	eventGetter  eventGetter
	schemaGetter schemaGetter
	mounter      event.Mounter
	epoch        uint64
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mounter event.Mounter,
	epoch uint64,
) *eventScanner {
	return &eventScanner{
		eventGetter:  eventStore,
		schemaGetter: schemaStore,
		mounter:      mounter,
		epoch:        epoch,
	}
}

// scan retrieves and processes events from both eventStore and schemaStore based on the provided scanTask and limits.
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
func (s *eventScanner) scan(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit scanLimit,
) ([]event.Event, bool, error) {
	// Initialize scan session
	sess := s.newSession(ctx, dispatcherStat, dataRange, limit)
	defer sess.recordMetrics()

	// Fetch DDL events
	ddlEvents, err := s.fetchDDLEvents(sess)
	if err != nil {
		return nil, false, err
	}

	// Get event iterator
	iter, err := s.getEventIterator(sess)
	if err != nil {
		return nil, false, err
	}
	if iter == nil {
		return s.handleEmptyIterator(ddlEvents, sess), false, nil
	}
	defer s.closeIterator(iter)

	// Execute event scanning and merging
	events, interrupted, err := s.scanAndMergeEvents(sess, ddlEvents, iter)
	return events, interrupted, err
}

// fetchDDLEvents retrieves DDL events for the scan
func (s *eventScanner) fetchDDLEvents(session *session) ([]event.DDLEvent, error) {
	ddlEvents, err := s.schemaGetter.FetchTableDDLEvents(
		session.dispatcherStat.info.GetID(),
		session.dataRange.Span.TableID,
		session.dispatcherStat.filter,
		session.dataRange.StartTs,
		session.dataRange.EndTs,
	)
	if err != nil {
		log.Error("get ddl events failed", zap.Error(err), zap.Stringer("dispatcherID", session.dispatcherStat.id))
		return nil, err
	}
	return ddlEvents, nil
}

// getEventIterator gets the event iterator for DML events
func (s *eventScanner) getEventIterator(session *session) (eventstore.EventIterator, error) {
	iter, err := s.eventGetter.GetIterator(session.dispatcherStat.id, session.dataRange)
	if err != nil {
		log.Error("read events failed", zap.Error(err), zap.Stringer("dispatcherID", session.dispatcherStat.id))
		return nil, err
	}

	return iter, nil
}

// handleEmptyIterator handles the case when there are no DML events
func (s *eventScanner) handleEmptyIterator(ddlEvents []event.DDLEvent, session *session) []event.Event {
	merger := newEventMerger(ddlEvents, session.dispatcherStat.id, s.epoch)
	events := merger.resolveDDLEvents(session.dataRange.EndTs)
	return events
}

// closeIterator closes the event iterator and records metrics
func (s *eventScanner) closeIterator(iter eventstore.EventIterator) {
	if iter != nil {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			metricEventStoreOutputKv.Add(float64(eventCount))
		}
	}
}

// scanAndMergeEvents performs the main scanning and merging logic
func (s *eventScanner) scanAndMergeEvents(
	session *session,
	ddlEvents []event.DDLEvent,
	iter eventstore.EventIterator,
) ([]event.Event, bool, error) {
	merger := newEventMerger(ddlEvents, session.dispatcherStat.id, s.epoch)
	processor := newDMLProcessor(s.mounter, s.schemaGetter)

	tableID := session.dataRange.Span.TableID
	dispatcher := session.dispatcherStat
	for {
		shouldStop, err := s.checkScanConditions(session)
		if err != nil {
			return nil, false, err
		}
		if shouldStop {
			return nil, false, nil
		}

		rawEvent, isNewTxn := iter.Next()
		if rawEvent == nil {
			events, err := s.finalizeScan(session, merger, processor)
			return events, false, err
		}

		session.observeRawEntry(rawEvent)
		if isNewTxn {
			tableInfo, err := s.getTableInfo4Txn(dispatcher, tableID, rawEvent.CRTs-1)
			if err != nil {
				return nil, false, err
			}
			if tableInfo == nil {
				log.Warn("table info not found, stop scanning, return nothing",
					zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID))
				return nil, false, nil
			}

			if err = processor.flushCurrentTxn(); err != nil {
				return nil, false, err
			}
			s.tryFlushBatch(merger, processor, session, rawEvent, tableInfo)

			if (session.eventBytes+processor.batchDML.GetSize()) > session.limit.maxDMLBytes ||
				time.Since(session.startTime) > session.limit.timeout {
				return s.interruptScan(session, merger, processor)
			}
			s.startNewTxn(session, processor, rawEvent.StartTs, rawEvent.CRTs, tableInfo, tableID)
		}

		if err = processor.appendRow(rawEvent); err != nil {
			log.Error("append row failed", zap.Error(err),
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", tableID),
				zap.Uint64("startTs", rawEvent.StartTs),
				zap.Uint64("commitTs", rawEvent.CRTs))
			return nil, false, err
		}

	}
}

// checkScanConditions checks context cancellation and dispatcher status
// return true if the scan should be stopped, false otherwise
func (s *eventScanner) checkScanConditions(session *session) (bool, error) {
	if session.isContextDone() {
		log.Warn("scan exits since context done", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Error(context.Cause(session.ctx)))
		return true, context.Cause(session.ctx)
	}

	if !session.dispatcherStat.isReadyRecevingData.Load() {
		return true, nil
	}

	return false, nil
}

func (s *eventScanner) getTableInfo4Txn(dispatcher *dispatcherStat, tableID int64, ts uint64) (*common.TableInfo, error) {
	tableInfo, err := s.schemaGetter.GetTableInfo(tableID, ts)
	if err == nil {
		return tableInfo, nil
	}

	if dispatcher.isRemoved.Load() {
		log.Warn("get table info failed, since the dispatcher is removed",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Error(err))
		return nil, nil
	}

	if errors.Is(err, &schemastore.TableDeletedError{}) {
		log.Warn("get table info failed, since the table is deleted",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Error(err))
		return nil, nil
	}

	log.Error("get table info failed, unknown reason",
		zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
		zap.Uint64("ts", ts), zap.Error(err))
	return nil, err
}

func (s *eventScanner) tryFlushBatch(
	merger *eventMerger,
	processor *dmlProcessor,
	session *session,
	rawEvent *common.RawKVEntry,
	tableInfo *common.TableInfo,
) {
	resolvedBatch := processor.getResolvedBatchDML()
	if resolvedBatch == nil {
		return
	}
	// Check if batch should be flushed
	tableUpdated := resolvedBatch.TableInfo.UpdateTS() != tableInfo.UpdateTS()
	hasNewDDL := merger.hasMoreDDLs(rawEvent.CRTs)
	if hasNewDDL || tableUpdated {
		events := merger.appendDMLEvent(resolvedBatch, &session.lastCommitTs)
		session.appendEvents(events)
		processor.resetBatchDML()
	}
}

func (s *eventScanner) startNewTxn(
	session *session,
	processor *dmlProcessor,
	startTs, commitTs uint64,
	tableInfo *common.TableInfo,
	tableID int64,
) {
	processor.startNewTxn(session.dispatcherStat.id, tableID, startTs, commitTs, tableInfo)
	session.lastCommitTs = commitTs
	session.dmlCount++
}

// finalizeScan finalizes the scan when all events have been processed
func (s *eventScanner) finalizeScan(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
) ([]event.Event, error) {
	if err := processor.clearCache(); err != nil {
		return nil, err
	}
	// Append final batch
	events := merger.appendDMLEvent(processor.getResolvedBatchDML(), &session.lastCommitTs)
	session.appendEvents(events)

	// Append remaining DDLs
	remainingEvents := merger.resolveDDLEvents(session.dataRange.EndTs)
	session.appendEvents(remainingEvents)

	return session.events, nil
}

// interruptScan handles scan interruption due to limits
func (s *eventScanner) interruptScan(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	newCommitTs uint64,
) ([]event.Event, bool, error) {
	if err := processor.clearCache(); err != nil {
		return nil, false, err
	}
	// Append current batch
	events := merger.appendDMLEvent(processor.getResolvedBatchDML(), &session.lastCommitTs)
	session.appendEvents(events)

	// Append DDLs up to last commit timestamp
	remainingEvents := merger.resolveDDLEvents(session.lastCommitTs)
	session.appendEvents(remainingEvents)

	if newCommitTs != session.lastCommitTs {
		events = append(events, event.NewResolvedEvent(session.lastCommitTs, merger.dispatcherID, merger.epoch))
	}
	return session.events, true, nil
}

// session manages the state and context of a scan operation
type session struct {
	ctx            context.Context
	dispatcherStat *dispatcherStat
	dataRange      common.DataRange
	limit          scanLimit

	// State tracking
	startTime         time.Time
	lastCommitTs      uint64
	scannedBytes      int64
	scannedEntryCount int
	// dmlCount is the count of transactions.
	dmlCount int

	// Result collection, including DDL, BatchedDML, ResolvedTs events in the timestamp order.
	events     []event.Event
	eventBytes int64
}

// newSession creates a new scan session
func (s *eventScanner) newSession(
	ctx context.Context,
	dispatcherStat *dispatcherStat,
	dataRange common.DataRange,
	limit scanLimit,
) *session {
	return &session{
		ctx:            ctx,
		dispatcherStat: dispatcherStat,
		dataRange:      dataRange,
		limit:          limit,
		startTime:      time.Now(),
		events:         make([]event.Event, 0),
	}
}

// observeRawEntry adds to the total bytes scanned
func (s *session) observeRawEntry(entry *common.RawKVEntry) {
	s.scannedBytes += entry.GetSize()
	s.scannedEntryCount++
}

// isContextDone checks if the context is cancelled
func (s *session) isContextDone() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

// recordMetrics records the scan duration metrics
func (s *session) recordMetrics() {
	metrics.EventServiceScanDuration.Observe(time.Since(s.startTime).Seconds())
	metrics.EventServiceScannedCount.Observe(float64(s.scannedEntryCount))
	metrics.EventServiceScannedTxnCount.Observe(float64(s.dmlCount))
	metrics.EventServiceScannedDMLSize.Observe(float64(s.eventBytes))
}

func (s *session) appendEvents(events []event.Event) {
	s.events = append(s.events, events...)
	var nBytes int64
	for _, item := range events {
		nBytes += item.GetSize()
	}
	s.eventBytes += nBytes
}

// eventMerger handles merging of DML and DDL events in timestamp order
type eventMerger struct {
	ddlEvents    []event.DDLEvent
	ddlIndex     int
	dispatcherID common.DispatcherID
	epoch        uint64
}

// newEventMerger creates a new event merger
func newEventMerger(
	ddlEvents []event.DDLEvent,
	dispatcherID common.DispatcherID,
	epoch uint64,
) *eventMerger {
	return &eventMerger{
		ddlEvents:    ddlEvents,
		ddlIndex:     0,
		dispatcherID: dispatcherID,
		epoch:        epoch,
	}
}

func (m *eventMerger) popDDLEvents(until uint64) []event.Event {
	var events []event.Event

	// Add all DDL events that are before the given timestamp
	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].FinishedTs <= until {
		events = append(events, &m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}
	return events
}

// appendDMLEvent appends a DML event and any preceding DDL events
func (m *eventMerger) appendDMLEvent(dml *event.BatchDMLEvent, lastCommitTs *uint64) []event.Event {
	if dml == nil || dml.Len() == 0 {
		return nil
	}

	commitTs := dml.GetCommitTs()
	events := m.popDDLEvents(commitTs)
	events = append(events, dml)
	*lastCommitTs = commitTs
	return events
}

// resolveDDLEvents return all remaining DDL events up to endTs
func (m *eventMerger) resolveDDLEvents(endTs uint64) []event.Event {
	events := m.popDDLEvents(endTs)
	return events
}

// hasMoreDDLs return true if there are DDLs
func (m *eventMerger) hasMoreDDLs(commitTs uint64) bool {
	return m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].FinishedTs < commitTs
}

// dmlProcessor handles DML event processing and batching
type dmlProcessor struct {
	mounter      event.Mounter
	schemaGetter schemaGetter

	// insertRowCache is used to cache the split update event's insert part of the current transaction.
	// It will be used to append to the current DML event when the transaction is finished.
	// And it will be cleared when the transaction is finished.
	insertRowCache []*common.RawKVEntry

	// currentDML is the transaction that is handling now
	currentDML *event.DMLEvent

	batchDML *event.BatchDMLEvent
}

// newDMLProcessor creates a new DML processor
func newDMLProcessor(mounter event.Mounter, schemaGetter schemaGetter) *dmlProcessor {
	return &dmlProcessor{
		mounter:        mounter,
		schemaGetter:   schemaGetter,
		insertRowCache: make([]*common.RawKVEntry, 0),
	}
}

// startNewTxn should be called after flush the current transaction
func (p *dmlProcessor) startNewTxn(
	dispatcherID common.DispatcherID,
	tableID int64,
	startTs uint64, commitTs uint64,
	tableInfo *common.TableInfo,
) {
	if p.currentDML != nil {
		log.Panic("there is a transaction not flushed yet")
	}
	if p.batchDML == nil {
		p.batchDML = event.NewBatchDMLEvent()
	}
	p.currentDML = event.NewDMLEvent(dispatcherID, tableID, startTs, commitTs, tableInfo)
	p.batchDML.AppendDMLEvent(p.currentDML)
}

func (p *dmlProcessor) flushCurrentTxn() error {
	if p.currentDML == nil {
		log.Panic("no current DML event to flush")
	}
	err := p.clearCache()
	if err != nil {
		return err
	}
	p.currentDML = nil
	return nil
}

// appendRow appends a row to the current DML event.
//
// This method processes a raw KV entry and appends it to the current DML event. It handles
// different types of operations (insert, delete, update) with special handling for updates
// that modify unique key values.
//
// Parameters:
//   - rawEvent: The raw KV entry containing the row data and operation type
//
// Returns:
//   - error: Returns an error if:
//   - Unique key change detection fails
//   - Update split operation fails
//   - Row append operation fails
//
// The method follows this logic:
// 1. Checks if there's a current DML event to append to
// 2. For non-update operations, directly appends the row
// 3. For update operations:
//   - Checks if the update modifies any unique key values
//   - If unique keys are modified, splits the update into delete+insert operations
//   - Caches the insert part for later processing
//   - Appends the delete part to the current event
//
// 4. For normal updates (no unique key changes), appends the row directly
func (p *dmlProcessor) appendRow(rawEvent *common.RawKVEntry) error {
	if p.currentDML == nil {
		log.Panic("no current DML event to append to")
	}

	if !rawEvent.IsUpdate() {
		return p.currentDML.AppendRow(rawEvent, p.mounter.DecodeToChunk)
	}

	shouldSplit, err := event.IsUKChanged(rawEvent, p.currentDML.TableInfo)
	if err != nil {
		return err
	}

	if !shouldSplit {
		return p.currentDML.AppendRow(rawEvent, p.mounter.DecodeToChunk)
	}

	deleteRow, insertRow, err := rawEvent.SplitUpdate()
	if err != nil {
		return err
	}
	p.insertRowCache = append(p.insertRowCache, insertRow)
	return p.currentDML.AppendRow(deleteRow, p.mounter.DecodeToChunk)
}

// getResolvedBatchDML returns the current batch DML event
func (p *dmlProcessor) getResolvedBatchDML() *event.BatchDMLEvent {
	return p.batchDML
}

func (p *dmlProcessor) resetBatchDML() {
	p.batchDML = nil
}

func (p *dmlProcessor) clearCache() error {
	if len(p.insertRowCache) > 0 {
		for _, insertRow := range p.insertRowCache {
			if err := p.currentDML.AppendRow(insertRow, p.mounter.DecodeToChunk); err != nil {
				return err
			}
		}
		p.insertRowCache = make([]*common.RawKVEntry, 0)
	}
	return nil
}
