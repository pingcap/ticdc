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
	FetchTableDDLEvents(dispatcherID common.DispatcherID, tableID int64, filter filter.Filter, startTs, endTs uint64) ([]pevent.DDLEvent, error)
	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)
}

// ScanLimit defines the limits for a scan operation
// todo: should consider the bytes of decoded events.
type scanLimit struct {
	// maxScannedBytes is the maximum number of bytes to scan
	maxScannedBytes int64
	// timeout is the maximum time to spend scanning
	timeout time.Duration
}

// eventScanner scans events from eventStore and schemaStore
type eventScanner struct {
	eventGetter  eventGetter
	schemaGetter schemaGetter
	mounter      pevent.Mounter
	epoch        uint64
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mounter pevent.Mounter,
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
) (int64, []event.Event, bool, error) {
	// Initialize scan session
	sess := s.newSession(ctx, dispatcherStat, dataRange, limit)
	defer sess.recordMetrics()

	// Fetch DDL events
	ddlEvents, err := s.fetchDDLEvents(sess)
	if err != nil {
		return 0, nil, false, err
	}

	// Get event iterator
	iter, err := s.getEventIterator(sess)
	if err != nil {
		return 0, nil, false, err
	}
	if iter == nil {
		return 0, s.handleEmptyIterator(ddlEvents, sess), false, nil
	}
	defer s.closeIterator(iter)

	// Execute event scanning and merging
	events, interrupted, err := s.scanAndMergeEvents(sess, ddlEvents, iter)
	return sess.scannedBytes, events, interrupted, err
}

// fetchDDLEvents retrieves DDL events for the scan
func (s *eventScanner) fetchDDLEvents(session *session) ([]pevent.DDLEvent, error) {
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
func (s *eventScanner) handleEmptyIterator(ddlEvents []pevent.DDLEvent, session *session) []event.Event {
	merger := newEventMerger(ddlEvents, session.dispatcherStat.id, s.epoch)
	events := merger.appendRemainingDDLs(session.dataRange.EndTs)
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
	ddlEvents []pevent.DDLEvent,
	iter eventstore.EventIterator,
) ([]event.Event, bool, error) {
	merger := newEventMerger(ddlEvents, session.dispatcherStat.id, s.epoch)
	processor := newDMLProcessor(s.mounter, s.schemaGetter)
	checker := newLimitChecker(session.limit.maxScannedBytes, session.limit.timeout, session.startTime)

	tableID := session.dataRange.Span.TableID
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

		session.addBytes(rawEvent.GetSize())
		session.scannedEntryCount++

		if isNewTxn && checker.checkLimits(session.scannedBytes) {
			if checker.canInterrupt(rawEvent.CRTs, session.lastCommitTs, session.dmlCount) {
				return s.interruptScan(session, merger, processor)
			}
		}

		if isNewTxn {
			if err = s.handleNewTransaction(session, merger, processor, rawEvent, tableID); err != nil {
				return nil, false, err
			}
			continue
		}

		if err = processor.appendRow(rawEvent, session.dispatcherStat.filter); err != nil {
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

// handleNewTransaction processes a new transaction event, and append the rawEvent to it.
func (s *eventScanner) handleNewTransaction(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	rawEvent *common.RawKVEntry,
	tableID int64,
) error {
	// Get table info
	tableInfo, err := s.schemaGetter.GetTableInfo(tableID, rawEvent.CRTs-1)
	if err != nil {
		errorHandler := newErrorHandler(session.dispatcherStat.id)
		shouldReturn, returnErr := errorHandler.handleSchemaError(err, session.dispatcherStat)
		if shouldReturn {
			if returnErr != nil {
				log.Error("get table info failed, unknown reason", zap.Error(err),
					zap.Stringer("dispatcherID", session.dispatcherStat.id),
					zap.Int64("tableID", tableID),
					zap.Uint64("getTableInfoStartTs", rawEvent.CRTs-1))
				return returnErr
			}
			// For table deleted case, we need to append remaining DDLs
			if errors.Is(err, &schemastore.TableDeletedError{}) {
				remainingEvents := merger.appendRemainingDDLs(session.dataRange.EndTs)
				session.events = append(session.events, remainingEvents...)
			}
			return nil
		}
	}

	// Check if batch should be flushed
	hasNewDDL := merger.hasMoreDDLs() && rawEvent.CRTs > merger.nextDDLFinishedTs()
	if processor.shouldFlushBatch(tableInfo.UpdateTS(), hasNewDDL) {
		events := merger.appendDMLEvent(processor.getCurrentBatch(), &session.lastCommitTs)
		session.events = append(session.events, events...)
		processor.flushBatch(tableInfo.UpdateTS())
	}

	// Process new transaction
	if err = processor.processNewTransaction(rawEvent, tableID, tableInfo, session.dispatcherStat.id, session.dispatcherStat.filter); err != nil {
		return err
	}

	session.lastCommitTs = rawEvent.CRTs
	session.dmlCount++
	return nil
}

// finalizeScan finalizes the scan when all events have been processed
func (s *eventScanner) finalizeScan(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
) ([]event.Event, error) {
	if err := processor.clearCache(session.dispatcherStat.filter); err != nil {
		return nil, err
	}
	// Append final batch
	events := merger.appendDMLEvent(processor.getCurrentBatch(), &session.lastCommitTs)
	session.events = append(session.events, events...)

	// Append remaining DDLs
	remainingEvents := merger.appendRemainingDDLs(session.dataRange.EndTs)
	session.events = append(session.events, remainingEvents...)

	return session.events, nil
}

// interruptScan handles scan interruption due to limits
func (s *eventScanner) interruptScan(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
) ([]event.Event, bool, error) {
	if err := processor.clearCache(session.dispatcherStat.filter); err != nil {
		return nil, false, err
	}
	// Append current batch
	events := merger.appendDMLEvent(processor.getCurrentBatch(), &session.lastCommitTs)
	session.events = append(session.events, events...)

	// Append DDLs up to last commit timestamp
	remainingEvents := merger.appendRemainingDDLs(session.lastCommitTs)
	session.events = append(session.events, remainingEvents...)

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

	// Result collection
	events []event.Event
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

// addBytes adds to the total bytes scanned
func (s *session) addBytes(size int64) {
	s.scannedBytes += size
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
}

// limitChecker manages scan limits and interruption logic
type limitChecker struct {
	maxBytes  int64
	timeout   time.Duration
	startTime time.Time
}

// newLimitChecker creates a new limit checker
func newLimitChecker(maxBytes int64, timeout time.Duration, startTime time.Time) *limitChecker {
	return &limitChecker{
		maxBytes:  maxBytes,
		timeout:   timeout,
		startTime: startTime,
	}
}

// checkLimits returns true if any limit has been reached
func (c *limitChecker) checkLimits(totalBytes int64) bool {
	return totalBytes > c.maxBytes || time.Since(c.startTime) > c.timeout
}

// canInterrupt checks if scan can be interrupted at current position
func (c *limitChecker) canInterrupt(currentTs, lastCommitTs uint64, dmlCount int) bool {
	return currentTs > lastCommitTs && dmlCount > 0
}

// eventMerger handles merging of DML and DDL events in timestamp order
type eventMerger struct {
	ddlEvents    []pevent.DDLEvent
	ddlIndex     int
	dispatcherID common.DispatcherID
	epoch        uint64
}

// newEventMerger creates a new event merger
func newEventMerger(
	ddlEvents []pevent.DDLEvent,
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

// appendDMLEvent appends a DML event and any preceding DDL events
func (m *eventMerger) appendDMLEvent(dml *pevent.BatchDMLEvent, lastCommitTs *uint64) []event.Event {
	if dml == nil || dml.Len() == 0 {
		return nil
	}

	var events []event.Event
	commitTs := dml.GetCommitTs()

	// Add any DDL events that should come before this DML event
	for m.ddlIndex < len(m.ddlEvents) && commitTs > m.ddlEvents[m.ddlIndex].FinishedTs {
		events = append(events, &m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}

	events = append(events, dml)
	*lastCommitTs = commitTs

	return events
}

// appendRemainingDDLs appends all remaining DDL events up to endTs
func (m *eventMerger) appendRemainingDDLs(endTs uint64) []event.Event {
	var events []event.Event

	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].FinishedTs <= endTs {
		events = append(events, &m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}

	events = append(events, pevent.NewResolvedEvent(endTs, m.dispatcherID, m.epoch))

	return events
}

// hasMoreDDLs returns true if there are more DDL events to process
func (m *eventMerger) hasMoreDDLs() bool {
	return m.ddlIndex < len(m.ddlEvents)
}

// nextDDLFinishedTs returns the finished timestamp of the next DDL event
func (m *eventMerger) nextDDLFinishedTs() uint64 {
	if !m.hasMoreDDLs() {
		return 0
	}
	return m.ddlEvents[m.ddlIndex].FinishedTs
}

// dmlProcessor handles DML event processing and batching
type dmlProcessor struct {
	mounter      pevent.Mounter
	schemaGetter schemaGetter

	// insertRowCache is used to cache the split update event's insert part of the current transaction.
	// It will be used to append to the current DML event when the transaction is finished.
	// And it will be cleared when the transaction is finished.
	insertRowCache        []*common.RawKVEntry
	currentDML            *pevent.DMLEvent
	batchDML              *pevent.BatchDMLEvent
	lastTableInfoUpdateTs uint64
}

// newDMLProcessor creates a new DML processor
func newDMLProcessor(mounter pevent.Mounter, schemaGetter schemaGetter) *dmlProcessor {
	return &dmlProcessor{
		mounter:        mounter,
		schemaGetter:   schemaGetter,
		batchDML:       pevent.NewBatchDMLEvent(),
		insertRowCache: make([]*common.RawKVEntry, 0),
	}
}

func (p *dmlProcessor) clearCache(filter filter.Filter) error {
	if len(p.insertRowCache) > 0 {
		for _, insertRow := range p.insertRowCache {
			if err := p.currentDML.AppendRow(insertRow, p.mounter.DecodeToChunk, filter); err != nil {
				return err
			}
		}
		p.insertRowCache = make([]*common.RawKVEntry, 0)
	}
	return nil
}

// processNewTransaction processes a new transaction event
func (p *dmlProcessor) processNewTransaction(
	rawEvent *common.RawKVEntry,
	tableID int64,
	tableInfo *common.TableInfo,
	dispatcherID common.DispatcherID,
	filter filter.Filter,
) error {
	if p.currentDML != nil && len(p.insertRowCache) > 0 {
		for _, insertRow := range p.insertRowCache {
			if err := p.currentDML.AppendRow(insertRow, p.mounter.DecodeToChunk, filter); err != nil {
				return err
			}
		}
		p.insertRowCache = make([]*common.RawKVEntry, 0)
	}
	// Create a new DMLEvent for the current transaction
	p.currentDML = pevent.NewDMLEvent(dispatcherID, tableID, rawEvent.StartTs, rawEvent.CRTs, tableInfo)
	p.batchDML.AppendDMLEvent(p.currentDML)
	return p.appendRow(rawEvent, filter)
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
//   - No current DML event exists to append to
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
func (p *dmlProcessor) appendRow(rawEvent *common.RawKVEntry, filter filter.Filter) error {
	if p.currentDML == nil {
		return errors.New("no current DML event to append to")
	}

	if !rawEvent.IsUpdate() {
		return p.currentDML.AppendRow(rawEvent, p.mounter.DecodeToChunk, filter)
	}

	shouldSplit, err := pevent.IsUKChanged(rawEvent, p.currentDML.TableInfo)
	if err != nil {
		return err
	}
	if shouldSplit {
		deleteRow, insertRow, err := rawEvent.SplitUpdate()
		if err != nil {
			return err
		}
		p.insertRowCache = append(p.insertRowCache, insertRow)
		return p.currentDML.AppendRow(deleteRow, p.mounter.DecodeToChunk, filter)
	}

	return p.currentDML.AppendRow(rawEvent, p.mounter.DecodeToChunk, filter)
}

// getCurrentBatch returns the current batch DML event
func (p *dmlProcessor) getCurrentBatch() *pevent.BatchDMLEvent {
	return p.batchDML
}

// shouldFlushBatch determines if the current batch should be flushed
func (p *dmlProcessor) shouldFlushBatch(tableInfoUpdateTs uint64, hasNewDDL bool) bool {
	return tableInfoUpdateTs != p.lastTableInfoUpdateTs || hasNewDDL
}

// flushBatch flushes the current batch and creates a new one
func (p *dmlProcessor) flushBatch(tableInfoUpdateTs uint64) {
	p.lastTableInfoUpdateTs = tableInfoUpdateTs
	p.batchDML = pevent.NewBatchDMLEvent()
}

// errorHandler manages error handling for different scenarios
type errorHandler struct {
	dispatcherID common.DispatcherID
}

// newErrorHandler creates a new error handler
func newErrorHandler(dispatcherID common.DispatcherID) *errorHandler {
	return &errorHandler{
		dispatcherID: dispatcherID,
	}
}

// handleSchemaError handles schema-related errors
func (h *errorHandler) handleSchemaError(err error, dispatcherStat *dispatcherStat) (shouldReturn bool, returnErr error) {
	if dispatcherStat.isRemoved.Load() {
		log.Warn("get table info failed, since the dispatcher is removed", zap.Error(err), zap.Stringer("dispatcherID", h.dispatcherID))
		return true, nil
	}

	if errors.Is(err, &schemastore.TableDeletedError{}) {
		log.Warn("get table info failed, since the table is deleted", zap.Error(err), zap.Stringer("dispatcherID", h.dispatcherID))
		return true, nil
	}

	return true, err
}
