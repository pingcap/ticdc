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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// eventGetter is the interface for getting iterator of events
// The implementation of eventGetter is eventstore.EventStore
type eventGetter interface {
	GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) eventstore.EventIterator
}

// schemaGetter is the interface for getting schema info and ddl events
// The implementation of schemaGetter is schemastore.SchemaStore
type schemaGetter interface {
	FetchTableDDLEvents(keyspace common.KeyspaceMeta, dispatcherID common.DispatcherID, tableID int64, filter filter.Filter, startTs, endTs uint64) ([]event.DDLEvent, error)
	GetTableInfo(keyspace common.KeyspaceMeta, tableID int64, ts uint64) (*common.TableInfo, error)
}

// ScanLimit defines the limits for a scan operation
type scanLimit struct {
	// maxDMLBytes is the maximum number of bytes to scan
	maxDMLBytes int64

	// Only used in unit test, please do not set it in other places.
	// When it is set to true, the scan will count DMLEvent as 1 byte,
	// otherwise it will count the size of DMLEvent.
	isInUnitTest bool
}

// eventScanner scans events from eventStore and schemaStore
type eventScanner struct {
	eventGetter  eventGetter
	schemaGetter schemaGetter
	mounter      event.Mounter
	mode         int64
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mounter event.Mounter,
	mode int64,
) *eventScanner {
	return &eventScanner{
		eventGetter:  eventStore,
		schemaGetter: schemaStore,
		mounter:      mounter,
		mode:         mode,
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
	sess := newSession(ctx, dispatcherStat, dataRange, limit)
	defer sess.recordMetrics()

	// Fetch DDL events
	start := time.Now()
	events, err := s.fetchDDLEvents(dispatcherStat, dataRange)
	if err != nil {
		return 0, nil, false, err
	}
	metrics.EventServiceGetDDLEventDuration.Observe(time.Since(start).Seconds())

	iter := s.eventGetter.GetIterator(dispatcherStat.info.GetID(), dataRange)
	if iter == nil {
		resolved := event.NewResolvedEvent(dataRange.CommitTsEnd, dispatcherStat.id, dispatcherStat.epoch)
		events = append(events, resolved)
		sess.appendEvents(events)
		return 0, sess.events, false, nil
	}
	defer s.closeIterator(iter)

	// Execute event scanning and merging
	merger := newEventMerger(events)
	interrupted, err := s.scanAndMergeEvents(sess, merger, iter)
	return sess.eventBytes, sess.events, interrupted, err
}

// fetchDDLEvents retrieves DDL events which finishedTs are within the range (start, end]
func (s *eventScanner) fetchDDLEvents(stat *dispatcherStat, dataRange common.DataRange) ([]event.Event, error) {
	dispatcherID := stat.info.GetID()
	keyspaceMeta := common.KeyspaceMeta{
		ID:   stat.info.GetTableSpan().KeyspaceID,
		Name: stat.changefeedStat.changefeedID.Keyspace(),
	}
	ddlEvents, err := s.schemaGetter.FetchTableDDLEvents(
		keyspaceMeta,
		dispatcherID,
		dataRange.Span.TableID,
		stat.filter,
		dataRange.CommitTsStart,
		dataRange.CommitTsEnd,
	)
	if err != nil {
		log.Error("get ddl events failed", zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", dataRange.Span.TableID), zap.Error(err))
		return nil, err
	}

	result := make([]event.Event, 0, len(ddlEvents))
	for _, item := range ddlEvents {
		result = append(result, &item)
	}
	return result, nil
}

// closeIterator closes the event iterator and records metrics
func (s *eventScanner) closeIterator(iter eventstore.EventIterator) {
	if iter != nil {
		eventCount, _ := iter.Close()
		if eventCount != 0 {
			updateMetricEventStoreOutputKv(s.mode, float64(eventCount))
		}
	}
}

// scanAndMergeEvents performs the main scanning and merging logic
func (s *eventScanner) scanAndMergeEvents(
	session *session,
	merger *eventMerger,
	iter eventstore.EventIterator,
) (bool, error) {
	tableID := session.dataRange.Span.TableID
	dispatcher := session.dispatcherStat
	processor := newDMLProcessor(s.mounter, s.schemaGetter, dispatcher.filter, dispatcher.info.IsOutputRawChangeEvent())

	for {
		shouldStop, err := s.checkScanConditions(session)
		if err != nil {
			return false, err
		}
		if shouldStop {
			return false, nil
		}

		rawEvent, isNewTxn := iter.Next()
		if rawEvent == nil {
			err = finalizeScan(merger, processor, session, session.dataRange.CommitTsEnd)
			return false, err
		}

		session.observeRawEntry(rawEvent)
		if isNewTxn {
			tableInfo, err := s.getTableInfo4Txn(dispatcher, tableID, rawEvent.CRTs-1)
			if err != nil {
				return false, err
			}
			// table is deleted, still append remaining DDL event and resolved event.
			if tableInfo == nil {
				err = finalizeScan(merger, processor, session, rawEvent.CRTs-1)
				return false, err
			}

			if err = s.commitTxn(session, merger, processor, rawEvent.CRTs, tableInfo.GetUpdateTS()); err != nil {
				return false, err
			}

			if session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) &&
				merger.canInterrupt(rawEvent.CRTs, processor.batchDML) {
				interruptScan(session, merger, processor, rawEvent.CRTs, rawEvent.StartTs)
				return true, nil
			}

			err = s.startTxn(session, processor, rawEvent.StartTs, rawEvent.CRTs, tableInfo, tableID)
			if err != nil {
				return false, err
			}
		}

		if err = processor.appendRow(rawEvent); err != nil {
			log.Error("append row failed", zap.Error(err),
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", tableID),
				zap.Uint64("startTs", rawEvent.StartTs),
				zap.Uint64("commitTs", rawEvent.CRTs))
			return false, err
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
	return session.dispatcherStat.isRemoved.Load(), nil
}

func (s *eventScanner) getTableInfo4Txn(dispatcher *dispatcherStat, tableID int64, ts uint64) (*common.TableInfo, error) {
	keyspaceMeta := common.KeyspaceMeta{
		ID:   dispatcher.info.GetTableSpan().KeyspaceID,
		Name: dispatcher.info.GetChangefeedID().Keyspace(),
	}
	tableInfo, err := s.schemaGetter.GetTableInfo(keyspaceMeta, tableID, ts)
	if err == nil {
		return tableInfo, nil
	}

	if dispatcher.isRemoved.Load() {
		log.Warn("get table info failed, but the dispatcher is removed from the event service",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Error(err))
		return nil, nil
	}

	if errors.Is(err, &schemastore.TableDeletedError{}) {
		log.Warn("get table info failed, since the table is deleted",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts))
		return nil, nil
	}

	log.Error("get table info failed, unknown reason",
		zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
		zap.Uint64("ts", ts), zap.Error(err))
	return nil, err
}

func (s *eventScanner) startTxn(
	session *session,
	processor *dmlProcessor,
	startTs, commitTs uint64,
	tableInfo *common.TableInfo,
	tableID int64,
) error {
	shouldSplitTxn := session.dispatcherStat.txnAtomicity.ShouldSplitTxn()
	err := processor.startTxn(session.dispatcherStat.id, tableID, tableInfo, startTs, commitTs, shouldSplitTxn)
	if err != nil {
		return err
	}
	session.dmlCount++
	return nil
}

func (s *eventScanner) commitTxn(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	eventCommitTs, tableInfoUpdateTs uint64,
) error {
	if err := processor.commitTxn(); err != nil {
		return err
	}
	currentBatchDML := processor.getCurrentBatchDML()

	// Use DMLCount() instead of Len() to check if the batchDML is empty
	// because the batchDML may have some skipped rows, so the Len() can be 0 even if the batchDML is not empty
	if currentBatchDML == nil || currentBatchDML.DMLCount() == 0 {
		return nil
	}

	// Check if should flush the current batchDML and reset a new one
	tableUpdated := currentBatchDML.TableInfo.GetUpdateTS() != tableInfoUpdateTs
	hasNewDDL := merger.hasDDLLessThanCommitTs(eventCommitTs)
	if hasNewDDL || tableUpdated {
		events := merger.mergeWithPrecedingDDLs(currentBatchDML)
		session.appendEvents(events)
		processor.resetBatchDML()
	}
	return nil
}

// finalizeScan finalizes the scan when all events have been processed
// it's called when the iterator is nil, always indicates that all entries
// with the same commit-ts is processed, so it's ok to append resolved-ts event
func finalizeScan(
	merger *eventMerger,
	processor *dmlProcessor,
	sess *session,
	endTs uint64,
) error {
	if err := processor.commitTxn(); err != nil {
		return err
	}

	resolvedBatch := processor.getCurrentBatchDML()
	events := merger.mergeWithPrecedingDDLs(resolvedBatch)
	events = append(events, merger.resolveDDLEvents(endTs)...)

	resolveTs := event.NewResolvedEvent(endTs, sess.dispatcherStat.id, sess.dispatcherStat.epoch)
	events = append(events, resolveTs)
	sess.appendEvents(events)
	return nil
}

// interruptScan handles scan interruption due to limits
// it's called when the scan exceeds the limit, and it commits the current transaction,
// but other there may have some entries with the same commit-ts not processed yet,
// so only append the resolved-ts event if the new commit-ts is different from the last commit-ts.
func interruptScan(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	newCommitTs uint64,
	newStartTs uint64,
) {
	// Append current batch
	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

	if newCommitTs != merger.lastBatchDMLCommitTs {
		// lastCommitTs may be 0, if the scanner timeout and no one row scanned.
		// this usually happens when the CPU is overloaded.
		if merger.lastBatchDMLCommitTs == 0 {
			log.Info("interrupt scan when no DML event is scanned",
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", session.dataRange.Span.TableID),
				zap.Uint64("newCommitTs", newCommitTs),
				zap.Int("scannedEntryCount", session.scannedEntryCount),
				zap.Int("txnCount", session.dmlCount),
				zap.Duration("duration", time.Since(session.startTime)))
		} else {
			// This means we interrupt the scan at a position where the commitTs is different from the last batchDML commitTs
			// In this case, we need to append the DDL less than or equal to the last batchDML commitTs and the resolved-ts event with the last batchDML commitTs
			events = append(events, merger.resolveDDLEvents(merger.lastBatchDMLCommitTs)...)
			resolvedTs := event.NewResolvedEvent(merger.lastBatchDMLCommitTs, session.dispatcherStat.id, session.dispatcherStat.epoch)
			events = append(events, resolvedTs)
			log.Debug("scan interrupted at different commitTs with new event", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Uint64("CommitTs", merger.lastBatchDMLCommitTs), zap.Uint64("newCommitTs", newCommitTs), zap.Duration("duration", time.Since(session.startTime)))
		}
	} else {
		startTs := uint64(0)
		if processor.currentTxn != nil {
			startTs = processor.currentTxn.CurrentDMLEvent.GetStartTs()
		}
		log.Debug("scan interrupted at the same commitTs with new event", zap.Stringer("dispatcherID", session.dispatcherStat.id), zap.Uint64("startTs", startTs), zap.Uint64("commitTs", merger.lastBatchDMLCommitTs), zap.Uint64("newStartTs", newStartTs), zap.Uint64("newCommitTs", newCommitTs), zap.Duration("duration", time.Since(session.startTime)))
	}
	session.appendEvents(events)
}

// session manages the state and context of a scan operation
type session struct {
	ctx            context.Context
	dispatcherStat *dispatcherStat
	dataRange      common.DataRange

	limit scanLimit
	// State tracking
	startTime time.Time

	scannedBytes      int64
	scannedEntryCount int
	// dmlCount is the count of transactions.
	dmlCount int

	// Result collection, including DDL, BatchedDML, ResolvedTs events in the timestamp order.
	events     []event.Event
	eventBytes int64
}

// newSession creates a new scan session
func newSession(
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

	if !s.limit.isInUnitTest {
		for _, item := range events {
			s.eventBytes += item.GetSize()
		}
		return
	}

	// only for unit test
	for _, item := range events {
		if item.GetType() == event.TypeBatchDMLEvent {
			batchDML := item.(*event.BatchDMLEvent)
			s.eventBytes += int64(len(batchDML.DMLEvents))
		} else {
			s.eventBytes += 1
		}
	}
}

func (s *session) exceedLimit(nBytes int64, batchDMLs ...*event.BatchDMLEvent) bool {
	if s.limit.isInUnitTest && len(batchDMLs) > 0 {
		batchDML := batchDMLs[0]
		eventCount := len(batchDML.DMLEvents)
		return (s.eventBytes + int64(eventCount)) >= s.limit.maxDMLBytes
	}

	return (s.eventBytes + nBytes) >= s.limit.maxDMLBytes
}

// eventMerger handles merging of DML and DDL events in timestamp order
type eventMerger struct {
	ddlEvents []event.Event
	ddlIndex  int
	// Record the last batch dml that has been merge with preceding DDLs
	lastBatchDMLCommitTs uint64
}

// newEventMerger creates a new event merger
func newEventMerger(
	ddlEvents []event.Event,
) *eventMerger {
	return &eventMerger{
		ddlEvents: ddlEvents,
		ddlIndex:  0,
	}
}

// mergeWithPrecedingDDLs returns the DML event along with all preceding DDL events in timestamp order.
func (m *eventMerger) mergeWithPrecedingDDLs(batchDML *event.BatchDMLEvent) []event.Event {
	if batchDML == nil || batchDML.DMLCount() == 0 {
		return nil
	}

	commitTs := batchDML.GetCommitTs()
	var events []event.Event
	// Collect all DDL events with commitTs < dml.commitTs
	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() < commitTs {
		events = append(events, m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}

	events = append(events, batchDML)

	m.lastBatchDMLCommitTs = commitTs
	return events
}

// resolveDDLEvents return all remaining DDL events that have not been processed yet.
func (m *eventMerger) resolveDDLEvents(endTs uint64) []event.Event {
	var events []event.Event
	for m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() <= endTs {
		events = append(events, m.ddlEvents[m.ddlIndex])
		m.ddlIndex++
	}
	return events
}

// hasDDLLessThanCommitTs return true if there are DDLs
func (m *eventMerger) hasDDLLessThanCommitTs(commitTs uint64) bool {
	return m.ddlIndex < len(m.ddlEvents) && m.ddlEvents[m.ddlIndex].GetCommitTs() < commitTs
}

// hasDDLAtCommitTs checks if there's a DDL event at the specified commitTs
// This method doesn't modify the ddlIndex, it's used for checking only
func (m *eventMerger) hasDDLAtCommitTs(commitTs uint64) bool {
	for i := m.ddlIndex; i < len(m.ddlEvents); i++ {
		ddlCommitTs := m.ddlEvents[i].GetCommitTs()
		if ddlCommitTs == commitTs {
			return true
		}
		// Since DDL events are sorted by commitTs, if we find a larger commitTs, we can stop
		if ddlCommitTs > commitTs {
			break
		}
	}
	return false
}

// canInterrupt determines if we can interrupt the scan at the current position when reaching scan limits.
// The function ensures that DML and DDL events with the same commitTs are processed together atomically.
//
// Logic:
// 1. If currentDML.commitTs != newCommitTs: Can interrupt (different transactions)
// 2. If currentDML.commitTs == newCommitTs: Check if there are DDL events at this commitTs
//   - If no DDL at this commitTs: Can interrupt
//   - If DDL exists at this commitTs: Cannot interrupt (must process together)
//
// Examples:
//
// Case 1 - Different commitTs, can interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2)
//	          ▲
//	          └── currentDML.commitTs=x+1, newCommitTs=x+2, can interrupt
//
// Case 2 - Same commitTs, no DDL, can interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2) -> DML3(x+2)
//	                       ▲
//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, no DDL at x+2, can interrupt
//
// Case 3 - Same commitTs, has DDL, cannot interrupt:
// Event sequence:
//
//	DML1(x+1) -> DML2(x+2) -> DDL(x+2) -> DML3(x+2)
//	                       ▲
//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, DDL exists at x+2, cannot interrupt
//	                           Must process DML2, DML3, DDL together atomically
func (m *eventMerger) canInterrupt(newCommitTs uint64, currentBatchDML *event.BatchDMLEvent) bool {
	currentDMLCommitTs := uint64(0)
	if len(currentBatchDML.DMLEvents) > 0 {
		currentDMLCommitTs = currentBatchDML.GetCommitTs()
	}

	if currentDMLCommitTs != newCommitTs {
		return true
	}

	// Check if there are any DDL events at the lastCommitTs
	// If there are, we cannot interrupt to ensure they are processed together
	return !m.hasDDLAtCommitTs(newCommitTs)
}

// TxnEvent represents a transaction, it may generates one or multiple DMLEvents
type TxnEvent struct {
	BatchDML         *event.BatchDMLEvent
	CurrentDMLEvent  *event.DMLEvent
	DMLEventMaxRows  int32
	DMLEventMaxBytes int64
	shouldSplitTxn   bool
}

func newTxnEvent(
	batchDML *event.BatchDMLEvent,
	dispatcherID common.DispatcherID,
	tableID int64,
	tableInfo *common.TableInfo,
	startTs uint64,
	commitTs uint64,
	shouldSplitTxn bool,
) (*TxnEvent, error) {
	serverConfig := config.GetGlobalServerConfig()
	txn := &TxnEvent{
		BatchDML:         batchDML,
		CurrentDMLEvent:  event.NewDMLEvent(dispatcherID, tableID, startTs, commitTs, tableInfo),
		DMLEventMaxRows:  serverConfig.Debug.EventService.DMLEventMaxRows,
		DMLEventMaxBytes: serverConfig.Debug.EventService.DMLEventMaxBytes,
		shouldSplitTxn:   shouldSplitTxn,
	}
	return txn, txn.BatchDML.AppendDMLEvent(txn.CurrentDMLEvent)
}

func (t *TxnEvent) AppendRow(
	rawEvent *common.RawKVEntry,
	decode func(
		rawKv *common.RawKVEntry,
		tableInfo *common.TableInfo,
		chk *chunk.Chunk,
	) (int, *integrity.Checksum, error),
	filter filter.Filter,
) error {
	if t.shouldSplitTxn && (t.CurrentDMLEvent.Len() >= t.DMLEventMaxRows || t.CurrentDMLEvent.GetSize() >= t.DMLEventMaxBytes) {
		newDMLEvent := event.NewDMLEvent(
			t.CurrentDMLEvent.DispatcherID,
			t.CurrentDMLEvent.PhysicalTableID,
			t.CurrentDMLEvent.StartTs,
			t.CurrentDMLEvent.CommitTs,
			t.CurrentDMLEvent.TableInfo)
		t.CurrentDMLEvent = newDMLEvent
		err := t.BatchDML.AppendDMLEvent(newDMLEvent)
		if err != nil {
			return err
		}
	}
	return t.CurrentDMLEvent.AppendRow(rawEvent, decode, filter)
}

// dmlProcessor handles DML event processing and batching
type dmlProcessor struct {
	mounter      event.Mounter
	schemaGetter schemaGetter

	filter filter.Filter

	// insertRowCache is used to cache the split update event's insert part of the current transaction.
	// It will be used to append to the current DML event when the transaction is finished.
	// And it will be cleared when the transaction is finished.
	insertRowCache []*common.RawKVEntry

	// currentTxn is the transaction that is handling now
	currentTxn *TxnEvent

	batchDML             *event.BatchDMLEvent
	outputRawChangeEvent bool
}

// newDMLProcessor creates a new DML processor
func newDMLProcessor(
	mounter event.Mounter, schemaGetter schemaGetter,
	filter filter.Filter, outputRawChangeEvent bool,
) *dmlProcessor {
	return &dmlProcessor{
		mounter:              mounter,
		schemaGetter:         schemaGetter,
		filter:               filter,
		batchDML:             event.NewBatchDMLEvent(),
		insertRowCache:       make([]*common.RawKVEntry, 0),
		outputRawChangeEvent: outputRawChangeEvent,
	}
}

// startTxn should be called after flush the current transaction
func (p *dmlProcessor) startTxn(
	dispatcherID common.DispatcherID,
	tableID int64,
	tableInfo *common.TableInfo,
	startTs uint64,
	commitTs uint64,
	shouldSplitTxn bool,
) error {
	if p.currentTxn != nil {
		log.Panic("there is a transaction not flushed yet")
	}
	var err error
	p.currentTxn, err = newTxnEvent(p.batchDML, dispatcherID, tableID, tableInfo, startTs, commitTs, shouldSplitTxn)
	return err
}

func (p *dmlProcessor) commitTxn() error {
	if p.currentTxn != nil && len(p.insertRowCache) > 0 {
		for _, insertRow := range p.insertRowCache {
			if err := p.currentTxn.AppendRow(insertRow, p.mounter.DecodeToChunk, p.filter); err != nil {
				return err
			}
		}
		p.insertRowCache = make([]*common.RawKVEntry, 0)
	}
	p.currentTxn = nil
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
	if p.currentTxn == nil {
		log.Panic("no current DML event to append to")
	}

	rawEvent.Key = event.RemoveKeyspacePrefix(rawEvent.Key)

	if !rawEvent.IsUpdate() {
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter)
	}

	var (
		shouldSplit bool
		err         error
	)
	if !p.outputRawChangeEvent {
		shouldSplit, err = event.IsUKChanged(rawEvent, p.currentTxn.CurrentDMLEvent.TableInfo)
		if err != nil {
			return err
		}
	}

	if !shouldSplit {
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter)
	}

	log.Debug("split update event", zap.Uint64("startTs", rawEvent.StartTs),
		zap.Uint64("commitTs", rawEvent.CRTs),
		zap.String("table", p.currentTxn.CurrentDMLEvent.TableInfo.TableName.String()))
	deleteRow, insertRow, err := rawEvent.SplitUpdate()
	if err != nil {
		return err
	}
	p.insertRowCache = append(p.insertRowCache, insertRow)
	return p.currentTxn.AppendRow(deleteRow, p.mounter.DecodeToChunk, p.filter)
}

// getCurrentBatchDML returns the current batch DML event
func (p *dmlProcessor) getCurrentBatchDML() *event.BatchDMLEvent {
	return p.batchDML
}

// this should be called after the previous batchDML is flushed.
func (p *dmlProcessor) resetBatchDML() {
	p.batchDML = event.NewBatchDMLEvent()
}
