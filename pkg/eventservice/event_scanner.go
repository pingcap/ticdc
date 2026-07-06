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
	"io"
	"path/filepath"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/chunk"
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

type scanProgress struct {
	valid                bool
	txnCommitTs          uint64
	txnStartTs           uint64
	rowLevelScanPosition common.ScanPosition
}

func newTxnScanProgress(commitTs uint64, startTs uint64) scanProgress {
	return scanProgress{
		valid:       true,
		txnCommitTs: commitTs,
		txnStartTs:  startTs,
	}
}

func newRowLevelScanProgress(commitTs uint64, startTs uint64, position common.ScanPosition) scanProgress {
	progress := newTxnScanProgress(commitTs, startTs)
	if len(position) > 0 {
		progress.rowLevelScanPosition = make(common.ScanPosition, len(position))
		copy(progress.rowLevelScanPosition, position)
	}
	return progress
}

// newEventScanner creates a new EventScanner
func newEventScanner(
	eventStore eventGetter,
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
// - Maximum bytes processed (limit.MaxBytes) at a transaction boundary
// - Timeout duration (limit.Timeout) at a transaction boundary
// - Large transaction threshold inside the current transaction
//
// A transaction-boundary scan interruption is ONLY allowed when both conditions are met:
// 1. The current event's commit timestamp is greater than the lastCommitTs (a commit TS boundary is reached)
// 2. At least one DML event has been successfully scanned
//
// A current-transaction interruption is ONLY allowed when split transaction is enabled,
// the eventstore iterator provides a row-level scan position, and the current
// transaction fragment exceeds the large transaction threshold.
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
) (int64, []event.Event, scanProgress, bool, error) {
	// Initialize scan session
	sess := newSession(ctx, dispatcherStat, dataRange, limit)
	defer sess.recordMetrics()

	if state := dispatcherStat.getLargeTxnState(); state != nil &&
		state.getPhase() == largeTxnScanPhaseDrainInserts {
		interrupted, err := s.drainLargeTxnInserts(sess, state)
		if err != nil {
			_ = dispatcherStat.cleanupLargeTxnState()
		}
		return sess.eventBytes, sess.events, sess.progress, interrupted, err
	}

	// Fetch DDL events
	start := time.Now()
	events, err := s.fetchDDLEvents(dispatcherStat, dataRange)
	if err != nil {
		return 0, nil, scanProgress{}, false, err
	}
	metrics.EventServiceGetDDLEventDuration.Observe(time.Since(start).Seconds())

	iter, err := s.eventGetter.GetIterator(dispatcherStat.info.GetID(), dataRange)
	if err != nil {
		return 0, nil, scanProgress{}, false, err
	}
	if iter == nil {
		if state := dispatcherStat.getLargeTxnState(); state != nil &&
			state.getPhase() == largeTxnScanPhaseOriginal {
			dispatcherStat.markLargeTxnDrainInserts(state.startTs, state.commitTs, false, 0)
			sess.progress = newTxnScanProgress(state.commitTs, state.startTs)
			return 0, sess.events, sess.progress, true, nil
		}
		resolved := event.NewResolvedEvent(dataRange.CommitTsEnd, dispatcherStat.id, dispatcherStat.epoch)
		events = append(events, resolved)
		sess.appendEvents(events)
		sess.progress = newTxnScanProgress(dataRange.CommitTsEnd, 0)
		return 0, sess.events, sess.progress, false, nil
	}

	// Execute event scanning and merging
	merger := newEventMerger(events)
	interrupted, scanErr := s.scanAndMergeEvents(sess, merger, iter)
	closeErr := s.closeIterator(iter)
	if scanErr != nil {
		if closeErr != nil {
			log.Warn("event store iterator close returned error after scan error",
				zap.Stringer("dispatcherID", dispatcherStat.info.GetID()),
				zap.Error(closeErr))
		}
		_ = dispatcherStat.cleanupLargeTxnState()
		return sess.eventBytes, sess.events, sess.progress, interrupted, scanErr
	}
	if closeErr != nil {
		_ = dispatcherStat.cleanupLargeTxnState()
		return 0, nil, scanProgress{}, false, closeErr
	}
	return sess.eventBytes, sess.events, sess.progress, interrupted, nil
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
			zap.Int64("tableID", dataRange.Span.TableID), zap.Error(err), zap.Int64("mode", s.mode))
		return nil, err
	}

	result := make([]event.Event, 0, len(ddlEvents))
	for _, item := range ddlEvents {
		result = append(result, &item)
	}
	return result, nil
}

// closeIterator closes the event iterator and records metrics.
func (s *eventScanner) closeIterator(iter eventstore.EventIterator) error {
	if iter == nil {
		return nil
	}
	eventCount, err := iter.Close()
	if eventCount != 0 {
		updateMetricEventStoreOutputKv(s.mode, float64(eventCount))
	}
	return err
}

// scanAndMergeEvents performs the main scanning and merging logic
func (s *eventScanner) scanAndMergeEvents(
	session *session,
	merger *eventMerger,
	iter eventstore.EventIterator,
) (bool, error) {
	tableID := session.dataRange.Span.TableID
	dispatcher := session.dispatcherStat
	processor := newDMLProcessor(
		s.mounter,
		s.schemaGetter,
		dispatcher.filter,
		dispatcher.info.IsOutputRawChangeEvent(),
		s.mode,
		dispatcher.info.EnableIgnoreUpdateOnlyColumns())
	processor.dispatcherStat = dispatcher

	for {
		shouldStop, err := s.checkScanConditions(session)
		if err != nil {
			return false, err
		}
		if shouldStop {
			return false, nil
		}

		rawEvent, position, isNewTxn := nextEventWithScanPosition(iter)
		if rawEvent == nil {
			if state := dispatcher.getLargeTxnState(); processor.currentTxn == nil &&
				state != nil &&
				state.getPhase() == largeTxnScanPhaseOriginal {
				dispatcher.markLargeTxnDrainInserts(state.startTs, state.commitTs, false, 0)
				session.progress = newTxnScanProgress(state.commitTs, state.startTs)
				return true, nil
			}
			interrupted, err := s.finishCurrentTxn(
				session,
				merger,
				processor,
				0,
				0,
				false,
			)
			if err != nil || interrupted {
				return interrupted, err
			}
			err = finalizeScan(merger, processor, session, session.dataRange.CommitTsEnd)
			return false, err
		}

		if state := dispatcher.getLargeTxnState(); processor.currentTxn == nil &&
			state != nil &&
			state.getPhase() == largeTxnScanPhaseOriginal &&
			(rawEvent.StartTs != state.startTs || rawEvent.CRTs != state.commitTs) {
			dispatcher.markLargeTxnDrainInserts(state.startTs, state.commitTs, true, rawEvent.CRTs)
			session.progress = newTxnScanProgress(state.commitTs, state.startTs)
			return true, nil
		}

		if isNewTxn {
			tableInfo, err := s.getTableInfo4Txn(dispatcher, tableID, rawEvent.CRTs-1)
			if err != nil {
				return false, err
			}
			interrupted, err := s.finishCurrentTxn(
				session,
				merger,
				processor,
				rawEvent.CRTs,
				getTableInfoUpdateTs(tableInfo),
				tableInfo == nil,
			)
			if err != nil || interrupted {
				return interrupted, err
			}
			// The table has been deleted, so the current raw event cannot be
			// decoded as DML. Resolve to its commit ts to skip it; resolving to
			// rawEvent.CRTs-1 can equal the scan start and cause a no-progress loop.
			if tableInfo == nil {
				err = finalizeScan(merger, processor, session, rawEvent.CRTs)
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

		session.observeRawEntry(rawEvent, position)
		if err = processor.appendRow(rawEvent); err != nil {
			log.Error("append row failed", zap.Error(err),
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Int64("tableID", tableID),
				zap.Uint64("startTs", rawEvent.StartTs),
				zap.Uint64("commitTs", rawEvent.CRTs),
				zap.Int64("mode", s.mode))
			return false, err
		}
		if s.canInterruptCurrentTxn(session, merger, processor, rawEvent, position) {
			interruptCurrentTxn(session, merger, processor, rawEvent.CRTs, rawEvent.StartTs, position)
			return true, nil
		}
	}
}

func nextEventWithScanPosition(
	iter eventstore.EventIterator,
) (*common.RawKVEntry, common.ScanPosition, bool) {
	if positionIter, ok := iter.(eventstore.EventIteratorWithScanPosition); ok {
		return positionIter.NextWithScanPosition()
	}
	rawEvent, isNewTxn := iter.Next()
	return rawEvent, nil, isNewTxn
}

func getTableInfoUpdateTs(tableInfo *common.TableInfo) uint64 {
	if tableInfo == nil {
		return 0
	}
	return tableInfo.GetUpdateTS()
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
			zap.Uint64("ts", ts), zap.Error(err), zap.Int64("mode", s.mode))
		return nil, nil
	}

	if errors.Is(err, &schemastore.TableDeletedError{}) {
		log.Warn("get table info failed, since the table is deleted",
			zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
			zap.Uint64("ts", ts), zap.Int64("mode", s.mode))
		return nil, nil
	}

	log.Error("get table info failed, unknown reason",
		zap.Stringer("dispatcherID", dispatcher.id), zap.Int64("tableID", tableID),
		zap.Uint64("ts", ts), zap.Error(err), zap.Int64("mode", s.mode))
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

func (s *eventScanner) finishCurrentTxn(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	nextCommitTs uint64,
	nextTableInfoUpdateTs uint64,
	nextTableDeleted bool,
) (bool, error) {
	if processor.currentTxn == nil {
		return false, nil
	}
	currentStartTs := processor.currentTxn.CurrentDMLEvent.GetStartTs()
	currentCommitTs := processor.currentTxn.CurrentDMLEvent.GetCommitTs()

	if processor.hasSpilledInsertsForCurrentTxn() && merger.hasDDLAtCommitTs(currentCommitTs) {
		if err := processor.flushCachedInsertRows(); err != nil {
			return false, err
		}
		if err := processor.flushSpilledInsertsIntoCurrentTxn(); err != nil {
			return false, err
		}
	}

	if err := s.commitTxn(session, merger, processor, nextCommitTs, nextTableInfoUpdateTs); err != nil {
		return false, err
	}
	if !processor.hasLargeTxnState(currentStartTs, currentCommitTs) {
		return false, nil
	}

	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	session.appendEvents(events)
	processor.resetBatchDML()

	hasFollowingTxn := nextCommitTs != 0 && !nextTableDeleted
	session.dispatcherStat.markLargeTxnDrainInserts(
		currentStartTs,
		currentCommitTs,
		hasFollowingTxn,
		nextCommitTs,
	)
	if len(session.lastRowPosition) > 0 {
		session.progress = newRowLevelScanProgress(currentCommitTs, currentStartTs, session.lastRowPosition)
	} else {
		session.progress = newTxnScanProgress(currentCommitTs, currentStartTs)
	}
	return true, nil
}

func (s *eventScanner) canInterruptCurrentTxn(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	rawEvent *common.RawKVEntry,
	position common.ScanPosition,
) bool {
	if len(position) == 0 || !session.dispatcherStat.txnAtomicity.ShouldSplitTxn() {
		return false
	}
	if len(processor.insertRowCache) > 0 {
		return false
	}
	if processor.currentTxn == nil || !processor.currentTxn.exceedsLargeTxnThreshold() {
		return false
	}
	if !merger.canInterrupt(rawEvent.CRTs, processor.batchDML) {
		return false
	}
	return true
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
	sess.progress = newTxnScanProgress(endTs, 0)
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

func interruptCurrentTxn(
	session *session,
	merger *eventMerger,
	processor *dmlProcessor,
	commitTs uint64,
	startTs uint64,
	position common.ScanPosition,
) {
	events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	session.appendEvents(events)
	session.progress = newRowLevelScanProgress(commitTs, startTs, position)
	log.Debug("scan interrupted inside a large txn",
		zap.Stringer("dispatcherID", session.dispatcherStat.id),
		zap.Uint64("startTs", startTs),
		zap.Uint64("commitTs", commitTs),
		zap.Int("scannedEntryCount", session.scannedEntryCount),
		zap.Duration("duration", time.Since(session.startTime)))
}

func (s *eventScanner) drainLargeTxnInserts(
	session *session,
	state *largeTxnScanState,
) (bool, error) {
	processor := newDMLProcessor(
		s.mounter,
		s.schemaGetter,
		session.dispatcherStat.filter,
		session.dispatcherStat.info.IsOutputRawChangeEvent(),
		s.mode,
		session.dispatcherStat.info.EnableIgnoreUpdateOnlyColumns())
	processor.dispatcherStat = session.dispatcherStat

	for {
		shouldStop, err := s.checkScanConditions(session)
		if err != nil {
			return false, err
		}
		if shouldStop {
			return false, nil
		}

		entry, err := state.nextInsert()
		if err != nil {
			if session.dispatcherStat.isRemoved.Load() {
				return false, nil
			}
			if errors.Is(err, io.EOF) {
				if processor.currentTxn != nil {
					if err := processor.commitTxn(); err != nil {
						return false, err
					}
					if processor.getCurrentBatchDML().DMLCount() != 0 {
						session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
					}
				}
				session.progress = newTxnScanProgress(state.commitTs, state.startTs)
				hasFollowingTxn, followingCommitTs := state.snapshotDrainInfo()
				shouldResolveCommitTs := (!hasFollowingTxn || followingCommitTs > state.commitTs) &&
					session.dataRange.CommitTsEnd == state.commitTs
				if err := session.dispatcherStat.cleanupLargeTxnState(); err != nil {
					return false, err
				}
				if shouldResolveCommitTs {
					resolved := event.NewResolvedEvent(state.commitTs, session.dispatcherStat.id, session.dispatcherStat.epoch)
					session.appendEvents([]event.Event{resolved})
					session.progress = newTxnScanProgress(state.commitTs, 0)
					return false, nil
				}
				return true, nil
			}
			return false, err
		}

		if processor.currentTxn == nil {
			if err := processor.startTxn(
				session.dispatcherStat.id,
				state.tableID,
				state.tableInfo,
				state.startTs,
				state.commitTs,
				true,
			); err != nil {
				return false, err
			}
		}
		session.observeRawEntry(entry, nil)
		if err := processor.appendInsertRow(entry); err != nil {
			return false, err
		}
		if session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) {
			if err := processor.commitTxn(); err != nil {
				return false, err
			}
			session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
			session.progress = newTxnScanProgress(state.commitTs, state.startTs)
			return true, nil
		}
	}
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
	lastRowPosition   common.ScanPosition
	// dmlCount is the count of transactions.
	dmlCount int

	// Result collection, including DDL, BatchedDML, ResolvedTs events in the timestamp order.
	events     []event.Event
	eventBytes int64
	progress   scanProgress
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
func (s *session) observeRawEntry(entry *common.RawKVEntry, position common.ScanPosition) {
	s.scannedBytes += entry.GetSize()
	s.scannedEntryCount++
	if len(position) == 0 {
		s.lastRowPosition = nil
		return
	}
	s.lastRowPosition = make(common.ScanPosition, len(position))
	copy(s.lastRowPosition, position)
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
			s.eventBytes++
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
	BatchDML                 *event.BatchDMLEvent
	CurrentDMLEvent          *event.DMLEvent
	DMLEventMaxRows          int32
	DMLEventMaxBytes         int64
	rawKVBytes               int64
	largeTxnThresholdInBytes int64
	shouldSplitTxn           bool
}

const largeTxnInsertSpillDirName = "eventservice"

func getLargeTxnInsertSpillDir() string {
	return filepath.Join(config.GetGlobalServerConfig().DataDir, largeTxnInsertSpillDirName)
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
		BatchDML:                 batchDML,
		CurrentDMLEvent:          event.NewDMLEvent(dispatcherID, tableID, startTs, commitTs, tableInfo),
		DMLEventMaxRows:          serverConfig.Debug.EventService.DMLEventMaxRows,
		DMLEventMaxBytes:         serverConfig.Debug.EventService.DMLEventMaxBytes,
		largeTxnThresholdInBytes: serverConfig.Debug.EventService.LargeTxnThresholdInBytes,
		shouldSplitTxn:           shouldSplitTxn,
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
	filterContext filter.DMLFilterContext,
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
	return t.CurrentDMLEvent.AppendRow(rawEvent, decode, filter, filterContext)
}

func (t *TxnEvent) observeRawKVBytes(rawEvent *common.RawKVEntry) {
	t.rawKVBytes += rawEvent.GetSize()
}

func (t *TxnEvent) exceedsLargeTxnThreshold() bool {
	return t.shouldSplitTxn && t.rawKVBytes > t.largeTxnThresholdInBytes
}

// dmlTypeFilterCacheSize follows common.RowType iota values: delete, insert, update.
const dmlTypeFilterCacheSize = int(common.RowTypeUpdate) + 1

// dmlProcessor handles DML event processing and batching
type dmlProcessor struct {
	mounter      event.Mounter
	schemaGetter schemaGetter

	filter         filter.Filter
	filterContext  filter.DMLFilterContext
	dispatcherStat *dispatcherStat
	spillDir       string

	// dmlTypeFilterCache caches the pre-decode filter result within the current transaction.
	// The cache is reset when a new transaction starts. It is safe because tableInfo
	// and startTs are fixed for the current transaction.
	dmlTypeFilterCache [dmlTypeFilterCacheSize]struct {
		valid  bool
		ignore bool
	}

	// insertRowCache is used to cache the split update event's insert part of the current transaction.
	// It will be used to append to the current DML event when the transaction is finished.
	// And it will be cleared when the transaction is finished.
	insertRowCache []*common.RawKVEntry

	// currentTxn is the transaction that is handling now
	currentTxn *TxnEvent

	batchDML             *event.BatchDMLEvent
	outputRawChangeEvent bool
	mode                 int64
}

// newDMLProcessor creates a new DML processor
func newDMLProcessor(
	mounter event.Mounter, schemaGetter schemaGetter,
	dmlFilter filter.Filter, outputRawChangeEvent bool, mode int64,
	enableIgnoreUpdateOnlyColumns bool,
) *dmlProcessor {
	filterContext := filter.DMLFilterContext{}
	if enableIgnoreUpdateOnlyColumns {
		filterContext.EnableIgnoreUpdateOnlyColumns = true
	}
	return &dmlProcessor{
		mounter:              mounter,
		schemaGetter:         schemaGetter,
		filter:               dmlFilter,
		filterContext:        filterContext,
		batchDML:             event.NewBatchDMLEvent(),
		insertRowCache:       make([]*common.RawKVEntry, 0),
		spillDir:             getLargeTxnInsertSpillDir(),
		outputRawChangeEvent: outputRawChangeEvent,
		mode:                 mode,
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
	p.resetDMLTypeFilterCache()
	var err error
	p.currentTxn, err = newTxnEvent(p.batchDML, dispatcherID, tableID, tableInfo, startTs, commitTs, shouldSplitTxn)
	return err
}

func (p *dmlProcessor) commitTxn() error {
	if err := p.flushCachedInsertRows(); err != nil {
		return err
	}
	p.currentTxn = nil
	return nil
}

func (p *dmlProcessor) flushCachedInsertRows() error {
	if p.currentTxn == nil || len(p.insertRowCache) == 0 {
		return nil
	}
	for _, insertRow := range p.insertRowCache {
		if err := p.currentTxn.AppendRow(insertRow, p.mounter.DecodeToChunk, p.filter, p.filterContext); err != nil {
			return err
		}
	}
	p.insertRowCache = make([]*common.RawKVEntry, 0)
	return nil
}

func (p *dmlProcessor) spillCachedInsertRows() error {
	if len(p.insertRowCache) == 0 {
		return nil
	}
	state, err := p.getOrCreateLargeTxnState()
	if err != nil {
		return err
	}
	for _, insertRow := range p.insertRowCache {
		if err := state.appendInsert(insertRow); err != nil {
			return err
		}
	}
	p.insertRowCache = make([]*common.RawKVEntry, 0)
	return nil
}

func (p *dmlProcessor) shouldSpillSplitUpdateInsert() bool {
	if p.currentTxn == nil {
		return false
	}
	return p.currentTxn.exceedsLargeTxnThreshold() || p.hasSpilledInsertsForCurrentTxn()
}

func (p *dmlProcessor) hasSpilledInsertsForCurrentTxn() bool {
	if p.currentTxn == nil || p.dispatcherStat == nil {
		return false
	}
	current := p.currentTxn.CurrentDMLEvent
	return p.hasLargeTxnState(current.GetStartTs(), current.GetCommitTs())
}

func (p *dmlProcessor) hasLargeTxnState(startTs uint64, commitTs uint64) bool {
	if p.dispatcherStat == nil {
		return false
	}
	state := p.dispatcherStat.getLargeTxnState()
	return state != nil &&
		state.startTs == startTs &&
		state.commitTs == commitTs &&
		state.getPhase() == largeTxnScanPhaseOriginal
}

func (p *dmlProcessor) flushSpilledInsertsIntoCurrentTxn() error {
	if p.currentTxn == nil || p.dispatcherStat == nil {
		return nil
	}
	state := p.dispatcherStat.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return nil
	}
	for {
		insertRow, err := state.nextInsert()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return p.dispatcherStat.cleanupLargeTxnState()
			}
			return err
		}
		if err := p.appendInsertRow(insertRow); err != nil {
			return err
		}
	}
}

func (p *dmlProcessor) getOrCreateLargeTxnState() (*largeTxnScanState, error) {
	if p.dispatcherStat == nil {
		return nil, errors.New("dispatcher stat is required for large txn update spill")
	}
	currentDML := p.currentTxn.CurrentDMLEvent
	return p.dispatcherStat.getOrCreateLargeTxnState(
		p.spillDir,
		currentDML.GetTableID(),
		currentDML.TableInfo,
		currentDML.GetStartTs(),
		currentDML.GetCommitTs(),
	)
}

func (p *dmlProcessor) appendInsertRow(rawEvent *common.RawKVEntry) error {
	if p.currentTxn == nil {
		log.Panic("no current DML event to append to")
	}
	rawEvent.Key = event.RemoveKeyspacePrefix(rawEvent.Key)
	updateMetricEventServiceSendDMLTypeCount(p.mode, rawEvent.GetType(), false)
	return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
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
	p.currentTxn.observeRawKVBytes(rawEvent)
	if p.shouldSpillSplitUpdateInsert() {
		if err := p.spillCachedInsertRows(); err != nil {
			return err
		}
	}

	rawType := rawEvent.GetType()
	if !rawEvent.IsUpdate() {
		updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, false)
		ignore, err := p.shouldIgnoreRawEventByDMLType(rawEvent)
		if err != nil {
			return err
		}
		if ignore {
			return nil
		}
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
	}

	var (
		shouldSplit bool
		err         error
	)
	ignore, err := p.shouldIgnoreDMLByEventType(common.RowTypeUpdate, rawEvent.StartTs)
	if err != nil {
		return err
	}
	if ignore {
		updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, false)
		return nil
	}

	if !p.outputRawChangeEvent {
		shouldSplit, err = event.IsUKChanged(rawEvent, p.currentTxn.CurrentDMLEvent.TableInfo)
		if err != nil {
			return err
		}
	}

	updateMetricEventServiceSendDMLTypeCount(p.mode, rawType, shouldSplit)

	if !shouldSplit {
		return p.currentTxn.AppendRow(rawEvent, p.mounter.DecodeToChunk, p.filter, p.filterContext)
	}

	log.Debug("split update event", zap.Uint64("startTs", rawEvent.StartTs),
		zap.Uint64("commitTs", rawEvent.CRTs),
		zap.String("table", p.currentTxn.CurrentDMLEvent.TableInfo.TableName.String()))
	deleteRow, insertRow, err := rawEvent.SplitUpdate()
	if err != nil {
		return err
	}
	ignoreInsert, err := p.shouldIgnoreRawEventByDMLType(insertRow)
	if err != nil {
		return err
	}
	if !ignoreInsert {
		if p.shouldSpillSplitUpdateInsert() {
			state, err := p.getOrCreateLargeTxnState()
			if err != nil {
				return err
			}
			if err := state.appendInsert(insertRow); err != nil {
				return err
			}
		} else {
			p.insertRowCache = append(p.insertRowCache, insertRow)
		}
	}
	ignoreDelete, err := p.shouldIgnoreRawEventByDMLType(deleteRow)
	if err != nil {
		return err
	}
	if ignoreDelete {
		return nil
	}
	return p.currentTxn.AppendRow(deleteRow, p.mounter.DecodeToChunk, p.filter, p.filterContext)
}

func (p *dmlProcessor) shouldIgnoreRawEventByDMLType(rawEvent *common.RawKVEntry) (bool, error) {
	rowType := common.RowTypeInsert
	if rawEvent.IsDelete() {
		rowType = common.RowTypeDelete
	} else if rawEvent.IsUpdate() {
		rowType = common.RowTypeUpdate
	}
	return p.shouldIgnoreDMLByEventType(rowType, rawEvent.StartTs)
}

func (p *dmlProcessor) shouldIgnoreDMLByEventType(rowType common.RowType, startTs uint64) (bool, error) {
	idx := int(rowType)
	if idx >= 0 && idx < len(p.dmlTypeFilterCache) {
		if p.dmlTypeFilterCache[idx].valid {
			return p.dmlTypeFilterCache[idx].ignore, nil
		}
	}

	if p.filter == nil {
		p.setDMLTypeFilterCache(rowType, false)
		return false, nil
	}
	ignore, err := p.filter.ShouldIgnoreDMLByEventType(
		rowType,
		p.currentTxn.CurrentDMLEvent.TableInfo,
		startTs,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	p.setDMLTypeFilterCache(rowType, ignore)
	return ignore, nil
}

func (p *dmlProcessor) setDMLTypeFilterCache(rowType common.RowType, ignore bool) {
	idx := int(rowType)
	if idx < 0 || idx >= len(p.dmlTypeFilterCache) {
		return
	}
	p.dmlTypeFilterCache[idx].valid = true
	p.dmlTypeFilterCache[idx].ignore = ignore
}

func (p *dmlProcessor) resetDMLTypeFilterCache() {
	for i := range p.dmlTypeFilterCache {
		p.dmlTypeFilterCache[i].valid = false
		p.dmlTypeFilterCache[i].ignore = false
	}
}

// getCurrentBatchDML returns the current batch DML event
func (p *dmlProcessor) getCurrentBatchDML() *event.BatchDMLEvent {
	return p.batchDML
}

// this should be called after the previous batchDML is flushed.
func (p *dmlProcessor) resetBatchDML() {
	p.batchDML = event.NewBatchDMLEvent()
}
