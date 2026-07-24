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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import (
	"io"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// txnScanContext groups the per-attempt objects needed by transaction strategy
// hooks. The scanner owns iteration and DDL/DML ordering; a strategy only
// decides where a transaction may stop and how pending transaction work resumes.
//
// For example, transaction (startTs=10, commitTs=100) contains rows r1, r2,
// and r3. The atomic strategy emits all three rows before it may stop. The split
// strategy may emit r1, publish progress (100, 10, P1), and resume r2 after
// EventStore position P1 in the next attempt. If a row is an update that changes
// a unique key, its delete half stays in the original scan while its insert half
// is spilled and drained only after all original rows, avoiding a downstream
// unique-key conflict across fragments.
//
// See docs/design/2026-07-22-eventservice-scan-progress-and-txn-strategy.md for
// the complete cursor and state-transition model.
type txnScanContext struct {
	scanner   *eventScanner
	session   *session
	merger    *eventMerger
	processor *dmlProcessor
}

// nextTxnMeta identifies the next iterator record that the scanner is about to
// process. A zero commitTs means iterator EOF. Split mode also uses this value
// when currentTxn is nil to decide whether a transaction retained from a prior
// row-level interruption has reached the end of its original EventStore rows.
type nextTxnMeta struct {
	startTs           uint64
	commitTs          uint64
	tableInfoUpdateTs uint64
	tableDeleted      bool
}

// txnScanStrategy owns behavior that differs between atomic and split
// transaction scanning while the event scanner retains the common iterator and
// DDL/DML merge loop.
type txnScanStrategy interface {
	// resumePending runs before DDL lookup and EventStore iterator creation.
	// handled means the pending-state path consumed this attempt completely;
	// interrupted asks the broker to schedule another attempt immediately.
	resumePending(ctx *txnScanContext) (handled bool, interrupted bool, err error)
	// startTxn creates the mode-specific TxnEvent after common boundary and
	// schema-version handling has selected the next transaction.
	startTxn(
		ctx *txnScanContext,
		startTs uint64,
		commitTs uint64,
		tableInfo *common.TableInfo,
		tableID int64,
	) error
	// finishTxn runs at a transaction boundary or iterator EOF. In split mode it
	// may also close an original phase retained from a previous scan even when
	// this attempt has not created currentTxn yet.
	finishTxn(ctx *txnScanContext, next nextTxnMeta) (interrupted bool, err error)
	// afterAppend is the only hook that may interrupt inside a transaction. A
	// non-empty position is the exact cursor after rawEvent.
	afterAppend(
		ctx *txnScanContext,
		rawEvent *common.RawKVEntry,
		position eventstore.ScanPosition,
	) (interrupted bool, err error)
}

// newTxnScanStrategy selects split behavior only when the dispatcher's
// transaction atomicity configuration explicitly permits cross-scan fragments.
func newTxnScanStrategy(shouldSplitTxn bool) txnScanStrategy {
	if shouldSplitTxn {
		return splitTxnScanStrategy{}
	}
	return atomicTxnScanStrategy{}
}

// atomicTxnScanStrategy preserves table-level transaction atomicity by making
// both pending-resume and per-row interruption no-ops.
type atomicTxnScanStrategy struct{}

func (atomicTxnScanStrategy) resumePending(
	_ *txnScanContext,
) (bool, bool, error) {
	return false, false, nil
}

func (atomicTxnScanStrategy) startTxn(
	ctx *txnScanContext,
	startTs uint64,
	commitTs uint64,
	tableInfo *common.TableInfo,
	tableID int64,
) error {
	return startTxn(ctx, startTs, commitTs, tableInfo, tableID, false)
}

func (atomicTxnScanStrategy) finishTxn(
	ctx *txnScanContext,
	next nextTxnMeta,
) (bool, error) {
	if ctx.processor == nil || ctx.processor.currentTxn == nil {
		return false, nil
	}
	return false, ctx.scanner.commitTxn(
		ctx.session,
		ctx.merger,
		ctx.processor,
		next.commitTs,
		next.tableInfoUpdateTs,
	)
}

func (atomicTxnScanStrategy) afterAppend(
	_ *txnScanContext,
	_ *common.RawKVEntry,
	_ eventstore.ScanPosition,
) (bool, error) {
	return false, nil
}

// splitTxnScanStrategy permits row-level interruption and owns the two-phase
// original-rows/spilled-inserts lifecycle for large transactions.
type splitTxnScanStrategy struct{}

func (splitTxnScanStrategy) resumePending(
	ctx *txnScanContext,
) (bool, bool, error) {
	state := ctx.session.dispatcherStat.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseDrainInserts {
		return false, false, nil
	}
	interrupted, err := drainLargeTxnInserts(ctx, state)
	return true, interrupted, err
}

func (splitTxnScanStrategy) startTxn(
	ctx *txnScanContext,
	startTs uint64,
	commitTs uint64,
	tableInfo *common.TableInfo,
	tableID int64,
) error {
	return startTxn(ctx, startTs, commitTs, tableInfo, tableID, true)
}

func (splitTxnScanStrategy) finishTxn(
	ctx *txnScanContext,
	next nextTxnMeta,
) (bool, error) {
	if ctx.processor == nil || ctx.processor.currentTxn == nil {
		return finishPendingSplitTxn(ctx.session, next), nil
	}
	return finishCurrentSplitTxn(ctx, next)
}

// finishPendingSplitTxn handles the boundary discovered after row-level resume.
// If the next row still belongs to the retained (startTs, commitTs), the
// original phase continues. A different transaction or EOF proves that all
// original rows were read, so progress becomes (C, S, nil) and the next scan
// starts draining delayed inserts before it may pass this transaction.
func finishPendingSplitTxn(session *session, next nextTxnMeta) bool {
	dispatcher := session.dispatcherStat
	state := dispatcher.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return false
	}
	if next.commitTs != 0 && next.startTs == state.startTs && next.commitTs == state.commitTs {
		return false
	}

	dispatcher.bigTxnMetrics.flush()
	dispatcher.markLargeTxnDrainInserts(state.startTs, state.commitTs, next.commitTs != 0, next.commitTs)
	session.progress = newTxnScanProgress(state.commitTs, state.startTs)
	return true
}

func (splitTxnScanStrategy) afterAppend(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	position eventstore.ScanPosition,
) (bool, error) {
	if !canInterruptCurrentTxn(ctx, rawEvent, position) {
		return false, nil
	}
	interruptCurrentTxn(ctx, rawEvent.CRTs, rawEvent.StartTs, position)
	return true, nil
}

// startTxn contains setup shared by both strategies; shouldSplitTxn controls
// whether the resulting TxnEvent may be emitted as cross-scan fragments.
func startTxn(
	ctx *txnScanContext,
	startTs uint64,
	commitTs uint64,
	tableInfo *common.TableInfo,
	tableID int64,
	shouldSplitTxn bool,
) error {
	err := ctx.processor.startTxn(
		ctx.session.dispatcherStat.id,
		tableID,
		tableInfo,
		startTs,
		commitTs,
		shouldSplitTxn,
	)
	if err != nil {
		return err
	}
	ctx.session.dmlCount++
	return nil
}

// finishCurrentSplitTxn commits the current fragment at a transaction boundary.
// A transaction with spilled inserts moves from the original phase to the drain
// phase and deliberately keeps progress at (C, S). If a DDL exists at the same
// commit-ts, the inserts are merged back into the current batch instead so the
// merger can preserve the required DML(C) -> DDL(C) order.
func finishCurrentSplitTxn(ctx *txnScanContext, next nextTxnMeta) (bool, error) {
	processor := ctx.processor
	currentStartTs := processor.currentTxn.CurrentDMLEvent.GetStartTs()
	currentCommitTs := processor.currentTxn.CurrentDMLEvent.GetCommitTs()

	if processor.hasSpilledInsertsForCurrentTxn() && ctx.merger.hasDDLAtCommitTs(currentCommitTs) {
		if err := processor.flushCachedInsertRows(); err != nil {
			return false, err
		}
		if err := processor.flushSpilledInsertsIntoCurrentTxn(); err != nil {
			return false, err
		}
	}

	if err := ctx.scanner.commitTxn(
		ctx.session,
		ctx.merger,
		processor,
		next.commitTs,
		next.tableInfoUpdateTs,
	); err != nil {
		return false, err
	}
	if !processor.hasLargeTxnState(currentStartTs, currentCommitTs) {
		return false, nil
	}

	events := ctx.merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	ctx.session.appendEvents(events)
	processor.resetBatchDML()

	hasFollowingTxn := next.commitTs != 0 && !next.tableDeleted
	ctx.session.dispatcherStat.markLargeTxnDrainInserts(
		currentStartTs,
		currentCommitTs,
		hasFollowingTxn,
		next.commitTs,
	)
	if len(ctx.session.lastRowPosition) > 0 {
		ctx.session.progress = newRowLevelScanProgress(
			currentCommitTs,
			currentStartTs,
			ctx.session.lastRowPosition,
		)
	} else {
		ctx.session.progress = newTxnScanProgress(currentCommitTs, currentStartTs)
	}
	return true, nil
}

// canInterruptCurrentTxn requires an exact EventStore row position, a fragment
// above the large-transaction threshold, and a DML/DDL merge boundary that is
// safe to split. Cached UK-update inserts must be spilled first so no insert
// half is lost when the in-memory processor is discarded after interruption.
func canInterruptCurrentTxn(
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	position eventstore.ScanPosition,
) bool {
	if len(position) == 0 || len(ctx.processor.insertRowCache) > 0 {
		return false
	}
	if ctx.processor.currentTxn == nil || !ctx.processor.currentTxn.exceedsLargeTxnThreshold() {
		return false
	}
	return ctx.merger.canInterrupt(rawEvent.CRTs, ctx.processor.batchDML)
}

// interruptCurrentTxn emits the current fragment and publishes (C, S, P).
// That row-level progress must later overwrite the transaction-level progress
// observed by the broker send path, or EventStore would skip the remainder of
// this transaction on the next scan.
func interruptCurrentTxn(
	ctx *txnScanContext,
	commitTs uint64,
	startTs uint64,
	position eventstore.ScanPosition,
) {
	processor := ctx.processor
	if processor.currentTxn != nil {
		currentTxn := processor.currentTxn
		currentDML := currentTxn.CurrentDMLEvent
		ctx.session.dispatcherStat.bigTxnMetrics.addFragment(
			currentDML.GetStartTs(),
			currentDML.GetCommitTs(),
			currentTxn.rawKVBytes,
			currentTxn.largeTxnThresholdInBytes)
	}
	events := ctx.merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())
	ctx.session.appendEvents(events)
	ctx.session.progress = newRowLevelScanProgress(commitTs, startTs, position)
	log.Debug("scan interrupted inside a large txn",
		zap.Stringer("dispatcherID", ctx.session.dispatcherStat.id),
		zap.Uint64("startTs", startTs),
		zap.Uint64("commitTs", commitTs),
		zap.Int("scannedEntryCount", ctx.session.scannedEntryCount),
		zap.Duration("duration", time.Since(ctx.session.startTime)))
}

// drainLargeTxnInserts serves a pending drain directly from spill storage; it
// does not create an EventStore iterator. EventStore progress remains (C, S,
// nil) while state.drainedInsertCount tracks the independent position inside
// the spill file. On a failed attempt the reader rolls back to the last
// committed drain count, and on EOF the spill state is cleaned before scanning
// can move beyond the transaction.
func drainLargeTxnInserts(
	ctx *txnScanContext,
	state *largeTxnScanState,
) (bool, error) {
	session := ctx.session
	drainedInsertCount := 0
	returnWithError := func(scanErr error) (bool, error) {
		if rollbackErr := state.rollbackDrain(); rollbackErr != nil {
			log.Warn("reset large transaction spill reader failed",
				zap.Stringer("dispatcherID", session.dispatcherStat.id),
				zap.Error(rollbackErr))
		}
		return false, scanErr
	}

	processor := newDMLProcessor(
		ctx.scanner.mounter,
		ctx.scanner.schemaGetter,
		session.dispatcherStat.filter,
		session.dispatcherStat.info.IsOutputRawChangeEvent(),
		ctx.scanner.mode,
		session.dispatcherStat.info.EnableIgnoreUpdateOnlyColumns())
	processor.ctx = session.ctx
	processor.dispatcherStat = session.dispatcherStat

	for {
		shouldStop, err := ctx.scanner.checkScanConditions(session)
		if err != nil {
			return returnWithError(err)
		}
		if shouldStop {
			return false, nil
		}

		entry, err := state.nextInsert(session.ctx)
		if err != nil {
			if session.dispatcherStat.isRemoved.Load() {
				return false, nil
			}
			if errors.Is(err, io.EOF) {
				interrupted, finishErr := completeLargeTxnInsertDrain(
					session, state, processor, drainedInsertCount)
				if finishErr != nil {
					return returnWithError(finishErr)
				}
				return interrupted, nil
			}
			return returnWithError(err)
		}
		drainedInsertCount++

		if err := appendDrainedInsert(session, state, processor, entry); err != nil {
			return returnWithError(err)
		}
		if !session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) {
			continue
		}
		if err := flushLargeTxnInsertBatch(
			session, state, processor, drainedInsertCount); err != nil {
			return returnWithError(err)
		}
		return true, nil
	}
}

// appendDrainedInsert lazily recreates the original transaction in the current
// scan attempt, then appends one insert read from spill storage.
func appendDrainedInsert(
	session *session,
	state *largeTxnScanState,
	processor *dmlProcessor,
	entry *common.RawKVEntry,
) error {
	if processor.currentTxn == nil {
		if err := processor.startTxn(
			session.dispatcherStat.id,
			state.tableID,
			state.tableInfo,
			state.startTs,
			state.commitTs,
			true,
		); err != nil {
			return err
		}
	}
	session.observeRawEntry(entry, nil)
	return processor.appendInsertRow(entry)
}

// flushLargeTxnInsertBatch publishes a size-limited drain fragment and
// advances the spill retry boundary only after that fragment is complete.
func flushLargeTxnInsertBatch(
	session *session,
	state *largeTxnScanState,
	processor *dmlProcessor,
	drainedInsertCount int,
) error {
	if err := processor.commitTxn(); err != nil {
		return err
	}
	session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
	state.commitDrainedInserts(drainedInsertCount)
	session.progress = newTxnScanProgress(state.commitTs, state.startTs)
	return nil
}

// completeLargeTxnInsertDrain handles spill EOF. It returns interrupted=true
// when normal EventStore/DDL scanning still needs another attempt.
func completeLargeTxnInsertDrain(
	session *session,
	state *largeTxnScanState,
	processor *dmlProcessor,
	drainedInsertCount int,
) (bool, error) {
	if processor.currentTxn != nil {
		if err := processor.commitTxn(); err != nil {
			return false, err
		}
		if processor.getCurrentBatchDML().DMLCount() != 0 {
			session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
		}
	}

	state.commitDrainedInserts(drainedInsertCount)
	session.progress = newTxnScanProgress(state.commitTs, state.startTs)

	hasFollowingTxn, followingCommitTs := state.snapshotDrainInfo()
	noFollowingTxnAtCommitTs := !hasFollowingTxn || followingCommitTs > state.commitTs
	rangeEndsAtTxnCommitTs := session.dataRange.CommitTsEnd == state.commitTs
	shouldResolveCommitTs := noFollowingTxnAtCommitTs && rangeEndsAtTxnCommitTs

	if err := session.dispatcherStat.cleanupLargeTxnState(); err != nil {
		log.Warn("cleanup drained large transaction spill failed",
			zap.Stringer("dispatcherID", session.dispatcherStat.id),
			zap.Error(err))
	}
	if !shouldResolveCommitTs {
		return true, nil
	}

	resolved := event.NewResolvedEvent(
		state.commitTs,
		session.dispatcherStat.id,
		session.dispatcherStat.epoch,
	)
	session.appendEvents([]event.Event{resolved})
	session.progress = newTxnScanProgress(state.commitTs, 0)
	return false, nil
}

// spillCachedInsertRows persists insert halves collected before a split update
// crossed the large-transaction threshold. They must leave the in-memory cache
// before a row-level interruption discards the current processor.
func (p *dmlProcessor) spillCachedInsertRows() error {
	if len(p.insertRowCache) == 0 {
		return nil
	}
	state, err := p.getOrCreateLargeTxnState()
	if err != nil {
		return err
	}
	for _, insertRow := range p.insertRowCache {
		if err := state.appendInsert(p.ctx, insertRow); err != nil {
			return err
		}
	}
	p.insertRowCache = make([]*common.RawKVEntry, 0)
	return nil
}

// shouldSpillSplitUpdateInsert keeps spilling once a transaction has entered
// the spill lifecycle, even if a later in-memory fragment is below the size
// threshold.
func (p *dmlProcessor) shouldSpillSplitUpdateInsert() bool {
	if p.currentTxn == nil {
		return false
	}
	return p.currentTxn.exceedsLargeTxnThreshold() || p.hasSpilledInsertsForCurrentTxn()
}

// hasSpilledInsertsForCurrentTxn prevents state from one transaction from being
// mistaken for pending inserts of another transaction at the same commit-ts.
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

// flushSpilledInsertsIntoCurrentTxn is the same-commit-ts DDL path. It folds
// delayed inserts back into the current batch instead of entering a separate
// drain attempt, then removes the exhausted spill state.
func (p *dmlProcessor) flushSpilledInsertsIntoCurrentTxn() error {
	if p.currentTxn == nil || p.dispatcherStat == nil {
		return nil
	}
	state := p.dispatcherStat.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return nil
	}
	for {
		insertRow, err := state.nextInsert(p.ctx)
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

// getOrCreateLargeTxnState returns dispatcher-owned state that survives the
// current processor and scan attempt. The identity fields prevent fragments
// from different transactions from sharing one spill lifecycle.
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
