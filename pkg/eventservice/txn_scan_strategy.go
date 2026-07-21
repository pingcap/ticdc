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

type txnScanContext struct {
	scanner   *eventScanner
	session   *session
	merger    *eventMerger
	processor *dmlProcessor
}

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
	resumePending(ctx *txnScanContext) (handled bool, interrupted bool, err error)
	startTxn(
		ctx *txnScanContext,
		startTs uint64,
		commitTs uint64,
		tableInfo *common.TableInfo,
		tableID int64,
	) error
	finishTxn(ctx *txnScanContext, next nextTxnMeta) (interrupted bool, err error)
	afterAppend(
		ctx *txnScanContext,
		rawEvent *common.RawKVEntry,
		position eventstore.ScanPosition,
	) (interrupted bool, err error)
}

func newTxnScanStrategy(shouldSplitTxn bool) txnScanStrategy {
	if shouldSplitTxn {
		return splitTxnScanStrategy{}
	}
	return atomicTxnScanStrategy{}
}

type atomicTxnScanStrategy struct{}

func (atomicTxnScanStrategy) resumePending(
	ctx *txnScanContext,
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
	ctx *txnScanContext,
	rawEvent *common.RawKVEntry,
	position eventstore.ScanPosition,
) (bool, error) {
	return false, nil
}

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

func finishPendingSplitTxn(session *session, next nextTxnMeta) bool {
	dispatcher := session.dispatcherStat
	state := dispatcher.getLargeTxnState()
	if state == nil || state.getPhase() != largeTxnScanPhaseOriginal {
		return false
	}
	if next.commitTs != 0 && next.startTs == state.startTs && next.commitTs == state.commitTs {
		return false
	}

	dispatcher.finishPendingBigTxnMetric()
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
		ctx.session.dispatcherStat.addBigTxnMetricFragment(
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
	processor.dispatcherStat = session.dispatcherStat

	for {
		shouldStop, err := ctx.scanner.checkScanConditions(session)
		if err != nil {
			return returnWithError(err)
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
						return returnWithError(err)
					}
					if processor.getCurrentBatchDML().DMLCount() != 0 {
						session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
					}
				}
				state.commitDrainedInserts(drainedInsertCount)
				session.progress = newTxnScanProgress(state.commitTs, state.startTs)
				hasFollowingTxn, followingCommitTs := state.snapshotDrainInfo()
				shouldResolveCommitTs := (!hasFollowingTxn || followingCommitTs > state.commitTs) &&
					session.dataRange.CommitTsEnd == state.commitTs
				if err := session.dispatcherStat.cleanupLargeTxnState(); err != nil {
					log.Warn("cleanup drained large transaction spill failed",
						zap.Stringer("dispatcherID", session.dispatcherStat.id),
						zap.Error(err))
				}
				if shouldResolveCommitTs {
					resolved := event.NewResolvedEvent(
						state.commitTs,
						session.dispatcherStat.id,
						session.dispatcherStat.epoch,
					)
					session.appendEvents([]event.Event{resolved})
					session.progress = newTxnScanProgress(state.commitTs, 0)
					return false, nil
				}
				return true, nil
			}
			return returnWithError(err)
		}
		drainedInsertCount++

		if processor.currentTxn == nil {
			if err := processor.startTxn(
				session.dispatcherStat.id,
				state.tableID,
				state.tableInfo,
				state.startTs,
				state.commitTs,
				true,
			); err != nil {
				return returnWithError(err)
			}
		}
		session.observeRawEntry(entry, nil)
		if err := processor.appendInsertRow(entry); err != nil {
			return returnWithError(err)
		}
		if session.exceedLimit(processor.batchDML.GetSize(), processor.batchDML) {
			if err := processor.commitTxn(); err != nil {
				return returnWithError(err)
			}
			session.appendEvents([]event.Event{processor.getCurrentBatchDML()})
			state.commitDrainedInserts(drainedInsertCount)
			session.progress = newTxnScanProgress(state.commitTs, state.startTs)
			return true, nil
		}
	}
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
