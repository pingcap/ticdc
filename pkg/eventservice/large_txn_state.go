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
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
)

type largeTxnScanPhase int

const (
	largeTxnScanPhaseOriginal largeTxnScanPhase = iota
	largeTxnScanPhaseDrainInserts
)

type largeTxnScanState struct {
	mu sync.Mutex

	startTs  uint64
	commitTs uint64
	tableID  int64

	tableInfo *common.TableInfo

	phase             largeTxnScanPhase
	hasFollowingTxn   bool
	followingCommitTs uint64

	spill  *largeTxnInsertSpill
	reader *largeTxnInsertSpillReader
	// drainedInsertCount is the number of insert rows returned by completed
	// drain scans. It lets an errored drain reopen the reader and retry only the
	// rows from the current scan attempt.
	drainedInsertCount int
	cleaned            bool
}

func (a *dispatcherStat) getOrCreateLargeTxnState(
	spillDir string,
	tableID int64,
	tableInfo *common.TableInfo,
	startTs uint64,
	commitTs uint64,
) (*largeTxnScanState, error) {
	a.largeTxnStateMu.Lock()
	defer a.largeTxnStateMu.Unlock()

	if a.largeTxnState != nil {
		state := a.largeTxnState
		if state.startTs != startTs || state.commitTs != commitTs || state.tableID != tableID {
			return nil, errors.Errorf(
				"large txn spill state mismatch, existing start-ts: %d, commit-ts: %d, table-id: %d, new start-ts: %d, commit-ts: %d, table-id: %d",
				state.startTs, state.commitTs, state.tableID, startTs, commitTs, tableID)
		}
		return state, nil
	}

	var keyspaceID uint32
	if a.info != nil {
		keyspaceID = a.info.GetTableSpan().KeyspaceID
	}
	spill, err := newLargeTxnInsertSpill(spillDir, keyspaceID)
	if err != nil {
		return nil, err
	}
	state := &largeTxnScanState{
		startTs:   startTs,
		commitTs:  commitTs,
		tableID:   tableID,
		tableInfo: tableInfo,
		spill:     spill,
	}
	a.largeTxnState = state
	return state, nil
}

func (a *dispatcherStat) getLargeTxnState() *largeTxnScanState {
	a.largeTxnStateMu.Lock()
	defer a.largeTxnStateMu.Unlock()
	return a.largeTxnState
}

func (a *dispatcherStat) hasPendingLargeTxnState() bool {
	a.largeTxnStateMu.Lock()
	defer a.largeTxnStateMu.Unlock()
	return a.largeTxnState != nil
}

func (a *dispatcherStat) markLargeTxnDrainInserts(
	startTs uint64,
	commitTs uint64,
	hasFollowingTxn bool,
	followingCommitTs uint64,
) {
	a.largeTxnStateMu.Lock()
	defer a.largeTxnStateMu.Unlock()
	if a.largeTxnState == nil ||
		a.largeTxnState.startTs != startTs ||
		a.largeTxnState.commitTs != commitTs {
		return
	}
	a.largeTxnState.markDrainInserts(hasFollowingTxn, followingCommitTs)
}

func (a *dispatcherStat) cleanupLargeTxnState() error {
	a.largeTxnStateMu.Lock()
	state := a.largeTxnState
	a.largeTxnStateMu.Unlock()

	if state == nil {
		return nil
	}
	if err := state.cleanup(); err != nil {
		return err
	}

	a.largeTxnStateMu.Lock()
	if a.largeTxnState == state {
		a.largeTxnState = nil
	}
	a.largeTxnStateMu.Unlock()
	return nil
}

func (s *largeTxnScanState) appendInsert(ctx context.Context, entry *common.RawKVEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleaned {
		return errors.New("large txn state has been cleaned up")
	}
	if s.phase != largeTxnScanPhaseOriginal {
		return errors.New("large txn spill is no longer accepting original txn rows")
	}
	return s.spill.Append(ctx, entry)
}

func (s *largeTxnScanState) nextInsert(ctx context.Context) (*common.RawKVEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleaned {
		return nil, errors.New("large txn state has been cleaned up")
	}
	if s.reader == nil {
		reader, err := s.spill.NewReader()
		if err != nil {
			return nil, err
		}
		for range s.drainedInsertCount {
			if _, err := reader.Next(ctx); err != nil {
				_ = reader.Close()
				return nil, errors.WrapError(
					errors.ErrSpillFileOp, err, "seek committed spill rows")
			}
		}
		s.reader = reader
	}

	entry, err := s.reader.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	return entry, nil
}

func (s *largeTxnScanState) commitDrainedInserts(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainedInsertCount += count
}

func (s *largeTxnScanState) rollbackDrain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	s.reader = nil
	return err
}

func (s *largeTxnScanState) markDrainInserts(hasFollowingTxn bool, followingCommitTs uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleaned {
		return
	}
	s.phase = largeTxnScanPhaseDrainInserts
	s.hasFollowingTxn = hasFollowingTxn
	s.followingCommitTs = followingCommitTs
}

func (s *largeTxnScanState) getPhase() largeTxnScanPhase {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.phase
}

func (s *largeTxnScanState) snapshotDrainInfo() (bool, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasFollowingTxn, s.followingCommitTs
}

func (s *largeTxnScanState) cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cleaned {
		return nil
	}
	var closeErr error
	if s.reader != nil {
		closeErr = s.reader.Close()
		s.reader = nil
	}
	cleanupErr := s.spill.Cleanup()
	if cleanupErr != nil {
		return cleanupErr
	}
	s.cleaned = true
	return closeErr
}
