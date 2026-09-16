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
// See the License for the specific language governing permissions and
// limitations under the License.

package eventservice

import "github.com/pingcap/ticdc/logservice/eventstore"

// scanProgress is the immutable EventStore resume point published after a scan
// result has entered the broker's send pipeline.
//
// For example, transaction (startTs=10, commitTs=100) contains rows r1, r2,
// and r3. If a split scan stops after r1 at EventStore position P1, it must
// publish (100, 10, P1). The next scan starts at commitTs 100 and resumes after
// P1, so it can still read r2 and r3. Publishing (100, 10, nil) instead would
// tell EventStore that the whole transaction has been processed and would skip
// those two rows.
//
// The meaningful forms are:
//   - (C, 0, nil): everything through commitTs C is complete.
//   - (C, S, nil): transaction (S, C) is complete in EventStore, although its
//     delayed spill inserts may still need to be drained.
//   - (C, S, P): resume transaction (S, C) after row position P.
//
// A non-empty rowLevelScanPosition is the most precise cursor and takes
// precedence over txnStartTs when EventStore constructs its iterator bounds.
type scanProgress struct {
	// valid distinguishes a complete, publishable snapshot from the zero value
	// left by a canceled or otherwise unfinished scan.
	valid bool
	// txnCommitTs becomes the next scan range's CommitTsStart. With a zero
	// txnStartTs it can represent a resolved boundary rather than a transaction.
	txnCommitTs uint64
	// txnStartTs identifies the completed or partially completed transaction at
	// txnCommitTs. Zero means there is no pending transaction at that boundary.
	txnStartTs uint64
	// rowLevelScanPosition is an opaque EventStore cursor immediately after the
	// last processed row. When non-empty, it resumes inside (txnStartTs,
	// txnCommitTs) instead of skipping that transaction.
	rowLevelScanPosition eventstore.ScanPosition
}

// newTxnScanProgress creates boundary- or transaction-level progress. A zero
// startTs means the commit-ts boundary is fully resolved; a non-zero startTs
// keeps the next request at that transaction until any pending work is done.
func newTxnScanProgress(commitTs uint64, startTs uint64) scanProgress {
	return scanProgress{
		valid:       true,
		txnCommitTs: commitTs,
		txnStartTs:  startTs,
	}
}

// newRowLevelScanProgress creates an exact resume point inside a transaction.
// The position is cloned because ScanPosition is a byte slice owned by the
// EventStore iterator and the progress snapshot outlives that iterator.
func newRowLevelScanProgress(commitTs uint64, startTs uint64, position eventstore.ScanPosition) scanProgress {
	progress := newTxnScanProgress(commitTs, startTs)
	progress.rowLevelScanPosition = cloneScanPosition(position)
	return progress
}

// cloneScanPosition prevents a published immutable snapshot from sharing a
// mutable byte slice with an iterator or a later scan request.
func cloneScanPosition(position eventstore.ScanPosition) eventstore.ScanPosition {
	if len(position) == 0 {
		return nil
	}
	cloned := make(eventstore.ScanPosition, len(position))
	copy(cloned, position)
	return cloned
}
