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

// scanProgress is an immutable snapshot published after a scan result has
// been sent. A non-empty rowLevelScanPosition resumes inside the transaction.
type scanProgress struct {
	valid                bool
	txnCommitTs          uint64
	txnStartTs           uint64
	rowLevelScanPosition eventstore.ScanPosition
}

func newTxnScanProgress(commitTs uint64, startTs uint64) scanProgress {
	return scanProgress{
		valid:       true,
		txnCommitTs: commitTs,
		txnStartTs:  startTs,
	}
}

func newRowLevelScanProgress(commitTs uint64, startTs uint64, position eventstore.ScanPosition) scanProgress {
	progress := newTxnScanProgress(commitTs, startTs)
	progress.rowLevelScanPosition = cloneScanPosition(position)
	return progress
}

func cloneScanPosition(position eventstore.ScanPosition) eventstore.ScanPosition {
	if len(position) == 0 {
		return nil
	}
	cloned := make(eventstore.ScanPosition, len(position))
	copy(cloned, position)
	return cloned
}
