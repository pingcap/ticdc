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

package eventstore

import "github.com/pingcap/ticdc/pkg/common"

// ScanPosition is an opaque eventstore-owned token used to resume after a
// specific row inside a scan window.
type ScanPosition []byte

// ScanCursor identifies where scanning should resume inside a DataRange.
// Position is an opaque eventstore-owned token and takes precedence over
// TxnStartTs when both are present.
type ScanCursor struct {
	TxnStartTs uint64
	Position   ScanPosition
}

// ScanRequest combines a pure data range with its resume cursor.
type ScanRequest struct {
	Range  common.DataRange
	Cursor ScanCursor
}
