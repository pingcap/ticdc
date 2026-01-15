// Copyright 2024 PingCAP, Inc.
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

package range_checker

import (
	"strconv"
	"strings"
	"sync/atomic"
)

// TableIDRangeChecker is used to check if all table IDs are covered.
type TableIDRangeChecker struct {
	needCount   int
	tableIDs    []int64
	reportedMap map[int64]struct{}
	covered     atomic.Bool
}

// NewTableCountChecker creates a new TableIDRangeChecker.
func NewTableCountChecker(tableIDs []int64) *TableIDRangeChecker {
	tc := &TableIDRangeChecker{
		tableIDs:    tableIDs,
		needCount:   len(tableIDs),
		reportedMap: make(map[int64]struct{}, len(tableIDs)),
		covered:     atomic.Bool{},
	}
	return tc
}

// AddSubRange adds table id to the range checker.
func (rc *TableIDRangeChecker) AddSubRange(tableID int64, _, _ []byte) {
	rc.reportedMap[tableID] = struct{}{}
}

// IsFullyCovered checks if all table IDs are covered.
func (rc *TableIDRangeChecker) IsFullyCovered() bool {
	return rc.covered.Load() || len(rc.reportedMap) >= rc.needCount
}

// Reset resets the reported tables.
func (rc *TableIDRangeChecker) Reset() {
	rc.reportedMap = make(map[int64]struct{}, rc.needCount)
	rc.covered.Store(false)
}

func (rc *TableIDRangeChecker) Detail() string {
	buf := &strings.Builder{}
	buf.WriteString("reported count: ")
	buf.WriteString(strconv.FormatInt(int64(len(rc.reportedMap)), 10))
	buf.WriteString(", require count: ")
	buf.WriteString(strconv.FormatInt(int64(rc.needCount), 10))

	var uncoveredIDs []string
	for _, id := range rc.tableIDs {
		if _, ok := rc.reportedMap[id]; !ok {
			uncoveredIDs = append(uncoveredIDs, strconv.FormatInt(id, 10))
		}
	}

	if len(uncoveredIDs) > 0 {
		buf.WriteString(", uncovered tables: ")
		buf.WriteString(strings.Join(uncoveredIDs, ", "))
	}

	return buf.String()
}

func (rc *TableIDRangeChecker) MarkCovered() {
	rc.covered.Store(true)
}
