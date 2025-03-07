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
	"fmt"
	"sync/atomic"

	"github.com/ngaut/log"
	"go.uber.org/zap"
)

// TableIDRangeChecker is used to check if all table IDs are covered.
type TableIDRangeChecker struct {
	needCount   int
	reportedMap map[int64]struct{}
	covered     atomic.Bool
}

// NewTableCountChecker creates a new TableIDRangeChecker.
func NewTableCountChecker(tables int) *TableIDRangeChecker {
	tc := &TableIDRangeChecker{
		needCount:   tables,
		reportedMap: make(map[int64]struct{}, tables),
		covered:     atomic.Bool{},
	}
	return tc
}

// AddSubRange adds table id to the range checker.
func (rc *TableIDRangeChecker) AddSubRange(tableID int64, _, _ []byte) {
	log.Info("add sub range", zap.Int64("tableID", tableID), zap.Any("needCount", rc.needCount))
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
	return fmt.Sprintf("reported count: %d, require count: %d", len(rc.reportedMap), rc.needCount)
}

func (rc *TableIDRangeChecker) MarkCovered() {
	rc.covered.Store(true)
}
