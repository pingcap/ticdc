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

package schema

import "sync/atomic"

type Workload interface {
	// BuildCreateTableStatement returns the create-table sql of the table n
	BuildCreateTableStatement(n int) string
	// BuildInsertSql returns the insert sql statement of the tableN, insert batchSize records
	BuildInsertSql(tableN int, batchSize int) string
	// BuildUpdateSql return the update sql statement based on the update option
	BuildUpdateSql(opt UpdateOption) string
}

type UpdateOption struct {
	TableIndex      int
	Batch           int
	IsSpecialUpdate bool
	RangeNum        int
}

type TableUpdateRange struct {
	TableIndex int
	// Indicate how many rows between start and end.
	BatchSize int
	Start     int
	End       int
}

type TableUpdateRangeCache struct {
	len int
	// TableUpdateRanges is a slice of tableUpdateRange
	tableUpdateRanges []*TableUpdateRange
	nextIndex         atomic.Int32
}

func NewTableUpdateRangeCache(len int) *TableUpdateRangeCache {
	return &TableUpdateRangeCache{
		len:               len,
		tableUpdateRanges: make([]*TableUpdateRange, 0, len),
	}
}

func (c *TableUpdateRangeCache) GetNextTableUpdateRange() *TableUpdateRange {
	c.nextIndex.Add(1)
	return c.tableUpdateRanges[c.nextIndex.Load()%int32(len(c.tableUpdateRanges))]
}

func (c *TableUpdateRangeCache) AddTableUpdateRange(tableUpdateRange *TableUpdateRange) {
	c.tableUpdateRanges = append(c.tableUpdateRanges, tableUpdateRange)
}

func (c *TableUpdateRangeCache) Len() int {
	return c.len
}
