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

package main

import (
	"encoding/binary"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// The operation distinguishes DELETE and INSERT produced by splitting one UPDATE.
type replayKey struct {
	tableID  int64
	commitTs uint64
	rowType  common.RowType
	handle   string
}

// replayHandle uses exact, length-prefixed values to distinguish composite keys,
// including values containing NUL bytes. No usable handle means no deduplication.
func replayHandle(row chunk.Row, table *common.TableInfo) string {
	var key []byte
	for _, id := range table.GetOrderedHandleKeyColumnIDs() {
		offset := table.MustGetColumnOffsetByID(id)
		value := common.ExtractColVal(&row, table.GetColumns()[offset], offset)
		if value == nil {
			return ""
		}
		s := common.ColumnValueString(value)
		key = binary.AppendUvarint(key, uint64(len(s)))
		key = append(key, s...)
	}
	return string(key)
}

// filterReplayRows keeps callbacks on the original event, whose chunk may be
// owned by the decoder's pool. Only partially duplicated events need a new chunk.
func filterReplayRows(e *event.DMLEvent, seen, committed map[replayKey]struct{}) *event.DMLEvent {
	if e.TableInfo == nil || len(e.TableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		return e
	}
	keep := make([]bool, 0, e.Len())
	kept := 0
	keptPhysicalRows := 0
	e.Rewind()
	for row, ok := e.GetNextRow(); ok; row, ok = e.GetNextRow() {
		handleRow := row.Row
		// Identify an UPDATE by its original handle, including handle changes.
		if row.RowType != common.RowTypeInsert {
			handleRow = row.PreRow
		}
		handle := replayHandle(handleRow, e.TableInfo)
		key := replayKey{tableID: e.PhysicalTableID, commitTs: e.CommitTs, rowType: row.RowType, handle: handle}
		_, duplicate := seen[key]
		_, flushed := committed[key]
		retain := handle == "" || (!duplicate && !flushed)
		if handle != "" {
			seen[key] = struct{}{}
		}
		keep = append(keep, retain)
		if retain {
			kept++
			keptPhysicalRows++
			if row.RowType == common.RowTypeUpdate {
				keptPhysicalRows++
			}
		}
	}
	e.Rewind()
	if kept == len(keep) {
		return e
	}
	if kept == 0 {
		return nil
	}
	filtered := event.NewDMLEvent(e.DispatcherID, e.PhysicalTableID, e.StartTs, e.CommitTs, e.TableInfo)
	filtered.Version = e.Version
	filtered.Seq = e.Seq
	filtered.Epoch = e.Epoch
	filtered.ReplicatingTs = e.ReplicatingTs
	filtered.TableInfoVersion = e.TableInfoVersion
	filtered.ApproximateSize = e.ApproximateSize
	filtered.Rows = chunk.NewChunkWithCapacity(e.TableInfo.GetFieldSlice(), keptPhysicalRows)
	filtered.RowTypes = make([]common.RowType, 0, keptPhysicalRows)
	filtered.AddPostFlushFunc(e.PostFlush)
	filtered.AddPostEnqueueFunc(e.PostEnqueue)
	physicalOffset := 0
	for i, retain := range keep {
		width := 1
		if e.RowTypes[physicalOffset] == common.RowTypeUpdate {
			width = 2
		}
		if retain {
			filtered.Rows.Append(e.Rows, e.PreviousTotalOffset+physicalOffset, e.PreviousTotalOffset+physicalOffset+width)
			filtered.RowTypes = append(filtered.RowTypes, e.RowTypes[physicalOffset:physicalOffset+width]...)
			if len(e.RowKeys) != 0 {
				filtered.RowKeys = append(filtered.RowKeys, e.RowKeys[physicalOffset:physicalOffset+width]...)
			}
			if len(e.Checksum) != 0 {
				filtered.Checksum = append(filtered.Checksum, e.Checksum[i])
			}
			filtered.Length++
		}
		physicalOffset += width
	}
	return filtered
}
