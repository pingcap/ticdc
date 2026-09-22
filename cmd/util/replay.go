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

package util

import (
	"encoding/binary"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// replayKey identifies one row mutation of one transaction.
//
// A transaction mutates a handle key at most once, and the replayed copy of a mutation
// carries the same commit-ts, so (row type, handle key) is the only stable identity: a
// copied message has a different offset and dispatcher sequence, and the MQ payload
// itself carries no event id.
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

// ReplayFilter drops replayed row mutations from the batch being written downstream.
//
// MQ delivery replays a row mutation at an unclosed commit-ts boundary: when a
// dispatcher sends data that never reached a reliable checkpoint, a new owner rescans
// from an earlier checkpoint and sends the same upstream mutation again. The replayed
// copy is indistinguishable from the original by commit-ts, so it must be dropped here:
// keeping both copies makes the sink batch merger reject its input.
//
// Callers must run FilterBatch before handing events to the sink, Commit after the
// retained rows are durable, and Advance whenever the resolved-ts watermark advances.
type ReplayFilter struct {
	// seen holds the row mutations observed in the batch being filtered.
	seen map[replayKey]struct{}
	// committed holds the row mutations already written downstream whose commit-ts is
	// still ahead of the watermark, so a replay buffered before the watermark advanced
	// is still dropped. Mutations behind the watermark are released, since callers drop
	// those messages by the watermark itself.
	committed map[replayKey]struct{}
}

// NewReplayFilter creates an empty replay filter.
func NewReplayFilter() *ReplayFilter {
	return &ReplayFilter{}
}

// FilterBatch removes the row mutations already seen in this batch, and the ones already
// written downstream with a commit-ts the watermark has not passed yet.
//
// Events without a removed row are returned as-is, partially replayed events are rebuilt
// with a new chunk, and fully replayed events are returned in dropped. The caller must run
// the flush callbacks of the dropped events after the retained rows are durable, otherwise
// the decoder chunk they own and the flush barrier waiting for them never complete.
func (f *ReplayFilter) FilterBatch(events []*commonEvent.DMLEvent) (retained, dropped []*commonEvent.DMLEvent) {
	f.seen = make(map[replayKey]struct{}, len(events))
	for _, e := range events {
		if filtered := filterReplayRows(e, f.seen, f.committed); filtered != nil {
			retained = append(retained, filtered)
		} else {
			dropped = append(dropped, e)
		}
	}
	return retained, dropped
}

// Commit keeps the row mutations observed in the last batch whose commit-ts is still
// ahead of the watermark, so a replay buffered before the watermark advanced is dropped
// later. It must be called after the retained rows are durable.
func (f *ReplayFilter) Commit(watermark uint64) {
	for key := range f.seen {
		if key.commitTs >= watermark {
			if f.committed == nil {
				f.committed = make(map[replayKey]struct{})
			}
			f.committed[key] = struct{}{}
		}
	}
	clear(f.seen)
}

// Advance releases the tracked mutations whose commit-ts fell behind the watermark. The
// caller drops those mutations by the watermark itself, so they no longer need tracking.
func (f *ReplayFilter) Advance(watermark uint64) {
	for key := range f.committed {
		if key.commitTs < watermark {
			delete(f.committed, key)
		}
	}
	if len(f.committed) == 0 {
		f.committed = nil
	}
}

// Len reports the number of tracked mutations, which is used by tests and metrics.
func (f *ReplayFilter) Len() int {
	return len(f.committed)
}

// filterReplayRows keeps callbacks on the original event, whose chunk may be
// owned by the decoder's pool. Only partially duplicated events need a new chunk.
func filterReplayRows(e *commonEvent.DMLEvent, seen, committed map[replayKey]struct{}) *commonEvent.DMLEvent {
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
	filtered := commonEvent.NewDMLEvent(e.DispatcherID, e.PhysicalTableID, e.StartTs, e.CommitTs, e.TableInfo)
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
