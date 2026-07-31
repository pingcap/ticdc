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

package util

import (
	"math"
	"sort"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// EventsGroup stores change event messages.
type EventsGroup struct {
	Partition int32
	tableID   int64

	messages      []*codeccommon.DMLMessage
	HighWatermark uint64
	// AppliedWatermark is the maximum CommitTs that has been successfully flushed
	// to the downstream for this group.
	//
	// It is used to distinguish "safe to ignore" replays (CommitTs <=
	// AppliedWatermark) from "still needed" events that arrive late due to sink
	// retries / restarts.
	AppliedWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64) *EventsGroup {
	return &EventsGroup{
		Partition: partition,
		tableID:   tableID,
		messages:  make([]*codeccommon.DMLMessage, 0, 1024),
	}
}

// AppendMessage appends a message to event groups.
func (g *EventsGroup) AppendMessage(message *codeccommon.DMLMessage) {
	commitTs := message.GetCommitTs()
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}
	g.messages = append(g.messages, message)
}

// ResolveInto appends all messages with CommitTs <= resolve into dst in commit-ts order and removes
// them from the group. ResolveInto copies pointers into dst first, then clears the resolved messages
// so Go GC can reclaim them once downstream is done with them.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*codeccommon.DMLMessage) []*codeccommon.DMLMessage {
	if len(g.messages) == 0 {
		return dst
	}

	original := g.messages
	remaining := g.messages[:0]
	resolved := make([]*codeccommon.DMLMessage, 0, len(g.messages))

	var (
		lastCommitTs       uint64
		outOfOrder         bool
		outOfOrderLastTs   uint64
		outOfOrderCommitTs uint64
	)
	for _, message := range g.messages {
		commitTs := message.GetCommitTs()
		if commitTs > resolve {
			remaining = append(remaining, message)
			continue
		}
		if len(resolved) > 0 && commitTs < lastCommitTs && !outOfOrder {
			outOfOrder = true
			outOfOrderLastTs = lastCommitTs
			outOfOrderCommitTs = commitTs
		}
		lastCommitTs = commitTs
		resolved = append(resolved, message)
	}
	if len(resolved) == 0 {
		return dst
	}

	if outOfOrder {
		log.Warn("DML events are out of order before flush, sort them",
			zap.Int32("partition", g.Partition),
			zap.Int64("tableID", g.tableID),
			zap.Uint64("resolveTs", resolve),
			zap.Int("resolved", len(resolved)),
			zap.Uint64("lastCommitTs", outOfOrderLastTs),
			zap.Uint64("commitTs", outOfOrderCommitTs))
		sort.SliceStable(resolved, func(i, j int) bool {
			return resolved[i].GetCommitTs() < resolved[j].GetCommitTs()
		})
	}

	dst = append(dst, resolved...)
	clear(original[len(remaining):])
	g.messages = remaining
	if len(g.messages) != 0 {
		firstCommitTs := g.messages[0].GetCommitTs()
		log.Debug("not all events resolved",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", len(resolved)), zap.Int("remained", len(g.messages)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", firstCommitTs))
	}
	return dst
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() []*codeccommon.DMLMessage {
	return g.ResolveInto(math.MaxUint64, nil)
}

// AppendOrMergeDMLEvent appends a DML event, or merges it into the previous event
// when both events belong to the same table group and have the same commit-ts.
func AppendOrMergeDMLEvent(events []*commonEvent.DMLEvent, row *commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	var lastDMLEvent *commonEvent.DMLEvent
	if len(events) > 0 {
		lastDMLEvent = events[len(events)-1]
	}

	// mergeDMLEvent := func(dst, src *commonEvent.DMLEvent) {
	// 	dst.Rows.Append(src.Rows, 0, src.Rows.NumRows())
	// 	dst.RowTypes = append(dst.RowTypes, src.RowTypes...)
	// 	dst.Length += src.Length
	// 	dst.PostTxnFlushed = append(dst.PostTxnFlushed, src.PostTxnFlushed...)
	// }

	if lastDMLEvent == nil || lastDMLEvent.GetCommitTs() < row.GetCommitTs() {
		return append(events, row)
	}

	if lastDMLEvent.GetCommitTs() == row.GetCommitTs() {
		lastDMLEvent.Rows.Append(row.Rows, 0, row.Rows.NumRows())
		lastDMLEvent.RowTypes = append(lastDMLEvent.RowTypes, row.RowTypes...)
		lastDMLEvent.Length += row.Length
		lastDMLEvent.PostTxnFlushed = append(lastDMLEvent.PostTxnFlushed, row.PostTxnFlushed...)
		return events
	}

	log.Panic("append event with smaller commit ts",
		zap.Int64("tableID", row.GetTableID()),
		zap.Uint64("lastCommitTs", lastDMLEvent.GetCommitTs()), zap.Uint64("commitTs", row.GetCommitTs()))
	return events
}
