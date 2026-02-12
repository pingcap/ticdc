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
	"slices"
	"sort"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// EventsGroup could store change event message.
type EventsGroup struct {
	Partition int32
	tableID   int64

	events []*commonEvent.DMLEvent
	// HighWatermark is the maximum CommitTs ever observed in this group.
	//
	// It is a "seen" watermark, not a "flushed/applied" watermark. When the
	// consumer reads faster than it flushes to downstream, HighWatermark can be
	// larger than AppliedWatermark.
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
		events:    make([]*commonEvent.DMLEvent, 0, 1024),
	}
}

// Append will append an event to event groups.
func (g *EventsGroup) Append(row *commonEvent.DMLEvent, force bool) {
	if row.CommitTs > g.HighWatermark {
		g.HighWatermark = row.CommitTs
	}

	var lastDMLEvent *commonEvent.DMLEvent
	if len(g.events) > 0 {
		lastDMLEvent = g.events[len(g.events)-1]
	}

	mergeDMLEvent := func(dst, src *commonEvent.DMLEvent) {
		dst.Rows.Append(src.Rows, 0, src.Rows.NumRows())
		dst.RowTypes = append(dst.RowTypes, src.RowTypes...)
		dst.Length += src.Length
		dst.PostTxnFlushed = append(dst.PostTxnFlushed, src.PostTxnFlushed...)
	}

	if lastDMLEvent == nil || lastDMLEvent.GetCommitTs() < row.GetCommitTs() {
		g.events = append(g.events, row)
		return
	}

	if lastDMLEvent.GetCommitTs() == row.GetCommitTs() {
		mergeDMLEvent(lastDMLEvent, row)
		return
	}

	if force {
		// A smaller CommitTs can appear at a larger Kafka offset after a TiCDC
		// restart/retry (at-least-once replay). In this case we need to insert the
		// event by CommitTs order. If the CommitTs already exists, merge it so one
		// upstream transaction isn't split into multiple downstream transactions.
		i := sort.Search(len(g.events), func(i int) bool {
			return g.events[i].CommitTs > row.CommitTs
		})
		if i > 0 && g.events[i-1].CommitTs == row.CommitTs {
			mergeDMLEvent(g.events[i-1], row)
			return
		}
		g.events = slices.Insert(g.events, i, row)
		return
	}
	log.Panic("append event with smaller commit ts",
		zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
		zap.Uint64("lastCommitTs", lastDMLEvent.GetCommitTs()), zap.Uint64("commitTs", row.GetCommitTs()))
}

// ResolveInto appends all events with CommitTs <= resolve into dst and removes them from the group.
// ResolveInto copies pointers into dst first, then clears the
// resolved prefix so Go GC can reclaim resolved events once downstream is done with them.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolve
	})
	if i == 0 {
		return dst
	}

	// Copy pointers out first so we can safely clear the group's slice without affecting callers.
	dst = append(dst, g.events[:i]...)
	clear(g.events[:i])
	g.events = g.events[i:]
	if len(g.events) != 0 {
		log.Debug("not all events resolved",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", i), zap.Int("remained", len(g.events)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", g.events[0].CommitTs))
	}
	return dst
}

// GetAllEvents will get all events.
func (g *EventsGroup) GetAllEvents() []*commonEvent.DMLEvent {
	result := g.events
	g.events = nil
	return result
}
