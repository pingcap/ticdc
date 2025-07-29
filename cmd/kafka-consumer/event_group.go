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

package main

import (
	"slices"
	"sort"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// EventsGroup could store change event message.
type eventsGroup struct {
	partition int32
	tableID   int64

	events        []*commonEvent.DMLEvent
	highWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64) *eventsGroup {
	return &eventsGroup{
		partition: partition,
		tableID:   tableID,
		events:    make([]*commonEvent.DMLEvent, 0, 1024),
	}
}

// Append will append an event to event groups.
func (g *eventsGroup) Append(row *commonEvent.DMLEvent, force bool) {
	if row.CommitTs > g.highWatermark {
		g.highWatermark = row.CommitTs
	}

	var lastDMLEvent *commonEvent.DMLEvent
	if len(g.events) > 0 {
		lastDMLEvent = g.events[len(g.events)-1]
	}

	if lastDMLEvent == nil || lastDMLEvent.GetCommitTs() < row.GetCommitTs() {
		g.events = append(g.events, row)
		return
	}

	if lastDMLEvent.GetCommitTs() == row.GetCommitTs() {
		lastDMLEvent.Rows.Append(row.Rows, 0, row.Rows.NumRows())
		lastDMLEvent.RowTypes = append(lastDMLEvent.RowTypes, row.RowTypes...)
		lastDMLEvent.Length += row.Length
		lastDMLEvent.PostTxnFlushed = append(lastDMLEvent.PostTxnFlushed, row.PostTxnFlushed...)
		return
	}

	if force {
		i := sort.Search(len(g.events), func(i int) bool {
			return g.events[i].CommitTs > row.CommitTs
		})
		g.events = slices.Insert(g.events, i, row)
		return
	}
	log.Panic("append event with smaller commit ts",
		zap.Int32("partition", g.partition), zap.Int64("tableID", g.tableID),
		zap.Uint64("lastCommitTs", lastDMLEvent.GetCommitTs()), zap.Uint64("commitTs", row.GetCommitTs()))
}

// Resolve will get events where CommitTs is less than resolveTs.
func (g *eventsGroup) Resolve(resolve uint64) []*commonEvent.DMLEvent {
	i := sort.Search(len(g.events), func(i int) bool {
		return g.events[i].CommitTs > resolve
	})

	result := g.events[:i]
	g.events = g.events[i:]
	if len(result) != 0 && len(g.events) != 0 {
		log.Warn("not all events resolved",
			zap.Int32("partition", g.partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", len(result)), zap.Int("remained", len(g.events)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", g.events[0].CommitTs))
	}

	return result
}
