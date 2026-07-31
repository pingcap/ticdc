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
<<<<<<< HEAD
	"slices"
=======
	"math"
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
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

<<<<<<< HEAD
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
			previous := g.events[i-1]
			// If the table info version is incompatible, we cannot merge the events,
			//  and the event may be replayed again after the table info is updated.
			// So we just skip this event to avoid potential panic in the downstream.
			if !compareTableInfo(previous, row) {
				log.Warn("skip replayed DML event due to incompatible table info, the event may be replayed again after the table info is updated",
					zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
					zap.Uint64("commitTs", row.CommitTs),
					zap.Any("previous", previous),
					zap.Any("now", row))
				return
			}
			mergeDMLEvent(previous, row)
			return
		}
		g.events = slices.Insert(g.events, i, row)
		return
	}
	log.Panic("append event with smaller commit ts",
		zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
		zap.Uint64("lastCommitTs", lastDMLEvent.GetCommitTs()), zap.Uint64("commitTs", row.GetCommitTs()))
}

func compareTableInfo(previous, now *commonEvent.DMLEvent) bool {
	previousInfo := previous.TableInfo.ToTiDBTableInfo()
	nowInfo := now.TableInfo.ToTiDBTableInfo()
	if previousInfo.UpdateTS > nowInfo.UpdateTS {
		log.Panic("previous dml event has bigger table info version",
			zap.Any("previous", previous),
			zap.Any("now", now))
	}
	return common.NewColumnSchema4Decoder(previousInfo).SameWithTableInfo(nowInfo)
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
=======
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
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))
	}
	return dst
}

<<<<<<< HEAD
// GetAllEvents will get all events.
func (g *EventsGroup) GetAllEvents() []*commonEvent.DMLEvent {
	result := g.events
	g.events = nil
	return result
=======
// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() []*codeccommon.DMLMessage {
	return g.ResolveInto(math.MaxUint64, nil)
>>>>>>> af33cc193 (consumer: sort fallback DML before flush (#5824))
}
