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
func (g *EventsGroup) AppendMessage(message *codeccommon.DMLMessage, force bool) {
	commitTs := message.GetCommitTs()
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}

	var lastMessage *codeccommon.DMLMessage
	if len(g.messages) > 0 {
		lastMessage = g.messages[len(g.messages)-1]
	}

	if lastMessage == nil || lastMessage.GetCommitTs() <= commitTs {
		g.messages = append(g.messages, message)
		return
	}

	if force {
		i := sort.Search(len(g.messages), func(i int) bool {
			return g.messages[i].GetCommitTs() > commitTs
		})
		g.messages = append(g.messages, nil)
		copy(g.messages[i+1:], g.messages[i:])
		g.messages[i] = message
		return
	}
	log.Panic("append event with smaller commit ts",
		zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
		zap.Uint64("lastCommitTs", lastMessage.GetCommitTs()), zap.Uint64("commitTs", commitTs))
}

// ResolveInto appends all messages with CommitTs <= resolve into dst and removes them from the group.
// ResolveInto copies pointers into dst first, then clears the resolved prefix so Go GC can reclaim
// resolved messages once downstream is done with them.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*codeccommon.DMLMessage) []*codeccommon.DMLMessage {
	i := sort.Search(len(g.messages), func(i int) bool {
		return g.messages[i].GetCommitTs() > resolve
	})
	if i == 0 {
		return dst
	}

	// Copy pointers out first so we can safely clear the group's slice without affecting callers.
	dst = append(dst, g.messages[:i]...)
	clear(g.messages[:i])
	g.messages = g.messages[i:]
	if len(g.messages) != 0 {
		log.Debug("not all events resolved",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", i), zap.Int("remained", len(g.messages)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", g.messages[0].GetCommitTs()))
	}
	return dst
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() []*codeccommon.DMLMessage {
	result := g.messages
	g.messages = nil
	return result
}

// AppendOrMergeDMLEvent appends a DML event, or merges it into the previous event
// when both events belong to the same table group and have the same commit-ts.
func AppendOrMergeDMLEvent(events []*commonEvent.DMLEvent, row *commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	var lastDMLEvent *commonEvent.DMLEvent
	if len(events) > 0 {
		lastDMLEvent = events[len(events)-1]
	}

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
