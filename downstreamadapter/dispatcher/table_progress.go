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

package dispatcher

import (
	"container/list"
	"sync"
	"time"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// TableProgress maintains event timestamp information in the sink.
// It provides the ability to:
// - Query the current table checkpoint timestamp
// - Check if there are any events waiting to be flushed
// - Query event size flushed per second
//
// TableProgress assumes the event timestamps are monotonically increasing.
//
// This struct is thread-safe.
type TableProgress struct {
	rwMutex     sync.RWMutex
	list        *list.List
	elemMap     map[Ts]*list.Element
	maxCommitTs uint64
	// lastSyncedTs is the last commit ts that has been synced to downstream.
	// It's used in /:changefeed_id/synced API.
	lastSyncedTs uint64

	// cumulate dml event size for a period of time,
	// it will be cleared after once query
	cumulateEventSize int64
	// it used to calculate the sum-dml-event-size/s for each dispatcher
	lastQueryTime time.Time
}

// Ts represents a timestamp pair, used for sorting primarily by commitTs and secondarily by startTs.
type Ts struct {
	commitTs uint64
	startTs  uint64
}

// NewTableProgress creates and initializes a new TableProgress instance.
func NewTableProgress() *TableProgress {
	return &TableProgress{
		list:              list.New(),
		elemMap:           make(map[Ts]*list.Element),
		maxCommitTs:       0,
		cumulateEventSize: 0,
		lastQueryTime:     time.Now(),
	}
}

func getFinalCommitTs(event commonEvent.Event) uint64 {
	commitTs := event.GetCommitTs()
	if event.GetType() == commonEvent.TypeSyncPointEvent {
		// if the event is a sync point event, we use the last commitTs(the largest commitTs in the event) to calculate the progress.
		// because a sync point event with multiple commitTs means there is no ddl / dmls between these commitTs.
		// So we can just use the largest commitTs in the sync point event to calculate the progress.
		commitTsList := event.(*commonEvent.SyncPointEvent).GetCommitTsList()
		commitTs = commitTsList[len(commitTsList)-1]
	}
	return commitTs
}

// Add inserts a new event into the TableProgress.
func (p *TableProgress) Add(event commonEvent.FlushableEvent) {
	commitTs := getFinalCommitTs(event)
	ts := Ts{startTs: event.GetStartTs(), commitTs: commitTs}

	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	elem := p.list.PushBack(ts)
	p.elemMap[ts] = elem
	p.maxCommitTs = commitTs
	event.PushFrontFlushFunc(func() {
		p.Remove(event)
	})
}

// Remove deletes an event from the TableProgress.
// Note: Consider implementing batch removal in the future if needed.
func (p *TableProgress) Remove(event commonEvent.FlushableEvent) {
	ts := Ts{startTs: event.GetStartTs(), commitTs: getFinalCommitTs(event)}
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if elem, ok := p.elemMap[ts]; ok {
		p.list.Remove(elem)
		delete(p.elemMap, ts)
		// Get the bigger last synced ts of dispatcher.
		// We don't allow lastSyncedTs to move backwards here.
		if p.lastSyncedTs < ts.commitTs {
			p.lastSyncedTs = ts.commitTs
		}
	}
	p.cumulateEventSize += event.GetSize()
}

// Empty checks if the TableProgress is empty.
func (p *TableProgress) Empty() bool {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	return p.list.Len() == 0
}

// Pass updates the maxCommitTs with the given event's commit timestamp.
func (p *TableProgress) Pass(event commonEvent.FlushableEvent) {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	p.maxCommitTs = getFinalCommitTs(event)
}

func (p *TableProgress) Len() int {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	return p.list.Len()
}

func (p *TableProgress) MaxCommitTs() uint64 {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	return p.maxCommitTs
}

// GetCheckpointTs returns the current checkpoint timestamp for the table span.
// It returns:
// 1. The commitTs of the earliest unflushed event minus 1, if there are unflushed events.
// 2. The highest commitTs seen minus 1, if there are no unflushed events.
// 3. 0, if no events have been processed yet.
//
// It also returns a boolean indicating whether the TableProgress is empty.
// If empty and resolvedTs > checkpointTs, use resolvedTs as the actual checkpointTs.
func (p *TableProgress) GetCheckpointTs() (uint64, bool) {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if p.list.Len() == 0 {
		if p.maxCommitTs == 0 {
			return 0, true
		}
		return p.maxCommitTs - 1, true
	}
	return p.list.Front().Value.(Ts).commitTs - 1, false
}

func (p *TableProgress) GetLastSyncedTs() uint64 {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()
	return p.lastSyncedTs
}

// GetEventSizePerSecond returns the sum-dml-event-size/s between the last query time and now.
// Besides, it clears the cumulateEventSize and update lastQueryTime to prepare for the next query.
func (p *TableProgress) GetEventSizePerSecond() float32 {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	eventSizePerSecond := float32(p.cumulateEventSize) / float32(time.Since(p.lastQueryTime).Seconds())
	p.cumulateEventSize = 0
	p.lastQueryTime = time.Now()

	if eventSizePerSecond == 0 {
		// The event size will only send to maintainer once per second.
		// So if no data is write, we use a tiny value instead of 0 to distinguish it from the status without eventSize
		return 1
	}

	return eventSizePerSecond
}
