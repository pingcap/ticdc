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

package logpuller

import (
	"fmt"

	"github.com/tikv/client-go/v2/oracle"
)

// TaskType represents the type of region task
type TaskType int

const (
	// TaskHighPrior represents region error or region change
	// This type has the highest priority
	TaskHighPrior TaskType = iota
	// TaskLowPrior represents new subscription
	// This type has the lowest priority
	TaskLowPrior
)

const (
	highPriorityBase   = 0
	lowPriorityBase    = 60 * 60 * 24 // 1 day
	forcedPriorityBase = 60 * 60      // 60 minutes
)

func (t TaskType) String() string {
	return fmt.Sprintf("%d", t)
}

// regionPriorityTask is a heap item for region scheduling.
// Lower priority values are popped first. The priority value is calculated when
// the task is created, so queued tasks do not rely on heap reordering as time
// passes or resolved-ts changes.
type regionPriorityTask struct {
	taskType   TaskType
	priority   int
	seq        uint64
	regionInfo regionInfo
	heapIndex  int // for heap.Item interface
}

// newRegionPriorityTask creates a new priority task for region.
func newRegionPriorityTask(taskType TaskType, regionInfo regionInfo, currentTs uint64, seq uint64) *regionPriorityTask {
	return &regionPriorityTask{
		taskType:   taskType,
		priority:   calculateRegionTaskPriority(taskType, regionInfo, currentTs),
		seq:        seq,
		regionInfo: regionInfo,
		heapIndex:  0, // 0 means not in heap
	}
}

func calculateRegionTaskPriority(taskType TaskType, regionInfo regionInfo, currentTs uint64) int {
	basePriority := 0
	switch taskType {
	case TaskHighPrior:
		basePriority = highPriorityBase // Highest priority
	case TaskLowPrior:
		basePriority = lowPriorityBase // Lowest priority
	}

	// ResolvedTsLag in seconds, longer lag means lower priority (higher value)
	resolvedTsLag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(regionInfo.subscribedSpan.resolvedTs.Load()))
	resolvedTsLagPenalty := int(resolvedTsLag.Seconds())

	return max(basePriority+resolvedTsLagPenalty, 0)
}

// Priority returns the fixed priority value, lower value means higher priority.
func (pt *regionPriorityTask) Priority() int {
	return pt.priority
}

// GetRegionInfo returns the underlying regionInfo
func (pt *regionPriorityTask) GetRegionInfo() regionInfo {
	return pt.regionInfo
}

// SetHeapIndex sets the heap index for heap.Item interface
func (pt *regionPriorityTask) SetHeapIndex(index int) {
	pt.heapIndex = index
}

// GetHeapIndex gets the heap index for heap.Item interface
func (pt *regionPriorityTask) GetHeapIndex() int {
	return pt.heapIndex
}

// LessThan implements heap.Item interface
// Returns true if this task has higher priority (lower priority value) than the other task.
func (pt *regionPriorityTask) LessThan(other *regionPriorityTask) bool {
	if pt.priority != other.priority {
		return pt.priority < other.priority
	}
	return pt.seq < other.seq
}
