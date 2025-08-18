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

package logpuller

import (
	"time"
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

// PriorityTask is the interface for priority-based tasks
// It implements heap.Item interface
type PriorityTask interface {
	// Priority returns the priority value, lower value means higher priority
	Priority() int

	// GetRegionInfo returns the underlying regionInfo
	GetRegionInfo() regionInfo

	// heap.Item interface methods
	SetHeapIndex(int)
	GetHeapIndex() int
	LessThan(PriorityTask) bool
}

// regionPriorityTask implements PriorityTask interface
type regionPriorityTask struct {
	taskType   TaskType
	createTime time.Time
	regionInfo regionInfo
	heapIndex  int // for heap.Item interface
}

// NewRegionPriorityTask creates a new priority task for region
func NewRegionPriorityTask(taskType TaskType, regionInfo regionInfo) PriorityTask {
	return &regionPriorityTask{
		taskType:   taskType,
		createTime: time.Now(),
		regionInfo: regionInfo,
		heapIndex:  0, // 0 means not in heap
	}
}

// Priority calculates the priority based on task type and wait time
// Lower value means higher priority
func (pt *regionPriorityTask) Priority() int {
	// Base priority based on task type
	basePriority := 0
	switch pt.taskType {
	case TaskHighPrior:
		basePriority = 0 // Highest priority
	case TaskLowPrior:
		basePriority = 1000 // Lowest priority
	}

	// Add time-based priority bonus
	// Wait time in milliseconds, longer wait time means higher priority (lower value)
	waitTime := time.Since(pt.createTime)
	timeBonus := int(waitTime.Milliseconds())

	// Total priority = base priority - time bonus
	// Lower value means higher priority
	return basePriority - timeBonus
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
// Returns true if this task has higher priority (lower priority value) than the other task
func (pt *regionPriorityTask) LessThan(other PriorityTask) bool {
	return pt.Priority() < other.Priority()
}
