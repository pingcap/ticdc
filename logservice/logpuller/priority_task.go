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

import "github.com/pingcap/kvproto/pkg/cdcpb"

// TaskType represents the scan priority level associated with a region task.
type TaskType int

const (
	// TaskHighPrior represents high scan priority.
	// For example, a region task for a region that is already close to caught up
	// is typically tagged with this level.
	TaskHighPrior TaskType = iota
	// TaskLowPrior represents low scan priority.
	// For example, a region task created for a subscription that starts from an
	// older start-ts is typically tagged with this level before it catches up.
	TaskLowPrior
)

func (t TaskType) String() string {
	switch t {
	case TaskHighPrior:
		return "high"
	case TaskLowPrior:
		return "low"
	default:
		return "unknown"
	}
}

func (t TaskType) scanPriority() cdcpb.ScanPriority {
	switch t {
	case TaskHighPrior:
		return cdcpb.ScanPriority_SCAN_PRIORITY_HIGH
	case TaskLowPrior:
		return cdcpb.ScanPriority_SCAN_PRIORITY_LOW
	default:
		return cdcpb.ScanPriority_SCAN_PRIORITY_LOW
	}
}

func taskTypeFromScanPriority(priority cdcpb.ScanPriority) TaskType {
	if priority == cdcpb.ScanPriority_SCAN_PRIORITY_HIGH {
		return TaskHighPrior
	}
	return TaskLowPrior
}

func normalizeScanPriority(priority cdcpb.ScanPriority) cdcpb.ScanPriority {
	return taskTypeFromScanPriority(priority).scanPriority()
}

type regionPriorityTask struct {
	taskType   TaskType
	regionInfo regionInfo
	sequence   uint64
	heapIndex  int // for heap.Item interface
}

// NewRegionPriorityTask creates a new priority task for region
func NewRegionPriorityTask(regionInfo regionInfo, _ uint64, sequence uint64) *regionPriorityTask {
	task := &regionPriorityTask{
		taskType:  taskTypeFromScanPriority(regionInfo.scanPriority),
		sequence:  sequence,
		heapIndex: 0, // 0 means not in heap
	}
	task.updateRegion(regionInfo, 0)
	return task
}

// updateRegion refreshes both the request data and its priority before the task
// enters another scheduling stage.
func (pt *regionPriorityTask) updateRegion(regionInfo regionInfo, _ uint64) {
	pt.regionInfo = regionInfo
	pt.taskType = taskTypeFromScanPriority(regionInfo.scanPriority)
}

// GetRegionInfo returns the underlying regionInfo
func (pt *regionPriorityTask) GetRegionInfo() regionInfo {
	return pt.regionInfo
}

func (pt *regionPriorityTask) canUseMaxWindow() bool {
	return pt.taskType == TaskHighPrior
}

// SetHeapIndex sets the heap index for heap.Item interface
func (pt *regionPriorityTask) SetHeapIndex(index int) {
	pt.heapIndex = index
}

// GetHeapIndex gets the heap index for heap.Item interface
func (pt *regionPriorityTask) GetHeapIndex() int {
	return pt.heapIndex
}

// LessThan implements heap.Item interface. Tasks in the same priority class are
// processed in submission order.
func (pt *regionPriorityTask) LessThan(other *regionPriorityTask) bool {
	if pt.taskType != other.taskType {
		return pt.taskType < other.taskType
	}
	return pt.sequence < other.sequence
}
