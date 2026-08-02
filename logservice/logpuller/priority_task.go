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
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/tikv/client-go/v2/oracle"
)

// TaskType represents the scan priority propagated to TiKV/CSE.
type TaskType int

const (
	// TaskHighPrior represents retries or scans that should preempt older work.
	TaskHighPrior TaskType = iota
	// TaskLowPrior represents background initial scans for new subscriptions.
	TaskLowPrior
)

const (
	lowLagRegionThreshold = 30 * time.Minute
)

type regionTaskPriority int

const (
	initializedRegionPriority regionTaskPriority = iota
	lowLagRegionPriority
	normalRegionPriority
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
	priority   regionTaskPriority
}

// NewRegionPriorityTask creates a new priority task for region
func NewRegionPriorityTask(regionInfo regionInfo, currentTs, sequence uint64) *regionPriorityTask {
	task := &regionPriorityTask{
		taskType:  taskTypeFromScanPriority(regionInfo.scanPriority),
		sequence:  sequence,
		heapIndex: 0, // 0 means not in heap
	}
	task.updateRegion(regionInfo, currentTs)
	return task
}

// updateRegion refreshes both the request data and its priority before the task
// enters another scheduling stage.
func (pt *regionPriorityTask) updateRegion(regionInfo regionInfo, currentTs uint64) {
	priority := normalRegionPriority
	if regionInfo.wasInitialized {
		priority = initializedRegionPriority
	} else if regionScanLag(currentTs, regionInfo.resolvedTs()) < lowLagRegionThreshold {
		priority = lowLagRegionPriority
	}
	pt.regionInfo = regionInfo
	pt.taskType = taskTypeFromScanPriority(regionInfo.scanPriority)
	pt.priority = priority
}

// GetRegionInfo returns the underlying regionInfo
func (pt *regionPriorityTask) GetRegionInfo() regionInfo {
	return pt.regionInfo
}

func (pt *regionPriorityTask) canUseMaxWindow() bool {
	return pt.priority != normalRegionPriority
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
	if pt.priority != other.priority {
		return pt.priority < other.priority
	}
	return pt.sequence < other.sequence
}

func regionScanLag(currentTs, checkpointTs uint64) time.Duration {
	currentTime := oracle.GetTimeFromTS(currentTs)
	checkpointTime := oracle.GetTimeFromTS(checkpointTs)
	if !currentTime.After(checkpointTime) {
		return 0
	}
	return currentTime.Sub(checkpointTime)
}
