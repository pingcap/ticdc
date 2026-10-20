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

// scanPriorityLevel controls TiCDC's local region scheduling and admission.
type scanPriorityLevel uint8

const (
	scanPriorityUnknown scanPriorityLevel = iota
	scanPriorityLow
	scanPriorityHigh
)

func (priority scanPriorityLevel) String() string {
	switch priority {
	case scanPriorityHigh:
		return "SCAN_PRIORITY_HIGH"
	case scanPriorityLow:
		return "SCAN_PRIORITY_LOW"
	default:
		return "SCAN_PRIORITY_UNKNOWN"
	}
}

func normalizeScanPriority(priority scanPriorityLevel) scanPriorityLevel {
	if priority == scanPriorityHigh {
		return scanPriorityHigh
	}
	return scanPriorityLow
}

func isHighScanPriority(priority scanPriorityLevel) bool {
	return normalizeScanPriority(priority) == scanPriorityHigh
}

type regionPriorityTask struct {
	regionInfo regionInfo
	sequence   uint64
	heapIndex  int // for heap.Item interface
}

// newRegionPriorityTask creates a new priority task for region.
func newRegionPriorityTask(regionInfo regionInfo, sequence uint64) *regionPriorityTask {
	regionInfo.scanPriority = normalizeScanPriority(regionInfo.scanPriority)
	return &regionPriorityTask{
		regionInfo: regionInfo,
		sequence:   sequence,
		heapIndex:  0, // 0 means not in heap
	}
}

func (pt *regionPriorityTask) priority() scanPriorityLevel {
	return normalizeScanPriority(pt.regionInfo.scanPriority)
}

func (pt *regionPriorityTask) canUseMaxWindow() bool {
	return isHighScanPriority(pt.regionInfo.scanPriority)
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
	if isHighScanPriority(pt.regionInfo.scanPriority) != isHighScanPriority(other.regionInfo.scanPriority) {
		return isHighScanPriority(pt.regionInfo.scanPriority)
	}
	return pt.sequence < other.sequence
}
