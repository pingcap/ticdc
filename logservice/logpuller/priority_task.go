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

<<<<<<< HEAD
import (
	"fmt"
	"time"

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
=======
import "github.com/pingcap/kvproto/pkg/cdcpb"

func normalizeScanPriority(priority cdcpb.ScanPriority) cdcpb.ScanPriority {
	if priority == cdcpb.ScanPriority_SCAN_PRIORITY_HIGH {
		return cdcpb.ScanPriority_SCAN_PRIORITY_HIGH
	}
	return cdcpb.ScanPriority_SCAN_PRIORITY_LOW
>>>>>>> d4b9c2259 (logpuller: refactor region request worker lifecycle (#5619))
}

func isHighScanPriority(priority cdcpb.ScanPriority) bool {
	return normalizeScanPriority(priority) == cdcpb.ScanPriority_SCAN_PRIORITY_HIGH
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

func (pt *regionPriorityTask) priority() cdcpb.ScanPriority {
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
