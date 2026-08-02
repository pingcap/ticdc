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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func newPriorityTestRegion(
	regionID uint64,
	checkpointTs uint64,
	wasInitialized bool,
) regionInfo {
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	state := &regionlock.LockedRangeState{}
	state.ResolvedTs.Store(checkpointTs)
	return regionInfo{
		verID:            tikv.NewRegionVerID(regionID, 1, 1),
		span:             span,
		subscribedSpan:   &subscribedSpan{subID: 1, startTs: checkpointTs, span: span},
		lockedRangeState: state,
		wasInitialized:   wasInitialized,
	}
}

func withScanPriority(region regionInfo, priority TaskType) regionInfo {
	region.scanPriority = priority.scanPriority()
	return region
}

func TestTaskTypeScanPriorityMapping(t *testing.T) {
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_HIGH, TaskHighPrior.scanPriority())
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_LOW, TaskLowPrior.scanPriority())
	require.Equal(t, TaskHighPrior, taskTypeFromScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_HIGH))
	require.Equal(t, TaskLowPrior, taskTypeFromScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_LOW))
	require.Equal(t, TaskLowPrior, taskTypeFromScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_UNKNOWN))
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_LOW, normalizeScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_UNKNOWN))
}

func TestRegionPriorityTaskQueueOrder(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	lowTask := NewRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(1, oracle.GoTimeToTS(currentTime.Add(-time.Hour)), false),
			TaskLowPrior,
		),
		currentTs, 3,
	)
	highTask1 := NewRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(2, oracle.GoTimeToTS(currentTime.Add(-10*time.Minute)), false),
			TaskHighPrior,
		),
		currentTs, 2,
	)
	highTask2 := NewRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(3, oracle.GoTimeToTS(currentTime.Add(-time.Hour)), true),
			TaskHighPrior,
		),
		currentTs, 1,
	)

	require.True(t, queue.Push(lowTask))
	require.True(t, queue.Push(highTask1))
	require.True(t, queue.Push(highTask2))

	for _, expectedRegionID := range []uint64{3, 2, 1} {
		task, err := queue.Pop(t.Context())
		require.NoError(t, err)
		require.Equal(t, expectedRegionID, task.regionInfo.verID.GetID())
	}
}

func TestRegionPriorityTaskFIFOWithinPriority(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)
	checkpointTs := oracle.GoTimeToTS(currentTime.Add(-time.Hour))

	first := NewRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(1, checkpointTs, false), TaskHighPrior), currentTs, 1)
	second := NewRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, checkpointTs, false), TaskHighPrior), currentTs, 2)

	require.True(t, queue.Push(second))
	require.True(t, queue.Push(first))

	task, err := queue.Pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), task.regionInfo.verID.GetID())
	task, err = queue.Pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), task.regionInfo.verID.GetID())
}

func TestRegionPriorityTaskUsesHighPriorityWindow(t *testing.T) {
	highTask := NewRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(1, 1, false), TaskHighPrior), 0, 1)
	lowTask := NewRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, 1, true), TaskLowPrior), 0, 2)

	require.True(t, highTask.canUseMaxWindow())
	require.False(t, lowTask.canUseMaxWindow())
}

func TestRegionPriorityTaskRefreshesPriorityBetweenStages(t *testing.T) {
	region := withScanPriority(newPriorityTestRegion(1, 1, false), TaskLowPrior)
	task := NewRegionPriorityTask(region, 0, 1)
	require.Equal(t, TaskLowPrior, task.taskType)

	region.scanPriority = TaskHighPrior.scanPriority()
	task.updateRegion(region, 0)
	require.Equal(t, TaskHighPrior, task.taskType)
}
