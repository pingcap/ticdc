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
) regionInfo {
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	state := &regionlock.LockedRangeState{}
	state.ResolvedTs.Store(checkpointTs)
	return regionInfo{
		verID:            tikv.NewRegionVerID(regionID, 1, 1),
		span:             span,
		subscribedSpan:   &subscribedSpan{subID: 1, startTs: checkpointTs, span: span},
		lockedRangeState: state,
	}
}

func withScanPriority(region regionInfo, priority cdcpb.ScanPriority) regionInfo {
	region.scanPriority = priority
	return region
}

func TestNormalizeScanPriority(t *testing.T) {
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_HIGH, normalizeScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_HIGH))
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_LOW, normalizeScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_LOW))
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_LOW, normalizeScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_UNKNOWN))
	require.True(t, isHighScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_HIGH))
	require.False(t, isHighScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_LOW))
	require.False(t, isHighScanPriority(cdcpb.ScanPriority_SCAN_PRIORITY_UNKNOWN))
}

func TestRegionPriorityTaskQueueOrder(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	currentTime := time.Now()

<<<<<<< HEAD
	lowTask := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(1, oracle.GoTimeToTS(currentTime.Add(-time.Hour))),
			cdcpb.ScanPriority_SCAN_PRIORITY_LOW,
		),
		3,
	)
	highTask1 := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(2, oracle.GoTimeToTS(currentTime.Add(-10*time.Minute))),
			cdcpb.ScanPriority_SCAN_PRIORITY_HIGH,
		),
		2,
	)
	highTask2 := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(3, oracle.GoTimeToTS(currentTime.Add(-time.Hour))),
			cdcpb.ScanPriority_SCAN_PRIORITY_HIGH,
		),
		1,
=======
	normalTask := newRegionPriorityTask(
		newPriorityTestRegion(1, oracle.GoTimeToTS(currentTime.Add(-time.Hour)), false),
		currentTs, 3,
	)
	lowLagTask := newRegionPriorityTask(
		newPriorityTestRegion(2, oracle.GoTimeToTS(currentTime.Add(-10*time.Minute)), false),
		currentTs, 2,
	)
	initializedTask := newRegionPriorityTask(
		newPriorityTestRegion(3, oracle.GoTimeToTS(currentTime.Add(-time.Hour)), true),
		currentTs, 1,
>>>>>>> 9903a1be7 (refactor)
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
	checkpointTs := oracle.GoTimeToTS(currentTime.Add(-time.Hour))

<<<<<<< HEAD
	first := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(1, checkpointTs), cdcpb.ScanPriority_SCAN_PRIORITY_HIGH), 1)
	second := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, checkpointTs), cdcpb.ScanPriority_SCAN_PRIORITY_HIGH), 2)
=======
	first := newRegionPriorityTask(newPriorityTestRegion(1, checkpointTs, false), currentTs, 1)
	second := newRegionPriorityTask(newPriorityTestRegion(2, checkpointTs, false), currentTs, 2)
>>>>>>> 9903a1be7 (refactor)

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
	highTask := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(1, 1), cdcpb.ScanPriority_SCAN_PRIORITY_HIGH), 1)
	lowTask := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, 1), cdcpb.ScanPriority_SCAN_PRIORITY_LOW), 2)

<<<<<<< HEAD
	require.True(t, highTask.canUseMaxWindow())
	require.False(t, lowTask.canUseMaxWindow())
}

func TestRegionPriorityTaskRefreshesRegionInfoBetweenStages(t *testing.T) {
	region := withScanPriority(newPriorityTestRegion(1, 1), cdcpb.ScanPriority_SCAN_PRIORITY_LOW)
	task := newRegionPriorityTask(region, 1)
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_LOW, task.priority())
=======
	belowThreshold := newRegionPriorityTask(newPriorityTestRegion(
		1,
		oracle.GoTimeToTS(currentTime.Add(-lowLagRegionThreshold+time.Millisecond)),
		false,
	), currentTs, 1)
	atThreshold := newRegionPriorityTask(newPriorityTestRegion(
		2,
		oracle.GoTimeToTS(currentTime.Add(-lowLagRegionThreshold)),
		false,
	), currentTs, 2)
	futureCheckpoint := newRegionPriorityTask(newPriorityTestRegion(
		3,
		oracle.GoTimeToTS(currentTime.Add(time.Second)),
		false,
	), currentTs, 3)

	require.Equal(t, lowLagRegionPriority, belowThreshold.priority)
	require.Equal(t, normalRegionPriority, atThreshold.priority)
	require.Equal(t, lowLagRegionPriority, futureCheckpoint.priority)
}

func TestRegionPriorityTaskRefreshesPriorityBetweenStages(t *testing.T) {
	checkpointTime := time.Now()
	checkpointTs := oracle.GoTimeToTS(checkpointTime)
	region := newPriorityTestRegion(1, checkpointTs, false)
	task := newRegionPriorityTask(region, oracle.GoTimeToTS(checkpointTime.Add(time.Minute)), 1)
	require.Equal(t, lowLagRegionPriority, task.priority)
>>>>>>> 9903a1be7 (refactor)

	region.scanPriority = cdcpb.ScanPriority_SCAN_PRIORITY_HIGH
	task.regionInfo = region
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_HIGH, task.priority())
}
