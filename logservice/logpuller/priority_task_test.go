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

func withScanPriority(region regionInfo, priority ScanPriorityLevel) regionInfo {
	region.scanPriority = priority
	return region
}

func TestNormalizeScanPriority(t *testing.T) {
	require.Equal(t, ScanPriorityHigh, normalizeScanPriority(ScanPriorityHigh))
	require.Equal(t, ScanPriorityLow, normalizeScanPriority(ScanPriorityLow))
	require.Equal(t, ScanPriorityLow, normalizeScanPriority(ScanPriorityUnknown))
	require.True(t, isHighScanPriority(ScanPriorityHigh))
	require.False(t, isHighScanPriority(ScanPriorityLow))
	require.False(t, isHighScanPriority(ScanPriorityUnknown))
}

func TestRegionPriorityTaskQueueOrder(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	currentTime := time.Now()

	lowTask := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(1, oracle.GoTimeToTS(currentTime.Add(-time.Hour))),
			ScanPriorityLow,
		),
		3,
	)
	highTask1 := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(2, oracle.GoTimeToTS(currentTime.Add(-10*time.Minute))),
			ScanPriorityHigh,
		),
		2,
	)
	highTask2 := newRegionPriorityTask(
		withScanPriority(
			newPriorityTestRegion(3, oracle.GoTimeToTS(currentTime.Add(-time.Hour))),
			ScanPriorityHigh,
		),
		1,
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

	first := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(1, checkpointTs), ScanPriorityHigh), 1)
	second := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, checkpointTs), ScanPriorityHigh), 2)

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
		withScanPriority(newPriorityTestRegion(1, 1), ScanPriorityHigh), 1)
	lowTask := newRegionPriorityTask(
		withScanPriority(newPriorityTestRegion(2, 1), ScanPriorityLow), 2)

	require.True(t, highTask.canUseMaxWindow())
	require.False(t, lowTask.canUseMaxWindow())
}

func TestRegionPriorityTaskRefreshesRegionInfoBetweenStages(t *testing.T) {
	region := withScanPriority(newPriorityTestRegion(1, 1), ScanPriorityLow)
	task := newRegionPriorityTask(region, 1)
	require.Equal(t, ScanPriorityLow, task.priority())

	region.scanPriority = ScanPriorityHigh
	task.regionInfo = region
	require.Equal(t, ScanPriorityHigh, task.priority())
}
