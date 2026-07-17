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

func TestRegionPriorityTaskQueueOrder(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

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
	)

	require.True(t, queue.Push(normalTask))
	require.True(t, queue.Push(lowLagTask))
	require.True(t, queue.Push(initializedTask))

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

	first := newRegionPriorityTask(newPriorityTestRegion(1, checkpointTs, false), currentTs, 1)
	second := newRegionPriorityTask(newPriorityTestRegion(2, checkpointTs, false), currentTs, 2)

	require.True(t, queue.Push(second))
	require.True(t, queue.Push(first))

	task, err := queue.Pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), task.regionInfo.verID.GetID())
	task, err = queue.Pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), task.regionInfo.verID.GetID())
}

func TestRegionPriorityTaskLowLagBoundary(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

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

	task.updateRegion(region, oracle.GoTimeToTS(checkpointTime.Add(time.Hour)))
	require.Equal(t, normalRegionPriority, task.priority)

	region.wasInitialized = true
	task.updateRegion(region, oracle.GoTimeToTS(checkpointTime.Add(time.Hour)))
	require.Equal(t, initializedRegionPriority, task.priority)
}
