// Copyright 2026 PingCAP, Inc.
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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestRegionInfo(subID SubscriptionID, regionID uint64) regionInfo {
	span := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
	}
	return newRegionInfo(
		tikv.NewRegionVerID(regionID, 1, 1),
		span,
		nil,
		&subscribedSpan{subID: subID, startTs: 100, span: span},
		false,
	)
}

func prepareRegionForAdmission(region regionInfo, checkpointTs uint64) regionInfo {
	region.lockedRangeState = &regionlock.LockedRangeState{}
	region.lockedRangeState.ResolvedTs.Store(checkpointTs)
	return region
}

func submitRegionForAdmission(
	t *testing.T,
	controller *regionAdmissionController,
	region regionInfo,
	currentTs uint64,
) {
	t.Helper()
	task := NewRegionPriorityTask(region, currentTs, region.verID.GetID())
	require.True(t, controller.submit(task))
}

func TestRegionAdmissionControllerNormalWindow(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	checkpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Hour))
	region1 := prepareRegionForAdmission(createTestRegionInfo(1, 1), checkpointTs)
	region2 := prepareRegionForAdmission(createTestRegionInfo(1, 2), checkpointTs)
	submitRegionForAdmission(t, controller, region1, currentTs)
	submitRegionForAdmission(t, controller, region2, currentTs)

	req1, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, 1, controller.stats().inflight)
	interrupt := make(chan struct{})
	close(interrupt)
	req2, err := controller.pop(t.Context(), interrupt)
	require.Nil(t, req2)
	require.NoError(t, err)

	require.True(t, req1.abort())
	req2, err = controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), req2.regionInfo.verID.GetID())
	require.True(t, req2.abort())
}

func TestRegionAdmissionControllerLowLagUsesMaxWindow(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	slowCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Hour))
	lowLagCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Minute))

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 1), slowCheckpointTs),
		currentTs)
	req1, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 2), slowCheckpointTs),
		currentTs)
	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 3), lowLagCheckpointTs),
		currentTs)

	req2, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), req2.regionInfo.verID.GetID())
	interrupt := make(chan struct{})
	close(interrupt)
	req3, err := controller.pop(t.Context(), interrupt)
	require.Nil(t, req3)
	require.NoError(t, err)
	require.Equal(t, 2, controller.stats().inflight)

	require.True(t, req1.abort())
	require.True(t, req2.abort())
	req3, err = controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), req3.regionInfo.verID.GetID())
	require.True(t, req3.abort())
}

func TestRegionAdmissionControllerPrioritizesInitializedRegion(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	slowCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Hour))
	lowLagCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Minute))

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 1), slowCheckpointTs),
		currentTs)
	req1, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 2), lowLagCheckpointTs),
		currentTs)
	initializedRegion := prepareRegionForAdmission(createTestRegionInfo(1, 3), slowCheckpointTs)
	initializedRegion.wasInitialized = true
	submitRegionForAdmission(t, controller,
		initializedRegion, currentTs)

	req2, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), req2.regionInfo.verID.GetID())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
	req3, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), req3.regionInfo.verID.GetID())
	require.True(t, req3.abort())
}

func TestRegionAdmissionLeaseReleasedOnce(t *testing.T) {
	controller := newRegionAdmissionController(1, 1)
	currentTs := oracle.GoTimeToTS(time.Now())
	region := prepareRegionForAdmission(createTestRegionInfo(1, 1), currentTs)
	submitRegionForAdmission(t, controller, region, currentTs)
	req, err := controller.pop(t.Context(), nil)
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		results <- req.finish()
	}()
	go func() {
		defer wg.Done()
		<-start
		results <- req.abort()
	}()
	close(start)
	wg.Wait()
	close(results)

	successes := 0
	for result := range results {
		if result {
			successes++
		}
	}
	require.Equal(t, 1, successes)
	require.Zero(t, controller.stats().inflight)
}

func TestRegionAdmissionControllerClose(t *testing.T) {
	controller := newRegionAdmissionController(1, 1)
	controller.close()
	region := prepareRegionForAdmission(createTestRegionInfo(1, 1), 1)
	require.False(t, controller.submit(NewRegionPriorityTask(region, 1, 1)))

	_, err := controller.pop(context.Background(), nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRegionAdmissionControllerDrainPending(t *testing.T) {
	controller := newRegionAdmissionController(1, 1)
	region1 := prepareRegionForAdmission(createTestRegionInfo(1, 1), 1)
	region2 := prepareRegionForAdmission(createTestRegionInfo(1, 2), 1)
	submitRegionForAdmission(t, controller, region1, 1)
	submitRegionForAdmission(t, controller, region2, 1)

	pending := controller.drain()
	require.Len(t, pending, 2)
	require.Zero(t, controller.stats().pending)
}
