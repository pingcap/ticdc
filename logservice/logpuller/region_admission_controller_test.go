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
	taskType TaskType,
	currentTs uint64,
) {
	t.Helper()
	task := NewRegionPriorityTask(taskType, region, currentTs)
	require.True(t, controller.submit(task, region, currentTs))
}

func TestRegionAdmissionControllerNormalWindow(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	checkpointTs := oracle.GoTimeToTS(time.Now().Add(-20 * time.Minute))
	region1 := prepareRegionForAdmission(createTestRegionInfo(1, 1), checkpointTs)
	region2 := prepareRegionForAdmission(createTestRegionInfo(1, 2), checkpointTs)
	submitRegionForAdmission(t, controller, region1, TaskLowPrior, currentTs)
	submitRegionForAdmission(t, controller, region2, TaskLowPrior, currentTs)

	req1, err := controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, controller.inflightCount())
	req2, closed := controller.tryPop()
	require.Nil(t, req2)
	require.False(t, closed)

	require.True(t, req1.abort())
	req2, err = controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), req2.regionInfo.verID.GetID())
	require.True(t, req2.abort())
}

func TestRegionAdmissionControllerFastScanUsesMaxWindow(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	slowCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-20 * time.Minute))
	fastCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Minute))

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 1), slowCheckpointTs),
		TaskLowPrior, currentTs)
	req1, err := controller.pop(t.Context())
	require.NoError(t, err)

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 2), slowCheckpointTs),
		TaskLowPrior, currentTs)
	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 3), fastCheckpointTs),
		TaskLowPrior, currentTs)

	req2, err := controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), req2.regionInfo.verID.GetID())
	req3, closed := controller.tryPop()
	require.Nil(t, req3)
	require.False(t, closed)
	require.Equal(t, 2, controller.inflightCount())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
	req3, err = controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), req3.regionInfo.verID.GetID())
	require.True(t, req3.abort())
}

func TestRegionAdmissionControllerPrioritizesRecovery(t *testing.T) {
	controller := newRegionAdmissionController(1, 2)
	currentTs := oracle.GoTimeToTS(time.Now())
	slowCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-20 * time.Minute))
	fastCheckpointTs := oracle.GoTimeToTS(time.Now().Add(-time.Minute))

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 1), slowCheckpointTs),
		TaskLowPrior, currentTs)
	req1, err := controller.pop(t.Context())
	require.NoError(t, err)

	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 2), fastCheckpointTs),
		TaskLowPrior, currentTs)
	submitRegionForAdmission(t, controller,
		prepareRegionForAdmission(createTestRegionInfo(1, 3), slowCheckpointTs),
		TaskHighPrior, currentTs)

	req2, err := controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), req2.regionInfo.verID.GetID())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
	req3, err := controller.pop(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(2), req3.regionInfo.verID.GetID())
	require.True(t, req3.abort())
}

func TestRegionAdmissionLeaseReleasedOnce(t *testing.T) {
	controller := newRegionAdmissionController(1, 1)
	currentTs := oracle.GoTimeToTS(time.Now())
	region := prepareRegionForAdmission(createTestRegionInfo(1, 1), currentTs)
	submitRegionForAdmission(t, controller, region, TaskLowPrior, currentTs)
	req, err := controller.pop(t.Context())
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
	require.Zero(t, controller.inflightCount())
}

func TestRegionAdmissionControllerClose(t *testing.T) {
	controller := newRegionAdmissionController(1, 1)
	controller.close()
	region := prepareRegionForAdmission(createTestRegionInfo(1, 1), 1)
	require.False(t, controller.submit(NewRegionPriorityTask(TaskLowPrior, region, 1), region, 1))

	_, err := controller.pop(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}
