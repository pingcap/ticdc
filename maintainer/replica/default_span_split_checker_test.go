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

package replica

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func createTestSpanReplication(cfID common.ChangeFeedID, tableID int64) *SpanReplication {
	totalSpan := common.TableIDToComparableSpan(tableID)
	return NewSpanReplication(cfID, common.NewDispatcherID(), 0, &totalSpan, 1, common.DefaultMode)
}

func TestDefaultSpanSplitChecker_AddReplica(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   10,
	}

	mockCache := testutil.NewMockRegionCache()
	appcontext.SetService(appcontext.RegionCache, mockCache)

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	// Create a test replica
	replica := createTestSpanReplication(cfID, 1)

	// Test adding replica
	checker.AddReplica(replica)
	require.Len(t, checker.allTasks, 1)
	require.Contains(t, checker.allTasks, replica.ID)

	status, exists := checker.allTasks[replica.ID]
	require.True(t, exists)
	require.Equal(t, replica, status.SpanReplication)
	require.Equal(t, 0, status.trafficScore)
	require.Equal(t, 0, status.regionCount)

	// Test adding duplicate replica
	checker.AddReplica(replica)
	require.Len(t, checker.allTasks, 1) // Should not add duplicate
}

func TestDefaultSpanSplitChecker_RemoveReplica(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   10,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	// Add replica first
	checker.AddReplica(replica)
	require.Len(t, checker.allTasks, 1)

	// Remove replica
	checker.RemoveReplica(replica)
	require.Len(t, checker.allTasks, 0)
	require.NotContains(t, checker.allTasks, replica.ID)
}

func TestDefaultSpanSplitChecker_UpdateStatus_TrafficCheck(t *testing.T) {
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   10,
	}

	testutil.SetUpTestServices()

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	checker.AddReplica(replica)

	// Test low traffic scenario
	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500, // Below threshold
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	checker.UpdateStatus(replica)
	spanStatus := checker.allTasks[replica.ID]
	require.Equal(t, 0, spanStatus.trafficScore)

	// Test high traffic scenario
	status.EventSizePerSecond = 1500 // Above threshold
	replica.UpdateStatus(status)

	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 1, spanStatus.trafficScore)

	// Test traffic score accumulation
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 2, spanStatus.trafficScore)

	// Test traffic score threshold
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 3, spanStatus.trafficScore)
	require.Contains(t, checker.splitReadyTasks, replica.ID)

	// Test zero traffic
	status.EventSizePerSecond = 0
	replica.UpdateStatus(status)
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 3, spanStatus.trafficScore) // Should not change
}

func TestDefaultSpanSplitChecker_UpdateStatus_RegionCheck(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	// Mock regions
	mockRegions := []*tikv.Region{
		testutil.MockRegionWithID(1),
		testutil.MockRegionWithID(2),
		testutil.MockRegionWithID(3),
		testutil.MockRegionWithID(4),
		testutil.MockRegionWithID(5),
		testutil.MockRegionWithID(6), // Above threshold
	}
	mockCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	mockCache.SetRegions(fmt.Sprintf("%s-%s", replica.Span.StartKey, replica.Span.EndKey), mockRegions)

	checker.AddReplica(replica)

	spanStatus := checker.allTasks[replica.ID]
	spanStatus.regionCheckTime = time.Now().Add(-time.Second * 20)

	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	// Test region count update
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 6, spanStatus.regionCount)
	require.Contains(t, checker.splitReadyTasks, replica.ID)

	// Test region check time interval
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 6, spanStatus.regionCount) // Should not change due to time interval
}

func TestDefaultSpanSplitChecker_UpdateStatus_RegionCheckError(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	// Mock region cache with error
	mockCache := appcontext.GetService[*testutil.MockCache](appcontext.RegionCache)
	mockCache.SetError(context.DeadlineExceeded)

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	checker.AddReplica(replica)

	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	// Test region check error handling
	checker.UpdateStatus(replica)
	spanStatus := checker.allTasks[replica.ID]
	require.Equal(t, 0, spanStatus.regionCount) // Should remain 0 due to error
}

func TestDefaultSpanSplitChecker_UpdateStatus_NonWorkingStatus(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	checker.AddReplica(replica)

	// Test non-working status
	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Stopped,
		EventSizePerSecond: 1500,
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	checker.UpdateStatus(replica)
	spanStatus := checker.allTasks[replica.ID]
	require.Equal(t, 0, spanStatus.trafficScore) // Should not update due to non-working status
}

func TestDefaultSpanSplitChecker_CheckRegionSplit(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	replica := createTestSpanReplication(cfID, 1)

	checker.AddReplica(replica)

	// Set up low traffic but high region count
	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	// Set region count above threshold
	spanStatus := checker.allTasks[replica.ID]
	spanStatus.regionCheckTime = time.Now() // set region check time to now, to skip get region from query
	spanStatus.regionCount = 10
	checker.UpdateStatus(replica)

	// Test check results - should return region split
	results := checker.Check(10)
	require.Len(t, results, 1)
	require.Equal(t, replica, results.([]DefaultSpanSplitCheckResult)[0].Span)
}

func TestDefaultSpanSplitChecker_Check_BatchLimit(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	// Add multiple replicas
	for i := 0; i < 5; i++ {
		replica := createTestSpanReplication(cfID, int64(i))

		checker.AddReplica(replica)

		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 1500,
		}
		replica.UpdateStatus(status)

		// Set high traffic score
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 5
		checker.splitReadyTasks[replica.ID] = spanStatus
	}

	// Test batch limit
	results := checker.Check(3)
	require.Len(t, results, 3) // Should respect batch limit
}

func TestDefaultSpanSplitChecker_Check_EmptyResults(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	// Test empty results when no split ready tasks
	results := checker.Check(10)
	require.Len(t, results, 0)

	// Add replica but don't meet split conditions
	replica := createTestSpanReplication(cfID, 1)

	checker.AddReplica(replica)

	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
	}
	replica.UpdateStatus(status)

	checker.UpdateStatus(replica)

	results = checker.Check(10)
	require.Len(t, results, 0)
}

func TestDefaultSpanSplitChecker_Stat(t *testing.T) {
	testutil.SetUpTestServices()

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspace)
	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold: 1000,
		RegionThreshold:   5,
	}

	checker := NewDefaultSpanSplitChecker(cfID, schedulerCfg)

	// Test empty stat
	stat := checker.Stat()
	require.Equal(t, "", stat)

	// Add split ready tasks
	replica1 := createTestSpanReplication(cfID, 1)

	replica2 := createTestSpanReplication(cfID, 2)

	checker.AddReplica(replica1)
	checker.AddReplica(replica2)

	// Set up split ready status
	spanStatus1 := checker.allTasks[replica1.ID]
	spanStatus1.trafficScore = 5
	spanStatus1.regionCount = 8

	spanStatus2 := checker.allTasks[replica2.ID]
	spanStatus2.trafficScore = 3
	spanStatus2.regionCount = 6

	checker.splitReadyTasks[replica1.ID] = spanStatus1
	checker.splitReadyTasks[replica2.ID] = spanStatus2

	// Test stat output
	stat = checker.Stat()
	require.Contains(t, stat, replica1.ID.String())
	require.Contains(t, stat, replica2.ID.String())
	require.Contains(t, stat, "trafficScore: 5")
	require.Contains(t, stat, "trafficScore: 3")
	require.Contains(t, stat, "regionCount: 8")
	require.Contains(t, stat, "regionCount: 6")
	require.Contains(t, stat, "];")
}
