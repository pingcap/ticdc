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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

func init() {
	SetEasyThresholdForTest()
	log.SetLevel(zap.DebugLevel)
}

// count should be in [1, 10000]
func splitTableSpanIntoMultiple(spanA *heartbeatpb.TableSpan, count int) []*heartbeatpb.TableSpan {
	if count < 1 || count > 10000 {
		log.Panic("invalid count", zap.Int("count", count))
	}
	spans := make([]*heartbeatpb.TableSpan, 0, count)
	for i := 0; i < count; i++ {
		// Use numeric suffix to support up to 10000 spans
		// Convert i to bytes to handle large numbers properly
		suffix := make([]byte, 2)
		suffix[0] = byte(i >> 8)   // High byte
		suffix[1] = byte(i & 0xFF) // Low byte

		startKey := append(append([]byte{}, spanA.StartKey...), suffix...)
		nextSuffix := make([]byte, 2)
		nextSuffix[0] = byte((i + 1) >> 8)   // High byte
		nextSuffix[1] = byte((i + 1) & 0xFF) // Low byte
		endKey := append(append([]byte{}, spanA.StartKey...), nextSuffix...)

		if i == 0 {
			startKey = spanA.StartKey
		}
		if i == count-1 {
			endKey = spanA.EndKey
		}
		spans = append(spans, &heartbeatpb.TableSpan{
			TableID:  spanA.TableID,
			StartKey: startKey,
			EndKey:   endKey,
		})
	}

	return spans
}

// createTestSplitSpanReplication creates span replications for testing
// Simulates already-split spans from a table
func createTestSplitSpanReplications(cfID common.ChangeFeedID, tableID int64, spansNum int) []*SpanReplication {
	totalSpan := common.TableIDToComparableSpan(common.DefaultKeyspaceID, tableID)
	spans := splitTableSpanIntoMultiple(&totalSpan, spansNum)
	replicas := make([]*SpanReplication, 0, len(spans))
	for _, span := range spans {
		replica := NewSpanReplication(cfID, common.NewDispatcherID(), tableID, span, 1, common.DefaultMode, false)
		replicas = append(replicas, replica)
	}

	return replicas
}

func TestSplitTableSpanIntoMultiple_Properties(t *testing.T) {
	spanA := common.TableIDToComparableSpan(common.DefaultKeyspaceID, 100)
	count := 10000
	spans := splitTableSpanIntoMultiple(&spanA, count)
	require.Len(t, spans, count)
	// 1. First span's startKey == spanA.StartKey
	require.Equal(t, spanA.StartKey, spans[0].StartKey)
	// 2. Last span's endKey == spanA.EndKey
	require.Equal(t, spanA.EndKey, spans[count-1].EndKey)
	for i := 0; i < count; i++ {
		span := spans[i]
		// 3. startKey < endKey (lexicographically)
		require.Less(t, string(span.StartKey), string(span.EndKey), "span %d: startKey >= endKey", i)
		if i > 0 {
			// 4. previous endKey == current startKey
			require.Equal(t, spans[i-1].EndKey, span.StartKey, "span %d: previous endKey != current startKey", i)
		}
	}
}

func TestSplitSpanChecker_AddReplica(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	// Test adding single replica
	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	require.Len(t, replicas, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	for _, replica := range replicas {
		checker.AddReplica(replica)
	}
	require.Len(t, checker.allTasks, len(replicas))
	for _, replica := range replicas {
		require.Contains(t, checker.allTasks, replica.ID)
	}

	status, exists := checker.allTasks[replicas[0].ID]
	require.True(t, exists)
	require.Equal(t, replicas[0], status.SpanReplication)
	require.Equal(t, 0, status.trafficScore)
	require.Equal(t, 0, status.regionCount)
	require.NotNil(t, status.lastThreeTraffic)
	require.Len(t, status.lastThreeTraffic, 3)
}

func TestSplitSpanChecker_RemoveReplica(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas first
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}
	require.Len(t, checker.allTasks, len(replicas))

	// Remove existing replica
	checker.RemoveReplica(replicas[0])
	require.Len(t, checker.allTasks, len(replicas)-1)
	require.NotContains(t, checker.allTasks, replicas[0].ID)
	require.Contains(t, checker.allTasks, replicas[1].ID)
	require.Contains(t, checker.allTasks, replicas[2].ID)

	// Remove non-existing replica (no-op)
	nonExistingReplica := NewSpanReplication(cfID, common.NewDispatcherID(), 100000, &heartbeatpb.TableSpan{
		TableID:  100000,
		StartKey: []byte{1},
		EndKey:   []byte{2},
	}, 1, common.DefaultMode, false)
	checker.RemoveReplica(nonExistingReplica)
	require.Len(t, checker.allTasks, len(replicas)-1) // Should not change

	// Remove all remaining replicas
	checker.RemoveReplica(replicas[1])
	checker.RemoveReplica(replicas[2])
	require.Len(t, checker.allTasks, 0)
}

func TestSplitSpanChecker_UpdateStatus_Traffic(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	replicas := createTestSplitSpanReplications(cfID, 100000, 1)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	replica := replicas[0]
	checker.AddReplica(replica)

	// Test low traffic scenario (score reset)
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
	require.Equal(t, 500.0, spanStatus.lastThreeTraffic[0])

	// Test high traffic scenario (score accumulation)
	status.EventSizePerSecond = 1500 // Above threshold
	replica.UpdateStatus(status)

	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 1, spanStatus.trafficScore)
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[0])
	require.Equal(t, 500.0, spanStatus.lastThreeTraffic[1])

	// Test traffic score accumulation
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 2, spanStatus.trafficScore)
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[0])
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[1])
	require.Equal(t, 500.0, spanStatus.lastThreeTraffic[2])

	// Test traffic score threshold crossing
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 3, spanStatus.trafficScore)

	// Test zero traffic handling
	status.EventSizePerSecond = 0
	replica.UpdateStatus(status)
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 3, spanStatus.trafficScore) // Should not change for zero traffic
	// When EventSizePerSecond is 0, lastThreeTraffic is not updated
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[0])

	// Test last three traffic history updates
	status.EventSizePerSecond = 2000
	replica.UpdateStatus(status)
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 4, spanStatus.trafficScore)
	require.Equal(t, 2000.0, spanStatus.lastThreeTraffic[0])
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[1])
	require.Equal(t, 1500.0, spanStatus.lastThreeTraffic[2])
}

func TestSplitSpanChecker_UpdateStatus_Region(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       5,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	replicas := createTestSplitSpanReplications(cfID, 100000, 1)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	replica := replicas[0]
	checker.AddReplica(replica)

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
	mockCache.SetRegions(fmt.Sprintf("%s-%s", replicas[0].Span.StartKey, replicas[0].Span.EndKey), mockRegions)

	// Set region check time to force update
	spanStatus := checker.allTasks[replica.ID]
	spanStatus.regionCheckTime = time.Now().Add(-2 * regionCheckInterval) // Force region check

	status := &heartbeatpb.TableSpanStatus{
		ID:                 replica.ID.ToPB(),
		ComponentStatus:    heartbeatpb.ComponentState_Working,
		EventSizePerSecond: 500,
		CheckpointTs:       1,
	}
	replica.UpdateStatus(status)

	// Test region count above threshold
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 6, spanStatus.regionCount)

	// Test region check interval enforcement
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 6, spanStatus.regionCount) // Should not change due to time interval

	// Test region count below threshold
	mockRegions = []*tikv.Region{
		testutil.MockRegionWithID(1),
		testutil.MockRegionWithID(2),
		testutil.MockRegionWithID(3),
	}
	mockCache.SetRegions(fmt.Sprintf("%s-%s", replicas[0].Span.StartKey, replicas[0].Span.EndKey), mockRegions)

	spanStatus.regionCheckTime = time.Now().Add(-2 * regionCheckInterval)

	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 3, spanStatus.regionCount)
}

func TestSplitSpanChecker_UpdateStatus_NonWorking(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       5,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	replicas := createTestSplitSpanReplications(cfID, 100000, 1)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	replica := replicas[0]
	checker.AddReplica(replica)

	// Test non-working status handling
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
	require.Equal(t, 0.0, spanStatus.lastThreeTraffic[0])

	// Test with other non-working statuses
	status.ComponentStatus = heartbeatpb.ComponentState_Stopped
	replica.UpdateStatus(status)
	checker.UpdateStatus(replica)
	spanStatus = checker.allTasks[replica.ID]
	require.Equal(t, 0, spanStatus.trafficScore) // Should still not update
}

func TestSplitSpanChecker_ChooseSplitSpans_Traffic(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up different traffic scores for spans
	// First span: traffic score = 4 (above threshold)
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 4
	spanStatus1.lastThreeTraffic = []float64{1500, 1500, 1500}

	// Second span: traffic score = 2 (below threshold)
	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 2
	spanStatus2.lastThreeTraffic = []float64{800, 800, 800}

	// Third span: traffic score = 1 (below threshold)
	spanStatus3 := checker.allTasks[replicas[2].ID]
	spanStatus3.trafficScore = 1
	spanStatus3.lastThreeTraffic = []float64{600, 600, 600}

	// Set node IDs for traffic calculation
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node2")
	replicas[2].SetNodeID("node1")

	// Test split decision
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[0], splitResult.SplitSpan)
}

func TestSplitSpanChecker_ChooseSplitSpans_Region(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       5,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 2)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up different region counts for spans
	// First span: region count = 8 (above threshold)
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.regionCount = 8
	spanStatus1.trafficScore = 0
	spanStatus1.lastThreeTraffic = []float64{500, 500, 500}

	// Second span: region count = 3 (below threshold)
	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.regionCount = 3
	spanStatus2.trafficScore = 0
	spanStatus2.lastThreeTraffic = []float64{400, 400, 400}

	// Set node IDs
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node2")

	// Test split decision
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[0], splitResult.SplitSpan)
}

func TestSplitSpanChecker_CheckMergeWhole_SingleNode(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     2000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set all spans on single node with low traffic and region count
	for _, replica := range replicas {
		replica.SetNodeID("node1")
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 2
		spanStatus.lastThreeTraffic = []float64{300, 300, 300}
	}

	// Test merge whole decision
	results := checker.Check(10)
	require.Len(t, results, 1)

	mergeResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpMerge, mergeResult.OpType)
	require.Len(t, mergeResult.MergeSpans, 3)
	require.Contains(t, mergeResult.MergeSpans, replicas[0])
	require.Contains(t, mergeResult.MergeSpans, replicas[1])
	require.Contains(t, mergeResult.MergeSpans, replicas[2])
}

func TestSplitSpanChecker_CheckMergeWhole_MultiNode(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     2000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")
	nodeManager.GetAliveNodes()["node3"] = node.NewInfo("node3", "127.0.0.1:8302")

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set spans distributed across multiple nodes with low traffic and region count
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node2")
	replicas[2].SetNodeID("node3")

	for _, replica := range replicas {
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 2
		spanStatus.lastThreeTraffic = []float64{300, 300, 300}
	}

	// Test merge whole decision - should move to single node then merge
	results := checker.Check(10)
	require.Len(t, results, 1)

	moveResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpMove, moveResult.OpType)
	require.Len(t, moveResult.MoveSpans, 2)
}

func TestSplitSpanChecker_CheckMergeWhole_ThresholdNotMet(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")

	replicas := createTestSplitSpanReplications(cfID, 100000, 2)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set spans with some exceeding half threshold
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")

	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.regionCount = 2
	spanStatus1.lastThreeTraffic = []float64{300, 300, 300}

	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.regionCount = 6
	spanStatus2.lastThreeTraffic = []float64{600, 600, 600}

	// Test merge whole decision - should skip merge due to threshold
	results := checker.Check(10)
	require.Len(t, results, 0) // No merge operation
}

func TestSplitSpanChecker_CheckBalanceTraffic_Balance(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 4)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up imbalanced traffic across nodes
	// Node1: high traffic (1200 + 800 = 2000)
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.lastThreeTraffic = []float64{1200, 1200, 1200}

	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.lastThreeTraffic = []float64{800, 800, 800}

	// Node2: low traffic (200 + 100 = 300)
	replicas[2].SetNodeID("node2")
	replicas[3].SetNodeID("node2")
	spanStatus3 := checker.allTasks[replicas[2].ID]
	spanStatus3.trafficScore = 0
	spanStatus3.lastThreeTraffic = []float64{200, 200, 200}
	spanStatus4 := checker.allTasks[replicas[3].ID]
	spanStatus4.trafficScore = 0
	spanStatus4.lastThreeTraffic = []float64{100, 100, 100}

	// Set region counts
	for _, spanStatus := range []*splitSpanStatus{spanStatus1, spanStatus2, spanStatus3, spanStatus4} {
		spanStatus.regionCount = 3
		spanStatus.GetStatus().CheckpointTs = oracle.ComposeTS(int64(time.Now().Add(-10*time.Second).UnixMilli()), 0)
	}
	checker.balanceCondition.statusUpdated = true

	// Test traffic balance decision
	results := checker.Check(10)
	require.Len(t, results, 1)

	moveResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpMove, moveResult.OpType)
	require.Equal(t, "node2", string(moveResult.TargetNode))
	require.Len(t, moveResult.MoveSpans, 1)
	require.True(t, moveResult.MoveSpans[0] == spanStatus2.SpanReplication)
}

func TestSplitSpanChecker_CheckBalanceTraffic_NoBalanceNeeded(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 2)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up balanced traffic across nodes
	// Node1: moderate traffic (500)
	replicas[0].SetNodeID("node1")
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.lastThreeTraffic = []float64{500, 500, 500}

	// Node2: moderate traffic (500)
	replicas[1].SetNodeID("node2")
	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.lastThreeTraffic = []float64{500, 500, 500}

	// Set region counts
	spanStatus1.regionCount = 5
	spanStatus2.regionCount = 5

	// Test traffic balance decision - should not need balance
	results := checker.Check(10)
	require.Len(t, results, 0) // No balance operation needed
}

func TestSplitSpanChecker_CheckBalanceTraffic_SplitIfNoMove(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 2)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up imbalanced traffic but no suitable spans for movement
	// Node1: high traffic (1500)
	replicas[0].SetNodeID("node1")
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.lastThreeTraffic = []float64{1500, 1500, 1500}

	// Node2: low traffic (100)
	replicas[1].SetNodeID("node2")
	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.lastThreeTraffic = []float64{100, 100, 100}

	// Set region counts
	spanStatus1.regionCount = 5
	spanStatus1.GetStatus().CheckpointTs = oracle.ComposeTS(int64(time.Now().Add(-10*time.Second).UnixMilli()), 0)
	spanStatus2.regionCount = 5
	spanStatus2.GetStatus().CheckpointTs = oracle.ComposeTS(int64(time.Now().Add(-10*time.Second).UnixMilli()), 0)

	checker.balanceCondition.statusUpdated = true

	// Test traffic balance decision - should split span from max traffic node
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[0], splitResult.SplitSpan) // Split the span from max traffic node
}

func TestSplitSpanChecker_CheckBalanceTraffic_SingleNode(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")

	replicas := createTestSplitSpanReplications(cfID, 100000, 2)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set all spans on single node
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")

	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.lastThreeTraffic = []float64{500, 500, 500}

	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.lastThreeTraffic = []float64{300, 300, 300}

	// Set region counts
	spanStatus1.regionCount = 5
	spanStatus2.regionCount = 5

	// Test traffic balance decision - should not need balance for single node
	results := checker.Check(10)
	require.Len(t, results, 0) // No balance operation needed
}

func TestSplitSpanChecker_CheckBalanceTraffic_TrafficFluctuation(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       0,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 4)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up scenario with high traffic fluctuation
	// Node1: spans with fluctuating traffic
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")
	spanStatus1 := checker.allTasks[replicas[0].ID]
	spanStatus1.trafficScore = 0
	// High fluctuation: latest traffic is very high, but previous traffic was low
	spanStatus1.lastThreeTraffic = []float64{2000, 300, 200} // Latest: 2000, Previous: 300, 200
	spanStatus2 := checker.allTasks[replicas[1].ID]
	spanStatus2.trafficScore = 0
	// High fluctuation: latest traffic is very high, but previous traffic was low
	spanStatus2.lastThreeTraffic = []float64{1800, 250, 150} // Latest: 1800, Previous: 250, 150

	// Node2: spans with fluctuating traffic
	replicas[2].SetNodeID("node2")
	replicas[3].SetNodeID("node2")
	spanStatus3 := checker.allTasks[replicas[2].ID]
	spanStatus3.trafficScore = 0
	// High fluctuation: latest traffic is very low, but previous traffic was high
	spanStatus3.lastThreeTraffic = []float64{100, 1800, 1600} // Latest: 100, Previous: 1800, 1600
	spanStatus4 := checker.allTasks[replicas[3].ID]
	spanStatus4.trafficScore = 0
	// High fluctuation: latest traffic is very low, but previous traffic was high
	spanStatus4.lastThreeTraffic = []float64{50, 1500, 1400} // Latest: 50, Previous: 1500, 1400

	// Calculate expected traffic distribution:
	// Node1 total: 2000 + 1800 = 3800 (latest), 300 + 250 = 550 (previous), 200 + 150 = 350 (oldest)
	// Node2 total: 100 + 50 = 150 (latest), 1800 + 1500 = 3300 (previous), 1600 + 1400 = 3000 (oldest)
	// Total: 3800 + 150 = 3950 (latest), 550 + 3300 = 3850 (previous), 350 + 3000 = 3350 (oldest)
	// Average: 3950/2 = 1975 (latest), 3850/2 = 1925 (previous), 3350/2 = 1675 (oldest)
	//
	// Node1 vs Average (latest): 3800 vs 1975 (192% of average) - should trigger balance
	// Node2 vs Average (latest): 150 vs 1975 (7.6% of average) - should trigger balance
	//
	// However, due to fluctuation check:
	// Node1 vs Average (previous): 550 vs 1925 (28.6% of average) - below 80% threshold
	// Node2 vs Average (previous): 3300 vs 1925 (171% of average) - above 120% threshold
	// This means the traffic pattern is fluctuating, so no balance should be performed

	// Test traffic balance decision - should not balance due to traffic fluctuation
	results := checker.Check(10)
	require.Len(t, results, 0) // No balance operation due to traffic fluctuation

	// Verify the traffic calculation logic
	// The checker should detect that traffic is fluctuating and avoid unnecessary scheduling
	// even though the latest traffic shows significant imbalance
}

func TestSplitSpanChecker_ChooseMergedSpans_LargeLag(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set all spans on same node with low traffic and region count (below 3/4 threshold)
	for _, replica := range replicas {
		replica.SetNodeID("node1")
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 3                             // Below 3/4 threshold (15)
		spanStatus.lastThreeTraffic = []float64{200, 200, 200} // Below 3/4 threshold (750)
	}

	// Test local merge decision
	results := checker.Check(10)
	require.Len(t, results, 0)
}

func TestSplitSpanChecker_ChooseMergedSpans_Continuous(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")

	replicas := createTestSplitSpanReplications(cfID, 100000, 3)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set all spans on same node with low traffic and region count (below 3/4 threshold)
	for _, replica := range replicas {
		replica.SetNodeID("node1")
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 3                             // Below 3/4 threshold (15)
		spanStatus.lastThreeTraffic = []float64{200, 200, 200} // Below 3/4 threshold (750)
	}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 200,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test local merge decision
	results := checker.Check(10)
	require.Len(t, results, 1)

	mergeResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpMerge, mergeResult.OpType)
	require.Len(t, mergeResult.MergeSpans, 3)
	require.Contains(t, mergeResult.MergeSpans, replicas[0])
	require.Contains(t, mergeResult.MergeSpans, replicas[1])
	require.Contains(t, mergeResult.MergeSpans, replicas[2])
}

func TestSplitSpanChecker_ChooseMoveSpans_SimpleMove(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 5)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up scenario where spans can be moved for merge preparation
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node2")
	replicas[2].SetNodeID("node1")
	replicas[3].SetNodeID("node2")
	replicas[4].SetNodeID("node1")

	for idx, replica := range replicas {
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 3
		switch idx / 2 {
		case 0:
			spanStatus.lastThreeTraffic = []float64{500, 500, 500}
		case 1:
			spanStatus.lastThreeTraffic = []float64{40, 40, 40}
		case 2:
			spanStatus.lastThreeTraffic = []float64{100, 100, 100}
		}
	}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 200,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test move decision for merge preparation
	results := checker.Check(10)
	require.Len(t, results, 1)

	moveResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpMove, moveResult.OpType)
	require.Equal(t, "node2", string(moveResult.TargetNode))
	require.Len(t, moveResult.MoveSpans, 1)
	require.Equal(t, replicas[2], moveResult.MoveSpans[0])
}

func TestSplitSpanChecker_ChooseMoveSpans_ExchangeMove(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       20,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 6)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node2")
	replicas[2].SetNodeID("node1")
	replicas[3].SetNodeID("node2")
	replicas[4].SetNodeID("node1")
	replicas[5].SetNodeID("node2")

	for _, replica := range replicas {
		spanStatus := checker.allTasks[replica.ID]
		spanStatus.trafficScore = 0
		spanStatus.regionCount = 3
		spanStatus.lastThreeTraffic = []float64{100, 100, 100}
	}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 100,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test exchange move decision
	results := checker.Check(10)
	require.Len(t, results, 4)

	// Verify exchange moves
	moveResults := results.([]SplitSpanCheckResult)
	for _, result := range moveResults {
		require.Equal(t, OpMove, result.OpType)
	}
}

func TestSplitSpanChecker_Check_FullFlow(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 4)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up complex scenario with multiple operation types
	// Node1: spans 0 and 1 (high traffic, need split)
	// Node2: spans 2 and 3 (low traffic, can be merged)
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")
	replicas[2].SetNodeID("node2")
	replicas[3].SetNodeID("node2")

	// Set high traffic for spans that need splitting
	spanStatus0 := checker.allTasks[replicas[0].ID]
	spanStatus0.trafficScore = 4 // Above threshold
	spanStatus0.regionCount = 5
	spanStatus0.lastThreeTraffic = []float64{1500, 1500, 1500}

	spanStatus1 := checker.allTasks[replicas[1].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.regionCount = 5
	spanStatus1.lastThreeTraffic = []float64{800, 800, 800}

	// Set low traffic for spans that can be merged
	spanStatus2 := checker.allTasks[replicas[2].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.regionCount = 3
	spanStatus2.lastThreeTraffic = []float64{300, 300, 300}

	spanStatus3 := checker.allTasks[replicas[3].ID]
	spanStatus3.trafficScore = 0
	spanStatus3.regionCount = 3
	spanStatus3.lastThreeTraffic = []float64{200, 200, 200}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 200,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test full workflow - should prioritize split over merge
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[0], splitResult.SplitSpan)
}

func TestSplitSpanChecker_Check_FullFlow_WriteThresholdZero(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     0, // No write threshold
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 4)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up scenario where only region threshold matters
	// Node1: spans 0 and 1 (low region count)
	// Node2: spans 2 and 3 (high region count, need split)
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")
	replicas[2].SetNodeID("node2")
	replicas[3].SetNodeID("node2")

	// Set high region count for spans that need splitting
	spanStatus0 := checker.allTasks[replicas[0].ID]
	spanStatus0.trafficScore = 0
	spanStatus0.regionCount = 3 // Above threshold
	spanStatus0.lastThreeTraffic = []float64{500, 500, 500}

	spanStatus1 := checker.allTasks[replicas[1].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.regionCount = 3
	spanStatus1.lastThreeTraffic = []float64{300, 300, 300}

	// Set low region count for spans that can be merged
	spanStatus2 := checker.allTasks[replicas[2].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.regionCount = 15
	spanStatus2.lastThreeTraffic = []float64{200, 200, 200}

	spanStatus3 := checker.allTasks[replicas[3].ID]
	spanStatus3.trafficScore = 0
	spanStatus3.regionCount = 5
	spanStatus3.lastThreeTraffic = []float64{100, 100, 100}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 200,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test full workflow with only region threshold - should prioritize region split over merge
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[2], splitResult.SplitSpan)
}

func TestSplitSpanChecker_Check_FullFlow_RegionThresholdZero(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       0, // No region threshold
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, 4)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker
	for _, replica := range replicas {
		checker.AddReplica(replica)
	}

	// Set up scenario where only traffic threshold matters
	// Node1: spans 0 and 1 (high traffic, need split)
	// Node2: spans 2 and 3 (low traffic, can be merged)
	replicas[0].SetNodeID("node1")
	replicas[1].SetNodeID("node1")
	replicas[2].SetNodeID("node2")
	replicas[3].SetNodeID("node2")

	// Set high traffic for spans that need splitting
	spanStatus0 := checker.allTasks[replicas[0].ID]
	spanStatus0.trafficScore = 4 // Above threshold
	spanStatus0.regionCount = 3
	spanStatus0.lastThreeTraffic = []float64{1500, 1500, 1500}

	spanStatus1 := checker.allTasks[replicas[1].ID]
	spanStatus1.trafficScore = 0
	spanStatus1.regionCount = 3
	spanStatus1.lastThreeTraffic = []float64{800, 800, 800}

	// Set low traffic for spans that can be merged
	spanStatus2 := checker.allTasks[replicas[2].ID]
	spanStatus2.trafficScore = 0
	spanStatus2.regionCount = 3
	spanStatus2.lastThreeTraffic = []float64{300, 300, 300}

	spanStatus3 := checker.allTasks[replicas[3].ID]
	spanStatus3.trafficScore = 0
	spanStatus3.regionCount = 3
	spanStatus3.lastThreeTraffic = []float64{200, 200, 200}

	// Set low lag scenario
	currentTime := time.Now()
	for _, replica := range replicas {
		status := &heartbeatpb.TableSpanStatus{
			ID:                 replica.ID.ToPB(),
			ComponentStatus:    heartbeatpb.ComponentState_Working,
			EventSizePerSecond: 0,
			CheckpointTs:       oracle.ComposeTS(int64(currentTime.Add(-10*time.Second).UnixMilli()), 0),
		}
		replica.UpdateStatus(status)
	}

	// Test full workflow with only traffic threshold - should prioritize traffic split over merge
	results := checker.Check(10)
	require.Len(t, results, 1)

	splitResult := results.([]SplitSpanCheckResult)[0]
	require.Equal(t, OpSplit, splitResult.OpType)
	require.Equal(t, replicas[0], splitResult.SplitSpan)
}

func TestSplitSpanChecker_Check_PerformanceWithManySpans(t *testing.T) {
	testutil.SetUpTestServices()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)

	spanCount := 10000
	regionCount := 3
	traffic := 100.0

	schedulerCfg := &config.ChangefeedSchedulerConfig{
		WriteKeyThreshold:     1000,
		RegionThreshold:       10,
		BalanceScoreThreshold: 1,
		MinTrafficPercentage:  0.8,
		MaxTrafficPercentage:  1.2,
	}

	nodeIDs := []node.ID{"node1", "node2"}
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodeManager.GetAliveNodes()["node1"] = node.NewInfo("node1", "127.0.0.1:8300")
	nodeManager.GetAliveNodes()["node2"] = node.NewInfo("node2", "127.0.0.1:8301")

	replicas := createTestSplitSpanReplications(cfID, 100000, spanCount)
	groupID := replicas[0].GetGroupID()
	checker := NewSplitSpanChecker(cfID, groupID, schedulerCfg)

	// Add replicas to checker, set traffic and regionCount, assign nodeID alternately
	for i, replica := range replicas {
		checker.AddReplica(replica)
		status := checker.allTasks[replica.ID]
		status.lastThreeTraffic = []float64{traffic, traffic, traffic}
		status.regionCount = regionCount
		replica.SetNodeID(nodeIDs[i%2])
	}

	start := time.Now()
	_ = checker.Check(100)
	duration := time.Since(start)

	if duration > time.Second {
		t.Fatalf("Check took too long: %v", duration)
	}
}
