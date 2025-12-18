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
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const latestTrafficIndex = 0

var (
	minTrafficBalanceThreshold         = float64(1024 * 1024) // 1MB
	maxMoveSpansCountForTrafficBalance = 4
	maxMoveSpansCountForMerge          = 16
	maxLagThreshold                    = float64(30) // 30s
	mergeThreshold                     = 5
)

type BalanceCause string

const (
	BalanceCauseByMinNode BalanceCause = "minNode"
	BalanceCauseByMaxNode BalanceCause = "maxNode"
	BalanceCauseByBoth    BalanceCause = "both"
	BalanceCauseNone      BalanceCause = "none"
)

// BalanceCondition is the condition of we need to balance the traffic of the span
// only when the balanceScore exceed the threshold, we will consider to balance the traffic of the span
// Only when the min/max traffic NodeID is keep the same, we will increase the balanceScore
// Otherwise, we will reset the balanceScore to 0
type BalanceCondition struct {
	minTrafficNodeID node.ID
	maxTrafficNodeID node.ID
	balanceScore     int
	balanceCause     BalanceCause
	statusUpdated    bool
}

func (b *BalanceCondition) reset() {
	b.minTrafficNodeID = ""
	b.maxTrafficNodeID = ""
	b.balanceScore = 0
	b.balanceCause = BalanceCauseNone
	b.statusUpdated = false
}

func (b *BalanceCondition) initFirstScore(
	minTrafficNodeID node.ID,
	maxTrafficNodeID node.ID,
	balanceCauseByMinNode bool,
	balanceCauseByMaxNode bool,
) {
	b.reset()
	b.balanceScore = 1
	if balanceCauseByMaxNode && balanceCauseByMinNode {
		b.balanceCause = BalanceCauseByBoth
	} else if balanceCauseByMaxNode {
		b.balanceCause = BalanceCauseByMaxNode
	} else if balanceCauseByMinNode {
		b.balanceCause = BalanceCauseByMinNode
	}
	b.minTrafficNodeID = minTrafficNodeID
	b.maxTrafficNodeID = maxTrafficNodeID
}

func (b *BalanceCondition) updateScore(minTrafficNodeID node.ID,
	maxTrafficNodeID node.ID,
	balanceCauseByMinNode bool,
	balanceCauseByMaxNode bool,
) {
	if b.balanceScore == 0 {
		b.initFirstScore(minTrafficNodeID, maxTrafficNodeID, balanceCauseByMinNode, balanceCauseByMaxNode)
	} else {
		if b.balanceCause == BalanceCauseByBoth {
			if b.minTrafficNodeID == minTrafficNodeID && b.maxTrafficNodeID == maxTrafficNodeID {
				b.balanceScore += 1
			} else if b.minTrafficNodeID == minTrafficNodeID {
				b.balanceScore += 1
				b.balanceCause = BalanceCauseByMinNode
			} else if b.maxTrafficNodeID == maxTrafficNodeID {
				b.balanceScore += 1
				b.balanceCause = BalanceCauseByMaxNode
			} else {
				b.initFirstScore(minTrafficNodeID, maxTrafficNodeID, balanceCauseByMinNode, balanceCauseByMaxNode)
			}
		} else if b.balanceCause == BalanceCauseByMaxNode {
			if b.maxTrafficNodeID == maxTrafficNodeID {
				b.balanceScore += 1
			} else {
				b.initFirstScore(minTrafficNodeID, maxTrafficNodeID, balanceCauseByMinNode, balanceCauseByMaxNode)
			}
		} else if b.balanceCause == BalanceCauseByMinNode {
			if b.minTrafficNodeID == minTrafficNodeID {
				b.balanceScore += 1
			} else {
				b.initFirstScore(minTrafficNodeID, maxTrafficNodeID, balanceCauseByMinNode, balanceCauseByMaxNode)
			}
		}
	}
	b.statusUpdated = false // reset
}

type SplitSpanChecker struct {
	changefeedID common.ChangeFeedID
	groupID      replica.GroupID
	allTasks     map[common.DispatcherID]*splitSpanStatus

	// when writeThreshold is 0, we don't check the traffic
	writeThreshold int
	// when regionThreshold is 0, we don't check the region count
	regionThreshold int

	balanceScoreThreshold int
	minTrafficPercentage  float64
	maxTrafficPercentage  float64

	balanceCondition BalanceCondition

	mergeThreshold  int
	mergeCheckCount int

	nodeManager *watcher.NodeManager
	pdClock     pdutil.Clock

	refresher              *RegionCountRefresher
	splitSpanCheckDuration prometheus.Observer
}

type splitSpanStatus struct {
	*SpanReplication

	trafficScore int
	// record the traffic of the span for last three times
	// idx = 0 is the latest traffic
	lastThreeTraffic []float64

	regionCount int
}

func NewSplitSpanChecker(
	changefeedID common.ChangeFeedID,
	groupID replica.GroupID,
	schedulerCfg *config.ChangefeedSchedulerConfig,
	refresher *RegionCountRefresher,
) *SplitSpanChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	return &SplitSpanChecker{
		changefeedID:           changefeedID,
		groupID:                groupID,
		allTasks:               make(map[common.DispatcherID]*splitSpanStatus),
		writeThreshold:         util.GetOrZero(schedulerCfg.WriteKeyThreshold),
		regionThreshold:        util.GetOrZero(schedulerCfg.RegionThreshold),
		balanceScoreThreshold:  util.GetOrZero(schedulerCfg.BalanceScoreThreshold),
		minTrafficPercentage:   util.GetOrZero(schedulerCfg.MinTrafficPercentage),
		maxTrafficPercentage:   util.GetOrZero(schedulerCfg.MaxTrafficPercentage),
		mergeThreshold:         mergeThreshold,
		mergeCheckCount:        0,
		nodeManager:            appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		pdClock:                appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		splitSpanCheckDuration: metrics.SplitSpanCheckDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), replica.GetGroupName(groupID)),

		refresher: refresher,
	}
}

func (s *SplitSpanChecker) AddReplica(replica *SpanReplication) {
	s.allTasks[replica.ID] = &splitSpanStatus{
		SpanReplication:  replica,
		regionCount:      0,
		trafficScore:     0,
		lastThreeTraffic: make([]float64, 3),
	}

	if s.regionThreshold > 0 {
		s.refresher.addDispatcher(context.Background(), replica.ID, replica.Span)
	}
}

func (s *SplitSpanChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)

	if s.regionThreshold > 0 {
		s.refresher.removeDispatcher(replica.ID)
	}
}

func (s *SplitSpanChecker) UpdateStatus(replica *SpanReplication) {
	status, ok := s.allTasks[replica.ID]
	if !ok {
		log.Warn("split span checker: replica not found", zap.String("changefeed", s.changefeedID.Name()), zap.String("replica", replica.ID.String()))
		return
	}
	if status.GetStatus().ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	// check traffic first
	// When there is totally no throughput, EventSizePerSecond will be 1 to distinguish from the status without eventSize.
	// So we don't need to special case of traffic = 0
	if status.GetStatus().EventSizePerSecond != 0 {
		if status.GetStatus().EventSizePerSecond < float32(s.writeThreshold) {
			status.trafficScore = 0
		} else {
			status.trafficScore++
			log.Debug("update traffic score",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Int64("group", s.groupID),
				zap.String("span", status.SpanReplication.ID.String()),
				zap.Any("trafficScore", status.trafficScore),
				zap.Any("eventSizePerSecond", status.GetStatus().EventSizePerSecond),
			)
		}

		status.lastThreeTraffic[2] = status.lastThreeTraffic[1]
		status.lastThreeTraffic[1] = status.lastThreeTraffic[0]
		status.lastThreeTraffic[0] = float64(status.GetStatus().EventSizePerSecond)
	}

	if s.regionThreshold > 0 {
		status.regionCount = s.refresher.getRegionCount(replica.ID)
	}

	s.balanceCondition.statusUpdated = true

	log.Debug("split span checker update status",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("replica", replica.ID),
		zap.Any("status", status.GetStatus()),
		zap.Int("status.regionCount", status.regionCount),
		zap.Int("status.trafficScore", status.trafficScore),
		zap.Any("status.lastThreeTraffic", status.lastThreeTraffic),
	)
}

type SplitSpanCheckResult struct {
	OpType OpType

	SplitSpan        *SpanReplication
	SplitTargetNodes []node.ID
	SpanNum          int
	SpanType         split.SplitType

	MergeSpans []*SpanReplication

	MoveSpans  []*SpanReplication
	TargetNode node.ID
}

func (s *SplitSpanChecker) checkAllTaskAvailableLocked() bool {
	for _, task := range s.allTasks {
		for _, traffic := range task.lastThreeTraffic {
			if traffic == 0 {
				return false
			}
		}
	}
	return true
}

// return some actions for scheduling the split spans
func (s *SplitSpanChecker) Check(batch int) replica.GroupCheckResult {
	start := time.Now()
	waitMerge := false
	defer func() {
		// if we don't wait for merge, we need to reset the merge check count
		if !waitMerge {
			s.mergeCheckCount = 0
		}
		s.splitSpanCheckDuration.Observe(time.Since(start).Seconds())
	}()
	log.Debug("SplitSpanChecker try to check",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("batch", batch))
	results := make([]SplitSpanCheckResult, 0)

	if !s.checkAllTaskAvailableLocked() {
		log.Debug("some task is not available, skip check",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Int64("group", int64(s.groupID)),
		)
		return results
	}

	aliveNodeIDs := s.nodeManager.GetAliveNodeIDs()

	lastThreeTrafficPerNode := make(map[node.ID][]float64)
	lastThreeTrafficSum := make([]float64, 3)
	// nodeID -> []*splitSpanStatus
	taskMap := make(map[node.ID][]*splitSpanStatus)

	for _, nodeID := range aliveNodeIDs {
		lastThreeTrafficPerNode[nodeID] = make([]float64, 3)
		taskMap[nodeID] = make([]*splitSpanStatus, 0)
	}

	// step1. check whether the split spans should be split again
	//        if a span's region count or traffic exceeds threshold, we should split it again
	results, totalRegionCount := s.chooseSplitSpans(lastThreeTrafficPerNode, lastThreeTrafficSum, taskMap)
	if len(results) > 0 {
		// If some spans need to split, we just return the results
		return results
	}

	// step2. check whether the whole dispatchers should be merged together.
	//        only when all spans' total region count and traffic are less then threshold/2, we can merge them together
	//        consider we only support to merge the spans in the same node, we first do move, then merge
	results = s.checkMergeWhole(totalRegionCount, lastThreeTrafficSum, lastThreeTrafficPerNode)
	if len(results) > 0 {
		return results
	}

	// step3. check the traffic of each node. If the traffic is not balanced,
	//        we try to move some spans from the node with max traffic to the node with min traffic
	results, minTrafficNodeID, maxTrafficNodeID := s.checkBalanceTraffic(aliveNodeIDs, lastThreeTrafficSum, lastThreeTrafficPerNode, taskMap)
	if len(results) > 0 {
		return results
	}

	// step4. check whether we need to do merge some spans.
	//        we can only merge spans when the lag is low.
	minCheckpointTs := uint64(math.MaxUint64)
	for _, status := range s.allTasks {
		if status.GetStatus().CheckpointTs < minCheckpointTs {
			minCheckpointTs = status.GetStatus().CheckpointTs
		}
	}

	pdTime := s.pdClock.CurrentTime()
	phyCkpTs := oracle.ExtractPhysical(minCheckpointTs)
	lag := float64(oracle.GetPhysical(pdTime)-phyCkpTs) / 1e3

	// only when the lag is less than 30s, we can consider to merge spans.
	if lag > maxLagThreshold {
		log.Debug("the lag for the group is too large, skip merge",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("groupID", s.groupID),
			zap.Float64("lag", lag),
		)
		return results
	}

	// if the span count is smaller than the upper limit, we don't need merge anymore.
	upperSpanCount := 0
	if s.writeThreshold > 0 {
		upperSpanCount = int(math.Ceil(lastThreeTrafficSum[latestTrafficIndex] / float64(s.writeThreshold)))
	}

	if s.regionThreshold > 0 {
		countByRegion := int(math.Ceil(float64(totalRegionCount) / float64(s.regionThreshold)))
		if countByRegion > upperSpanCount {
			upperSpanCount = int(countByRegion)
		}
	}

	// we have no need to make spans count too strict, it's ok for a small amount of spans.
	upperSpanCount = max(upperSpanCount, len(aliveNodeIDs)) * 2

	if upperSpanCount >= len(s.allTasks) {
		log.Debug("the span count is proper, so we don't need merge spans",
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Int64("groupID", s.groupID),
			zap.Float64("totalTraffic", lastThreeTrafficSum[0]),
			zap.Int("totalRegionCount", totalRegionCount),
			zap.Int("regionThreshold", s.regionThreshold),
			zap.Float32("writeThreshold", float32(s.writeThreshold)),
			zap.Int("spanCount", len(s.allTasks)),
		)
		return results
	}

	// use merge check count to avoid too frequent merge(when traffic frequent oscillations)
	waitMerge = true
	s.mergeCheckCount++
	if s.mergeCheckCount < s.mergeThreshold {
		return results
	}

	results, sortedSpanByStartKey := s.chooseMergedSpans(batch)
	if len(results) > 0 {
		return results
	}

	// step5. try to check whether we need move dispatchers to make merge possible
	return s.chooseMoveSpans(minTrafficNodeID, maxTrafficNodeID, sortedSpanByStartKey, lastThreeTrafficPerNode, taskMap)
}

// chooseMoveSpans finds multiple optimal span moves using a multi-priority search strategy:
// 1. Priority 1: Direct merge moves that maintain traffic balance
// 2. Priority 2: Merge moves with compensation to maintain balance
// 3. Priority 3: Pure traffic balance moves
// Returns multiple move plans sorted by priority and effectiveness
func (s *SplitSpanChecker) chooseMoveSpans(minTrafficNodeID node.ID, maxTrafficNodeID node.ID, sortedSpanByStartKey []*splitSpanStatus, lastThreeTrafficPerNode map[node.ID][]float64, taskMap map[node.ID][]*splitSpanStatus) []SplitSpanCheckResult {
	log.Debug("chooseMoveSpans try to choose move spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("minTrafficNodeID", minTrafficNodeID),
		zap.Any("maxTrafficNodeID", maxTrafficNodeID),
		zap.Int("totalSpans", len(sortedSpanByStartKey)))

	results := make([]SplitSpanCheckResult, 0)

	// If no any span in minTrafficNodeID, we random select one span from maxTrafficNodeID for it.
	if len(taskMap[minTrafficNodeID]) == 0 && len(taskMap[maxTrafficNodeID]) > 0 {
		randomSpan := taskMap[maxTrafficNodeID][rand.Intn(len(taskMap[maxTrafficNodeID]))]
		results = append(results, SplitSpanCheckResult{
			OpType: OpMove,
			MoveSpans: []*SpanReplication{
				randomSpan.SpanReplication,
			},
			TargetNode: minTrafficNodeID,
		})
		return results
	}

	// Build adjacency map for O(1) lookup of adjacent spans
	adjacencyMap := s.buildAdjacencyMap(sortedSpanByStartKey)

	// Try to find optimal span moves using multi-priority strategy
	if result := s.findOptimalSpanMoves(minTrafficNodeID, maxTrafficNodeID, sortedSpanByStartKey, adjacencyMap, lastThreeTrafficPerNode); len(result) > 0 {
		results = append(results, result...)
		log.Info("chooseMoveSpans found multiple move plans",
			zap.Any("changefeedID", s.changefeedID),
			zap.Any("groupID", s.groupID),
			zap.Int("moveCount", len(result)),
			zap.Any("moves", result))
		return results
	}

	return results
}

// buildAdjacencyMap builds a map from span ID to its adjacent spans for O(1) lookup
func (s *SplitSpanChecker) buildAdjacencyMap(sortedSpanByStartKey []*splitSpanStatus) map[common.DispatcherID][]*splitSpanStatus {
	adjacencyMap := make(map[common.DispatcherID][]*splitSpanStatus)

	for i, span := range sortedSpanByStartKey {
		adjacents := make([]*splitSpanStatus, 0, 2)

		// Check previous span
		if i > 0 {
			prev := sortedSpanByStartKey[i-1]
			if bytes.Equal(prev.Span.EndKey, span.Span.StartKey) {
				adjacents = append(adjacents, prev)
			}
		}

		// Check next span
		if i < len(sortedSpanByStartKey)-1 {
			next := sortedSpanByStartKey[i+1]
			if bytes.Equal(span.Span.EndKey, next.Span.StartKey) {
				adjacents = append(adjacents, next)
			}
		}

		adjacencyMap[span.ID] = adjacents
	}

	return adjacencyMap
}

// findOptimalSpanMoves finds optimal span moves with compensation if needed
// It tries to find multiple move plans instead of just the first one
func (s *SplitSpanChecker) findOptimalSpanMoves(
	minTrafficNodeID node.ID,
	maxTrafficNodeID node.ID,
	sortedSpanByStartKey []*splitSpanStatus,
	adjacencyMap map[common.DispatcherID][]*splitSpanStatus,
	lastThreeTrafficPerNode map[node.ID][]float64,
) []SplitSpanCheckResult {
	moveSpanTarget := make(map[common.DispatcherID]node.ID) // moveSpanTarget is used to store the target node for the spans ready to move

	// use to record the traffic after each move
	afterMoveTrafficPerNode := make(map[node.ID]float64)
	for nodeID := range lastThreeTrafficPerNode {
		afterMoveTrafficPerNode[nodeID] = lastThreeTrafficPerNode[nodeID][latestTrafficIndex]
	}

	// Track spans that have been selected to avoid duplicates（or spans need to be merged with the moved span, which should also not be moved again）
	selectedSpans := make(map[common.DispatcherID]bool)

	// Calculate current min and max traffic for balance check
	currentMinTraffic := lastThreeTrafficPerNode[minTrafficNodeID][latestTrafficIndex]
	currentMaxTraffic := lastThreeTrafficPerNode[maxTrafficNodeID][latestTrafficIndex]

	// Get all available nodes sorted by traffic (ascending) for better balance
	availableNodes := make([]node.ID, 0, len(lastThreeTrafficPerNode))
	for nodeID := range lastThreeTrafficPerNode {
		availableNodes = append(availableNodes, nodeID)
	}
	sort.Slice(availableNodes, func(i, j int) bool {
		return lastThreeTrafficPerNode[availableNodes[i]][latestTrafficIndex] < lastThreeTrafficPerNode[availableNodes[j]][latestTrafficIndex]
	})

	results := make([]SplitSpanCheckResult, 0)

	// Try to find spans that can be moved to any available node for merge
	// Prioritize nodes with lower traffic for better balance
	for _, targetNodeID := range availableNodes {
		if len(results) >= maxMoveSpansCountForMerge {
			break
		}

		for _, span := range sortedSpanByStartKey {
			if len(results) >= maxMoveSpansCountForMerge {
				break
			}

			//  Skip spans already in targetNodeID and spans that have already been selected
			if span.GetNodeID() == targetNodeID || selectedSpans[span.ID] {
				continue
			}

			// Check if moving this span to targetNodeID would enable merge
			if mergedSpans, ok := s.canMergeAfterMove(span, targetNodeID, adjacencyMap, moveSpanTarget); ok {
				// Calculate traffic changes for single move
				spanTraffic := span.lastThreeTraffic[latestTrafficIndex]
				trafficChanges := map[node.ID]float64{
					targetNodeID:     spanTraffic,  // target gets +spanTraffic
					span.GetNodeID(): -spanTraffic, // source gets -spanTraffic
				}

				// Check if this move maintains balance
				if s.isTrafficBalanceMaintained(trafficChanges, afterMoveTrafficPerNode, currentMinTraffic, currentMaxTraffic) {
					// Single move is sufficient
					results = append(results, SplitSpanCheckResult{
						OpType: OpMove,
						MoveSpans: []*SpanReplication{
							span.SpanReplication,
						},
						TargetNode: targetNodeID,
					})
					selectedSpans[span.ID] = true // Mark this span as selected
					moveSpanTarget[span.ID] = targetNodeID
					for _, mergedSpan := range mergedSpans {
						selectedSpans[mergedSpan.ID] = true // the mergedSpan should be merged with the selected span, so we should not move them again
					}

					continue // Move to next span
				}

				// Single move violates balance, try to find compensation move
				if compensationMove, compSpan := s.findCompensationMove(span, mergedSpans, targetNodeID, span.GetNodeID(), sortedSpanByStartKey, afterMoveTrafficPerNode,
					currentMinTraffic, currentMaxTraffic, selectedSpans, moveSpanTarget); compensationMove != nil {
					results = append(results, SplitSpanCheckResult{
						OpType: OpMove,
						MoveSpans: []*SpanReplication{
							span.SpanReplication,
						},
						TargetNode: targetNodeID,
					})
					results = append(results, *compensationMove)

					// Mark both spans as selected
					selectedSpans[span.ID] = true
					moveSpanTarget[span.ID] = targetNodeID
					for _, mergedSpan := range mergedSpans {
						selectedSpans[mergedSpan.ID] = true // the mergedSpan should be merged with the selected span, so we should not move them again
					}

					selectedSpans[compSpan.ID] = true
					moveSpanTarget[compSpan.ID] = compensationMove.TargetNode

					if mergedSpan, ok := s.canMergeAfterMove(compSpan, compensationMove.TargetNode, adjacencyMap, moveSpanTarget); ok {
						for _, mergedSpan := range mergedSpan {
							selectedSpans[mergedSpan.ID] = true // the mergedSpan should be merged with the selected span, so we should not move them again
						}
					}

					continue // Move to next span
				}
			}
		}
	}

	return results
}

// findCompensationMove finds a span to move from targetNode to sourceNode to compensate for the traffic imbalance
func (s *SplitSpanChecker) findCompensationMove(
	movedSpan *splitSpanStatus,
	mergedSpans []*splitSpanStatus,
	targetNode node.ID,
	sourceNode node.ID,
	sortedSpanByStartKey []*splitSpanStatus,
	afterMoveTrafficPerNode map[node.ID]float64,
	currentMinTraffic float64,
	currentMaxTraffic float64,
	selectedSpans map[common.DispatcherID]bool,
	moveSpanTarget map[common.DispatcherID]node.ID,
) (*SplitSpanCheckResult, *splitSpanStatus) {
	movedTraffic := movedSpan.lastThreeTraffic[latestTrafficIndex]

	// Build nodeSpanMap for efficient lookup
	targetNodeSpans := []*splitSpanStatus{}
	for _, span := range sortedSpanByStartKey {
		if span.ID == movedSpan.ID || selectedSpans[span.ID] {
			continue
		}

		for _, mergedSpan := range mergedSpans {
			if span.ID == mergedSpan.ID {
				continue
			}
		}

		target, ok := moveSpanTarget[span.ID]
		if (ok && target == targetNode) || (!ok && span.GetNodeID() == targetNode) {
			targetNodeSpans = append(targetNodeSpans, span)
		}
	}

	// Look for spans in targetNode that can be moved to sourceNode
	for _, span := range targetNodeSpans {
		// Try to find a span that can balance traffic
		// Note: compensation move doesn't need to enable merge, its main purpose is traffic balancing
		compensationTraffic := span.lastThreeTraffic[latestTrafficIndex]

		// Calculate traffic changes for both moves (main move + compensation move)
		trafficChanges := map[node.ID]float64{
			targetNode: movedTraffic - compensationTraffic,  // target: +movedTraffic - compensationTraffic
			sourceNode: -movedTraffic + compensationTraffic, // source: -movedTraffic + compensationTraffic
		}

		// Check if both moves together maintain balance
		if s.isTrafficBalanceMaintained(trafficChanges, afterMoveTrafficPerNode, currentMinTraffic, currentMaxTraffic) {
			return &SplitSpanCheckResult{
				OpType: OpMove,
				MoveSpans: []*SpanReplication{
					span.SpanReplication,
				},
				TargetNode: sourceNode,
			}, span
		}
	}

	return nil, nil
}

// canMergeAfterMove checks if a span can merge with adjacent spans after moving to targetNode
func (s *SplitSpanChecker) canMergeAfterMove(
	span *splitSpanStatus,
	targetNode node.ID,
	adjacencyMap map[common.DispatcherID][]*splitSpanStatus,
	moveSpanTarget map[common.DispatcherID]node.ID,
) ([]*splitSpanStatus, bool) {
	adjacents := adjacencyMap[span.ID]
	candidateSpan := []*splitSpanStatus{}

	for _, adjacent := range adjacents {
		target, ok := moveSpanTarget[adjacent.ID]
		if (ok && target == targetNode) || (!ok && adjacent.GetNodeID() == targetNode) {
			candidateSpan = append(candidateSpan, adjacent)
		}
	}

	totalRegionCount := span.regionCount
	totalTraffic := span.lastThreeTraffic[latestTrafficIndex]

	finalCandidate := []*splitSpanStatus{}

	for _, candidate := range candidateSpan {
		// Check region threshold
		if s.regionThreshold > 0 && totalRegionCount+candidate.regionCount > s.regionThreshold/4*3 {
			continue
		}

		// Check traffic threshold
		if s.writeThreshold > 0 && totalTraffic+candidate.lastThreeTraffic[latestTrafficIndex] > float64(s.writeThreshold)/4*3 {
			continue
		}

		totalRegionCount += candidate.regionCount
		totalTraffic += candidate.lastThreeTraffic[latestTrafficIndex]
		finalCandidate = append(finalCandidate, candidate)
	}
	if len(finalCandidate) > 0 {
		return finalCandidate, true
	}

	return finalCandidate, false
}

// isTrafficBalanceMaintained checks if the traffic changes maintain balance
// It returns true if newMinTraffic >= currentMinTraffic AND newMaxTraffic <= currentMaxTraffic
func (s *SplitSpanChecker) isTrafficBalanceMaintained(
	trafficChanges map[node.ID]float64, // nodeID -> traffic change (can be positive or negative)
	afterMoveTrafficPerNode map[node.ID]float64,
	currentMinTraffic float64,
	currentMaxTraffic float64,
) bool {
	// Find new min and max traffic after changes
	newMinTrafficOverall := currentMinTraffic
	newMaxTrafficOverall := currentMaxTraffic

	for nodeID, traffic := range afterMoveTrafficPerNode {
		var nodeTraffic float64
		if change, exists := trafficChanges[nodeID]; exists {
			nodeTraffic = traffic + change
		} else {
			nodeTraffic = traffic
		}

		if nodeTraffic < newMinTrafficOverall {
			newMinTrafficOverall = nodeTraffic
		}
		if nodeTraffic > newMaxTrafficOverall {
			newMaxTrafficOverall = nodeTraffic
		}
	}

	result := newMinTrafficOverall >= currentMinTraffic && newMaxTrafficOverall <= currentMaxTraffic
	if result {
		// update trafficChanges on after MoveTrafficPerNode
		for node, change := range trafficChanges {
			afterMoveTrafficPerNode[node] = afterMoveTrafficPerNode[node] + change
		}
	}
	// Ensure balance: new min traffic >= current min traffic AND new max traffic <= current max traffic
	return result
}

// The spans can be merged only when satisfy:
// 1. the spans are continuous and in the same node
// 2. the total region count and traffic are less then threshold/4*3 and threshold/4*3
func (s *SplitSpanChecker) chooseMergedSpans(batchSize int) ([]SplitSpanCheckResult, []*splitSpanStatus) {
	log.Debug("chooseMergedSpans try to choose merge spans", zap.Any("changefeedID", s.changefeedID), zap.Any("groupID", s.groupID))
	results := make([]SplitSpanCheckResult, 0)

	sortedSpanByStartKey := make([]*splitSpanStatus, 0, len(s.allTasks))
	for _, status := range s.allTasks {
		sortedSpanByStartKey = append(sortedSpanByStartKey, status)
	}

	// sort all spans based on the start key
	sort.Slice(sortedSpanByStartKey, func(i, j int) bool {
		return bytes.Compare(sortedSpanByStartKey[i].Span.StartKey, sortedSpanByStartKey[j].Span.StartKey) < 0
	})

	mergeSpans := make([]*SpanReplication, 0)
	prev := sortedSpanByStartKey[0]
	regionCount := prev.regionCount
	traffic := prev.lastThreeTraffic[latestTrafficIndex]
	mergeSpans = append(mergeSpans, prev.SpanReplication)

	submitAndClear := func(cur *splitSpanStatus) {
		if len(mergeSpans) > 1 {
			log.Info("chooseMergedSpans merge spans",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Int64("group", int64(s.groupID)),
				zap.Any("mergeSpans", mergeSpans),
				zap.Any("node", mergeSpans[0].GetNodeID()),
			)
			results = append(results, SplitSpanCheckResult{
				OpType:     OpMerge,
				MergeSpans: append([]*SpanReplication{}, mergeSpans...),
			})
		}
		mergeSpans = mergeSpans[:0]
		mergeSpans = append(mergeSpans, cur.SpanReplication)
		regionCount = cur.regionCount
		traffic = cur.lastThreeTraffic[latestTrafficIndex]
	}

	idx := 1
	for idx < len(sortedSpanByStartKey) {
		cur := sortedSpanByStartKey[idx]
		if !bytes.Equal(prev.Span.EndKey, cur.Span.StartKey) {
			// just panic for debug
			log.Panic("unexpected error: span is not continuous",
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Any("prev.Span", common.FormatTableSpan(prev.Span)),
				zap.Any("prev.dispatcherID", prev.ID),
				zap.Any("cur.Span", common.FormatTableSpan(cur.Span)),
				zap.Any("cur.dispatcherID", cur.ID),
			)
		}
		// not in the same node, can't merge
		if prev.GetNodeID() != cur.GetNodeID() {
			submitAndClear(cur)
			prev = cur
			idx++
			continue
		}

		// we can't merge if beyond the threshold
		if s.regionThreshold > 0 && regionCount+cur.regionCount > s.regionThreshold/4*3 {
			submitAndClear(cur)
			prev = cur
			idx++
			continue
		}

		if s.writeThreshold > 0 && traffic+cur.lastThreeTraffic[latestTrafficIndex] > float64(s.writeThreshold)/4*3 {
			submitAndClear(cur)
			prev = cur
			idx++
			continue
		}

		// prev and cur can merged together
		regionCount += cur.regionCount
		traffic += cur.lastThreeTraffic[latestTrafficIndex]
		mergeSpans = append(mergeSpans, cur.SpanReplication)

		prev = cur
		idx++

		if len(results) >= batchSize {
			return results, sortedSpanByStartKey
		}
	}

	if len(mergeSpans) > 1 {
		log.Info("chooseMergedSpans merge spans",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Int64("group", s.groupID),
			zap.Any("mergeSpans", mergeSpans),
			zap.Any("node", mergeSpans[0].GetNodeID()),
		)
		results = append(results, SplitSpanCheckResult{
			OpType:     OpMerge,
			MergeSpans: mergeSpans,
		})
	}

	return results, sortedSpanByStartKey
}

// check whether the whole dispatchers should be merged together.
// only when all spans' total region count and traffic are less then threshold/2, we can merge them together
// consider we only support to merge the spans in the same node, we first do move, then merge
func (s *SplitSpanChecker) checkMergeWhole(totalRegionCount int, lastThreeTrafficSum []float64, lastThreeTrafficPerNode map[node.ID][]float64) []SplitSpanCheckResult {
	log.Debug("checkMergeWhole try to merge whole spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("totalRegionCount", totalRegionCount),
		zap.Any("lastThreeTrafficSum", lastThreeTrafficSum),
		zap.Any("lastThreeTrafficPerNode", lastThreeTrafficPerNode))
	results := make([]SplitSpanCheckResult, 0)

	// check whether satisfy the threshold
	if s.regionThreshold > 0 && totalRegionCount > s.regionThreshold/2 {
		return results
	}

	if s.writeThreshold > 0 {
		for _, traffic := range lastThreeTrafficSum {
			if traffic > float64(s.writeThreshold)/2 {
				return results
			}
		}
	}

	nodeCount := 0
	var targetNode node.ID
	for nodeID, traffic := range lastThreeTrafficPerNode {
		if traffic[latestTrafficIndex] > 0 {
			nodeCount++
			targetNode = nodeID
		}
	}

	if nodeCount == 1 {
		// all spans are in the same node, we can merge directly
		ret := SplitSpanCheckResult{
			OpType:     OpMerge,
			MergeSpans: make([]*SpanReplication, 0, len(s.allTasks)),
		}
		for _, status := range s.allTasks {
			ret.MergeSpans = append(ret.MergeSpans, status.SpanReplication)
		}
		sort.Slice(ret.MergeSpans, func(i, j int) bool {
			return bytes.Compare(ret.MergeSpans[i].Span.StartKey, ret.MergeSpans[j].Span.StartKey) < 0
		})

		results = append(results, ret)
		return results
	}

	// move all spans to the targetNode
	ret := SplitSpanCheckResult{
		OpType:     OpMove,
		MoveSpans:  make([]*SpanReplication, 0, len(s.allTasks)),
		TargetNode: targetNode,
	}

	for _, status := range s.allTasks {
		if status.GetNodeID() != targetNode {
			ret.MoveSpans = append(ret.MoveSpans, status.SpanReplication)
		}
	}
	results = append(results, ret)
	return results
}

// chooseSplitSpans checks all split spans and determines whether any spans should be split again based on traffic and region thresholds.
// It returns a list of SplitSpanCheckResult indicating which spans should be split.
// The function also collects statistics about total region count, traffic per node, and organizes tasks by node.
func (s *SplitSpanChecker) chooseSplitSpans(
	lastThreeTrafficPerNode map[node.ID][]float64,
	lastThreeTrafficSum []float64,
	taskMap map[node.ID][]*splitSpanStatus,
) ([]SplitSpanCheckResult, int) {
	log.Debug("SplitSpanChecker try to choose split spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID))
	totalRegionCount := 0
	results := make([]SplitSpanCheckResult, 0)
	for _, status := range s.allTasks {
		nodeID := status.GetNodeID()
		if nodeID == "" {
			log.Info("split span checker: node id is empty, please check the node id",
				zap.String("changefeed", s.changefeedID.Name()),
				zap.String("dispatcherID", status.ID.String()),
				zap.String("span", common.FormatTableSpan(status.Span)))
			continue
		}

		if _, ok := lastThreeTrafficPerNode[nodeID]; !ok {
			// node is not alive, just skip.
			continue
		}

		// Accumulate statistics for traffic balancing and node distribution
		totalRegionCount += status.regionCount
		lastThreeTrafficSum[0] += status.lastThreeTraffic[0]
		lastThreeTrafficSum[1] += status.lastThreeTraffic[1]
		lastThreeTrafficSum[2] += status.lastThreeTraffic[2]

		lastThreeTrafficPerNode[nodeID][0] += status.lastThreeTraffic[0]
		lastThreeTrafficPerNode[nodeID][1] += status.lastThreeTraffic[1]
		lastThreeTrafficPerNode[nodeID][2] += status.lastThreeTraffic[2]
		taskMap[nodeID] = append(taskMap[nodeID], status)

		if s.writeThreshold > 0 {
			if status.trafficScore > trafficScoreThreshold {
				log.Info("chooseSplitSpans split span by traffic",
					zap.String("changefeed", s.changefeedID.String()),
					zap.Int64("group", s.groupID),
					zap.String("splitSpan", status.SpanReplication.ID.String()),
					zap.Any("splitTargetNodes", status.GetNodeID()),
				)
				spanNum := int(math.Ceil(status.lastThreeTraffic[latestTrafficIndex] / float64(s.writeThreshold)))
				splitTargetNodes := make([]node.ID, 0, spanNum)
				for i := 0; i < spanNum; i++ {
					splitTargetNodes = append(splitTargetNodes, status.GetNodeID())
				}

				results = append(results, SplitSpanCheckResult{
					OpType:           OpSplit,
					SplitSpan:        status.SpanReplication,
					SpanNum:          spanNum,
					SpanType:         split.GetSplitType(status.regionCount),
					SplitTargetNodes: splitTargetNodes,
				})
				continue
			}
		}

		if s.regionThreshold > 0 {
			if status.regionCount > s.regionThreshold {
				log.Info("chooseSplitSpans split span by region",
					zap.String("changefeed", s.changefeedID.String()),
					zap.String("splitSpan", status.SpanReplication.ID.String()),
					zap.Int64("group", s.groupID),
					zap.Any("splitTargetNodes", status.GetNodeID()),
				)

				spanNum := int(math.Ceil(float64(status.regionCount) / float64(s.regionThreshold)))
				splitTargetNodes := make([]node.ID, 0, spanNum)
				for i := 0; i < spanNum; i++ {
					splitTargetNodes = append(splitTargetNodes, status.GetNodeID())
				}

				results = append(results, SplitSpanCheckResult{
					OpType:           OpSplit,
					SplitSpan:        status.SpanReplication,
					SpanNum:          spanNum,
					SpanType:         split.SplitTypeRegionCount,
					SplitTargetNodes: splitTargetNodes,
				})
			}
		}
	}

	return results, totalRegionCount
}

// checkBalanceTraffic checks whether the traffic is balanced for each node.
// If the traffic is not balanced, we try to move some spans from the node with max traffic to the node with min traffic
// If not existing spans can be moved, we try to split a span from the node with max traffic.
func (s *SplitSpanChecker) checkBalanceTraffic(
	aliveNodeIDs []node.ID,
	lastThreeTrafficSum []float64,
	lastThreeTrafficPerNode map[node.ID][]float64,
	taskMap map[node.ID][]*splitSpanStatus,
) (results []SplitSpanCheckResult, minTrafficNodeID node.ID, maxTrafficNodeID node.ID) {
	log.Debug("checkBalanceTraffic try to balance traffic",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("aliveNodeIDs", aliveNodeIDs),
		zap.Any("lastThreeTrafficSum", lastThreeTrafficSum),
		zap.Any("lastThreeTrafficPerNode", lastThreeTrafficPerNode),
		zap.Any("balanceConditionScore", s.balanceCondition.balanceScore),
		zap.Any("balanceConditionStatusUpdated", s.balanceCondition.statusUpdated))

	nodeCount := len(aliveNodeIDs)

	// check whether the traffic is balance for each nodes
	avgLastThreeTraffic := make([]float64, 3)
	for idx, traffic := range lastThreeTrafficSum {
		avgLastThreeTraffic[idx] = traffic / float64(nodeCount)
	}

	// check whether we should balance the traffic for each node
	// sort by traffic first, if traffic is same, we sort by node id
	sort.Slice(aliveNodeIDs, func(i, j int) bool {
		leftTraffic := lastThreeTrafficPerNode[aliveNodeIDs[i]][latestTrafficIndex]
		rightTraffic := lastThreeTrafficPerNode[aliveNodeIDs[j]][latestTrafficIndex]
		if leftTraffic == rightTraffic {
			// We only need to keep the order of nodes with the same traffic fixed for a group.
			// We can have more randomness between different groups to avoid
			// a single node being assigned too much traffic every time in a multi-table scenario.
			if s.groupID%2 == 1 {
				return string(aliveNodeIDs[i]) < string(aliveNodeIDs[j])
			} else {
				return string(aliveNodeIDs[i]) > string(aliveNodeIDs[j])
			}
		}
		return leftTraffic < rightTraffic
	})

	minTrafficNodeID = aliveNodeIDs[0]
	maxTrafficNodeID = aliveNodeIDs[nodeCount-1]

	log.Debug("traffic node info", zap.Any("minTrafficNodeID", minTrafficNodeID), zap.Any("maxTrafficNodeID", maxTrafficNodeID))

	// no status updated, no need to do check balance
	if !s.balanceCondition.statusUpdated {
		return
	}

	// TODO(hyy): add a unit test for this
	// If the traffic in each node is quite low, we don't need to balance the traffic
	needCheckBalance := false
	for _, lastThreeTraffic := range lastThreeTrafficPerNode {
		for _, traffic := range lastThreeTraffic {
			if traffic > minTrafficBalanceThreshold { // 1MB // TODO:use a better threshold
				needCheckBalance = true
				break
			}
		}
		if needCheckBalance {
			break
		}
	}

	if !needCheckBalance {
		s.balanceCondition.reset()
		return
	}

	// to avoid the fluctuation of traffic, we check traffic in last three time.
	// only when each time, the min traffic is less than 80% of the avg traffic,
	// or the max traffic is larger than 120% of the avg traffic,
	// we consider the traffic is imbalanced.
	shouldBalance := true
	balanceCauseByMaxNode := true
	balanceCauseByMinNode := true
	for idx, traffic := range lastThreeTrafficPerNode[minTrafficNodeID] {
		if traffic > avgLastThreeTraffic[idx]*s.minTrafficPercentage {
			shouldBalance = false
			balanceCauseByMinNode = false
			break
		}
	}

	if !shouldBalance {
		shouldBalance = true
		for idx, traffic := range lastThreeTrafficPerNode[maxTrafficNodeID] {
			if traffic < avgLastThreeTraffic[idx]*s.maxTrafficPercentage || traffic < minTrafficBalanceThreshold {
				shouldBalance = false
				balanceCauseByMaxNode = false
				break
			}
		}
	}

	// update balanceScore
	if !shouldBalance {
		s.balanceCondition.reset()
		return
	} else {
		s.balanceCondition.updateScore(minTrafficNodeID, maxTrafficNodeID, balanceCauseByMinNode, balanceCauseByMaxNode)
		log.Debug("should balance, thus update score",
			zap.Any("minTrafficNodeID", minTrafficNodeID),
			zap.Any("maxTrafficNodeID", maxTrafficNodeID),
			zap.Any("balanceCauseByMinNode", balanceCauseByMinNode),
			zap.Any("balanceCauseByMaxNode", balanceCauseByMaxNode),
			zap.Any("balanceScore", s.balanceCondition.balanceScore),
			zap.Any("groupID", s.groupID),
			zap.Any("changefeedID", s.changefeedID),
		)
		if s.balanceCondition.balanceScore < s.balanceScoreThreshold {
			// now is unbalanced, but we want to check more times to avoid balance too frequently
			return
		}
	}

	// calculate the diff traffic between the avg traffic and the min/max traffic
	// we try to move spans, whose total traffic is close to diffTraffic,
	// from the max node to min node
	diffInMinNode := avgLastThreeTraffic[latestTrafficIndex] - lastThreeTrafficPerNode[minTrafficNodeID][latestTrafficIndex]
	diffInMaxNode := lastThreeTrafficPerNode[maxTrafficNodeID][latestTrafficIndex] - avgLastThreeTraffic[latestTrafficIndex]
	diffTraffic := math.Min(diffInMinNode, diffInMaxNode)

	sort.Slice(taskMap[maxTrafficNodeID], func(i, j int) bool {
		return taskMap[maxTrafficNodeID][i].lastThreeTraffic[latestTrafficIndex] < taskMap[maxTrafficNodeID][j].lastThreeTraffic[latestTrafficIndex]
	})
	moveSpans := make([]*SpanReplication, 0)

	sortedSpans := taskMap[maxTrafficNodeID]
	// select spans need to move from maxTrafficNodeID to minTrafficNodeID
	for len(sortedSpans) > 0 && len(moveSpans) < maxMoveSpansCountForTrafficBalance {
		idx, span := findClosestSmaller(sortedSpans, diffTraffic)
		if span != nil {
			moveSpans = append(moveSpans, span.SpanReplication)
		} else {
			break
		}
		diffTraffic -= span.lastThreeTraffic[latestTrafficIndex]
		if diffTraffic < 0 {
			log.Panic("unexpected error: diffTraffic is less than 0",
				zap.Float64("diffTraffic", diffTraffic),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Any("moveSpans", moveSpans),
				zap.Any("taskMap", taskMap),
			)
		}
		// we can only find next possible in sortedSpans[:idx]
		// because sortedSpans[idx + 1] is larger then the original diffTraffic
		sortedSpans = sortedSpans[:idx]

		if diffTraffic == 0 {
			break
		}
	}

	if len(moveSpans) > 0 {
		log.Info("checkBalanceTraffic move spans",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Int64("group", s.groupID),
			zap.Any("moveSpans", moveSpans),
			zap.Any("minTrafficNodeID", minTrafficNodeID),
		)
		results = append(results, SplitSpanCheckResult{
			OpType:     OpMove,
			MoveSpans:  moveSpans,
			TargetNode: minTrafficNodeID,
		})
		s.balanceCondition.reset()
		return
	}

	// no available existing spans, so we need to split span first
	// we choose to split a span near 2 * diffTraffic
	_, span := findClosestSmaller(taskMap[maxTrafficNodeID], 2*diffTraffic)
	// if the min traffic span also larger than 2 * diffTraffic, we just choose the first span
	if span == nil {
		span = taskMap[maxTrafficNodeID][0]
	}

	log.Info("checkBalanceTraffic split span",
		zap.Stringer("changefeed", s.changefeedID),
		zap.String("splitSpan", span.SpanReplication.ID.String()),
		zap.Int64("group", s.groupID),
		zap.Any("splitTargetNodes", []node.ID{minTrafficNodeID, maxTrafficNodeID}),
	)

	results = append(results, SplitSpanCheckResult{
		OpType:           OpSplit,
		SplitSpan:        span.SpanReplication,
		SpanNum:          2,
		SpanType:         split.GetSplitType(span.regionCount),
		SplitTargetNodes: []node.ID{minTrafficNodeID, maxTrafficNodeID}, // split the span, and one in minTrafficNode, one in maxTrafficNode, to balance traffic
	})

	s.balanceCondition.reset()
	return
}

func findClosestSmaller(spans []*splitSpanStatus, diffTraffic float64) (int, *splitSpanStatus) {
	// TODO: consider to use binarySearch for better performance
	for idx, span := range spans {
		if span.lastThreeTraffic[0] > diffTraffic {
			if idx > 0 {
				return idx - 1, spans[idx-1]
			}
			return -1, nil
		}
	}
	return len(spans) - 1, spans[len(spans)-1]
}

func (s *SplitSpanChecker) Stat() string {
	res := strings.Builder{}
	if s.writeThreshold > 0 {
		res.WriteString("traffic infos:")
		// record all the latest three traffic of tasks
		for _, status := range s.allTasks {
			res.WriteString(fmt.Sprintf("[task: %s, traffic: %f, %f, %f];", status.ID, status.lastThreeTraffic[0], status.lastThreeTraffic[1], status.lastThreeTraffic[2]))
		}
	}
	if s.regionThreshold > 0 {
		res.WriteString("region infos:")
		for _, status := range s.allTasks {
			res.WriteString(fmt.Sprintf("[task: %s, region: %d];", status.ID, status.regionCount))
		}
	}
	return res.String()
}

func (s *SplitSpanChecker) Name() string {
	return "split_span_checker"
}

// for test only
func SetEasyThresholdForTest() {
	minTrafficBalanceThreshold = 1
	maxLagThreshold = 120
	mergeThreshold = 1
}
