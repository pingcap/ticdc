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
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const latestTrafficIndex = 0

var minTrafficBalanceThreshold = float64(1024 * 1024) // 1MB
var maxMoveSpansCountForTrafficBalance = 4
var balanceScoreThreshold = 10

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

	balanceCondition BalanceCondition

	regionCache RegionCache
	nodeManager *watcher.NodeManager
	pdClock     pdutil.Clock

	splitSpanCheckDuration prometheus.Observer
}

type splitSpanStatus struct {
	*SpanReplication

	trafficScore int
	// record the traffic of the span for last three times
	// idx = 0 is the latest traffic
	lastThreeTraffic []float64

	regionCount     int
	regionCheckTime time.Time
}

func NewSplitSpanChecker(changefeedID common.ChangeFeedID, groupID replica.GroupID, schedulerCfg *config.ChangefeedSchedulerConfig) *SplitSpanChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	regionCache := appcontext.GetService[RegionCache](appcontext.RegionCache)
	return &SplitSpanChecker{
		changefeedID:           changefeedID,
		groupID:                groupID,
		allTasks:               make(map[common.DispatcherID]*splitSpanStatus),
		writeThreshold:         schedulerCfg.WriteKeyThreshold,
		regionThreshold:        schedulerCfg.RegionThreshold,
		regionCache:            regionCache,
		nodeManager:            appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		pdClock:                appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		splitSpanCheckDuration: metrics.SplitSpanCheckDuration.WithLabelValues(changefeedID.Namespace(), changefeedID.Name(), replica.GetGroupName(groupID)),
	}
}

func (s *SplitSpanChecker) AddReplica(replica *SpanReplication) {
	s.allTasks[replica.ID] = &splitSpanStatus{
		SpanReplication:  replica,
		regionCheckTime:  time.Now().Add(-regionCheckInterval), // Ensure the first time update status will calculate the region count
		regionCount:      0,
		trafficScore:     0,
		lastThreeTraffic: make([]float64, 3),
	}
}

func (s *SplitSpanChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)
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
			log.Info("update traffic score",
				zap.String("changefeed", s.changefeedID.String()),
				zap.Int64("group", int64(s.groupID)),
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
		// check region count, because the change of region count is not frequent, so we can check less frequently
		if time.Since(status.regionCheckTime) > regionCheckInterval {
			regions, err := s.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(context.Background(), 500), status.Span.StartKey, status.Span.EndKey)
			if err != nil {
				log.Warn("list regions failed, skip check region count", zap.String("changefeed", s.changefeedID.Name()), zap.String("span", status.Span.String()), zap.Error(err))
			} else {
				status.regionCount = len(regions)
				status.regionCheckTime = time.Now()
			}
		}
	}

	s.balanceCondition.statusUpdated = true

	log.Debug("split span checker update status",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("replica", replica.ID),
		zap.Any("status", status.GetStatus()),
		zap.Int("status.regionCount", status.regionCount),
		zap.Any("status.regionCheckTime", status.regionCheckTime),
		zap.Int("status.trafficScore", status.trafficScore),
		zap.Any("status.lastThreeTraffic", status.lastThreeTraffic),
	)
}

type SplitSpanCheckResult struct {
	OpType OpType

	SplitSpan        *SpanReplication
	SplitTargetNodes []node.ID
	SpanNum          int

	MergeSpans []*SpanReplication

	MoveSpans  []*SpanReplication
	TargetNode node.ID
}

func (s *SplitSpanChecker) checkAllTaskAvailable() bool {
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
	defer func() {
		s.splitSpanCheckDuration.Observe(time.Since(start).Seconds())
	}()
	log.Info("SplitSpanChecker try to check",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("batch", batch))
	results := make([]SplitSpanCheckResult, 0)

	if !s.checkAllTaskAvailable() {
		log.Info("some task is not available, skip check",
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
	// TODO: set a better threshold
	if lag > 30 {
		log.Info("the lag for the group is too large, skip merge",
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
		log.Info("the span count is proper, so we don't need merge spans",
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

	// sortedSpans is the sorted spans by the start key
	results, sortedSpans := s.chooseMergedSpans(batch)
	if len(results) > 0 {
		return results
	}

	// step5. try to check whether we need move dispatchers to make merge possible
	return s.chooseMoveSpans(minTrafficNodeID, maxTrafficNodeID, sortedSpans, lastThreeTrafficPerNode, taskMap)
}

// chooseMoveSpans finds multiple optimal span moves using a multi-priority search strategy:
// 1. Priority 1: Direct merge moves that maintain traffic balance
// 2. Priority 2: Merge moves with compensation to maintain balance
// 3. Priority 3: Pure traffic balance moves
// Returns multiple move plans sorted by priority and effectiveness
func (s *SplitSpanChecker) chooseMoveSpans(minTrafficNodeID node.ID, maxTrafficNodeID node.ID, sortedSpans []*splitSpanStatus, lastThreeTrafficPerNode map[node.ID][]float64, taskMap map[node.ID][]*splitSpanStatus) []SplitSpanCheckResult {
	log.Info("chooseMoveSpans try to choose move spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("minTrafficNodeID", minTrafficNodeID),
		zap.Any("maxTrafficNodeID", maxTrafficNodeID),
		zap.Int("totalSpans", len(sortedSpans)))

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
	adjacencyMap := s.buildAdjacencyMap(sortedSpans)

	// Try to find optimal span moves using multi-priority strategy
	if result := s.findOptimalSpanMoves(minTrafficNodeID, maxTrafficNodeID, sortedSpans, adjacencyMap, lastThreeTrafficPerNode); len(result) > 0 {
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
func (s *SplitSpanChecker) buildAdjacencyMap(sortedSpans []*splitSpanStatus) map[common.DispatcherID][]*splitSpanStatus {
	adjacencyMap := make(map[common.DispatcherID][]*splitSpanStatus)

	for i, span := range sortedSpans {
		adjacents := make([]*splitSpanStatus, 0, 2)

		// Check previous span
		if i > 0 {
			prev := sortedSpans[i-1]
			if bytes.Equal(prev.Span.EndKey, span.Span.StartKey) {
				adjacents = append(adjacents, prev)
			}
		}

		// Check next span
		if i < len(sortedSpans)-1 {
			next := sortedSpans[i+1]
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
func (s *SplitSpanChecker) findOptimalSpanMoves(minTrafficNodeID node.ID, maxTrafficNodeID node.ID, sortedSpans []*splitSpanStatus, adjacencyMap map[common.DispatcherID][]*splitSpanStatus, lastThreeTrafficPerNode map[node.ID][]float64) []SplitSpanCheckResult {
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
	maxMoves := 10 // Limit to 4 moves to avoid too many changes at once

	// Track spans that have been selected to avoid duplicates
	selectedSpans := make(map[common.DispatcherID]bool)

	// Try to find spans that can be moved to any available node for merge
	// Prioritize nodes with lower traffic for better balance
	for _, targetNodeID := range availableNodes {
		if len(results) >= maxMoves {
			break
		}

		for _, span := range sortedSpans {
			if len(results) >= maxMoves {
				break
			}

			if span.GetNodeID() == targetNodeID {
				continue // Skip spans already in targetNodeID
			}

			// Skip spans that have already been selected
			if selectedSpans[span.ID] {
				continue
			}

			// Check if moving this span to targetNodeID would enable merge
			if s.canMergeAfterMove(span, targetNodeID, adjacencyMap) {
				// Calculate traffic changes for single move
				spanTraffic := span.lastThreeTraffic[latestTrafficIndex]
				trafficChanges := map[node.ID]float64{
					targetNodeID:     spanTraffic,  // target gets +spanTraffic
					span.GetNodeID(): -spanTraffic, // source gets -spanTraffic
				}

				// Check if this move maintains balance
				if s.isTrafficBalanceMaintained(trafficChanges, lastThreeTrafficPerNode, currentMinTraffic, currentMaxTraffic) {
					// Single move is sufficient
					results = append(results, SplitSpanCheckResult{
						OpType: OpMove,
						MoveSpans: []*SpanReplication{
							span.SpanReplication,
						},
						TargetNode: targetNodeID,
					})
					selectedSpans[span.ID] = true // Mark this span as selected
					continue                      // Move to next span
				}

				// Single move violates balance, try to find compensation move
				if compensationMove := s.findCompensationMove(span, targetNodeID, span.GetNodeID(), sortedSpans, lastThreeTrafficPerNode, currentMinTraffic, currentMaxTraffic, selectedSpans); compensationMove != nil {
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
					for _, compSpan := range compensationMove.MoveSpans {
						selectedSpans[compSpan.ID] = true
					}
					continue // Move to next span
				}
			}
		}
	}

	return results
}

// findCompensationMove finds a span to move from targetNode to sourceNode to compensate for the traffic imbalance
func (s *SplitSpanChecker) findCompensationMove(movedSpan *splitSpanStatus, targetNode node.ID, sourceNode node.ID, sortedSpans []*splitSpanStatus, lastThreeTrafficPerNode map[node.ID][]float64, currentMinTraffic float64, currentMaxTraffic float64, selectedSpans map[common.DispatcherID]bool) *SplitSpanCheckResult {
	movedTraffic := movedSpan.lastThreeTraffic[latestTrafficIndex]

	// Build nodeSpanMap for efficient lookup
	targetNodeSpans := []*splitSpanStatus{}
	for _, span := range sortedSpans {
		if span.GetNodeID() == targetNode {
			targetNodeSpans = append(targetNodeSpans, span)
		}
	}

	// Look for spans in targetNode that can be moved to sourceNode
	for _, span := range targetNodeSpans {
		if span.ID == movedSpan.ID {
			continue // Skip the span we just moved
		}

		// Skip spans that have already been selected
		if selectedSpans[span.ID] {
			continue
		}

		// Try to find a span that can balance traffic
		// Note: compensation move doesn't need to enable merge, its main purpose is traffic balancing
		compensationTraffic := span.lastThreeTraffic[latestTrafficIndex]

		// Calculate traffic changes for both moves (main move + compensation move)
		trafficChanges := map[node.ID]float64{
			targetNode: movedTraffic - compensationTraffic,  // target: +movedTraffic - compensationTraffic
			sourceNode: -movedTraffic + compensationTraffic, // source: -movedTraffic + compensationTraffic
		}

		// Check if both moves together maintain balance
		if s.isTrafficBalanceMaintained(trafficChanges, lastThreeTrafficPerNode, currentMinTraffic, currentMaxTraffic) {
			return &SplitSpanCheckResult{
				OpType: OpMove,
				MoveSpans: []*SpanReplication{
					span.SpanReplication,
				},
				TargetNode: sourceNode,
			}
		}
	}

	return nil
}

// canMergeAfterMove checks if a span can merge with adjacent spans after moving to targetNode
func (s *SplitSpanChecker) canMergeAfterMove(span *splitSpanStatus, targetNode node.ID, adjacencyMap map[common.DispatcherID][]*splitSpanStatus) bool {
	adjacents := adjacencyMap[span.ID]

	for _, adjacent := range adjacents {
		if adjacent.GetNodeID() == targetNode {
			// Check if they can merge based on thresholds
			totalRegionCount := span.regionCount + adjacent.regionCount
			totalTraffic := span.lastThreeTraffic[latestTrafficIndex] + adjacent.lastThreeTraffic[latestTrafficIndex]

			// Check region threshold
			if s.regionThreshold > 0 && totalRegionCount > s.regionThreshold/4*3 {
				continue
			}

			// Check traffic threshold
			if s.writeThreshold > 0 && totalTraffic > float64(s.writeThreshold)/4*3 {
				continue
			}

			return true
		}
	}

	return false
}

// isTrafficBalanceMaintained checks if the traffic changes maintain balance
// It returns true if newMinTraffic >= currentMinTraffic AND newMaxTraffic <= currentMaxTraffic
func (s *SplitSpanChecker) isTrafficBalanceMaintained(
	trafficChanges map[node.ID]float64, // nodeID -> traffic change (can be positive or negative)
	lastThreeTrafficPerNode map[node.ID][]float64,
	currentMinTraffic float64,
	currentMaxTraffic float64,
) bool {
	// Find new min and max traffic after changes
	newMinTrafficOverall := currentMinTraffic
	newMaxTrafficOverall := currentMaxTraffic

	for nodeID, traffic := range lastThreeTrafficPerNode {
		var nodeTraffic float64
		if change, exists := trafficChanges[nodeID]; exists {
			nodeTraffic = traffic[latestTrafficIndex] + change
		} else {
			nodeTraffic = traffic[latestTrafficIndex]
		}

		if nodeTraffic < newMinTrafficOverall {
			newMinTrafficOverall = nodeTraffic
		}
		if nodeTraffic > newMaxTrafficOverall {
			newMaxTrafficOverall = nodeTraffic
		}
	}

	// Ensure balance: new min traffic >= current min traffic AND new max traffic <= current max traffic
	return newMinTrafficOverall >= currentMinTraffic && newMaxTrafficOverall <= currentMaxTraffic
}

// The spans can be merged only when satisfy:
// 1. the spans are continuous and in the same node
// 2. the total region count and traffic are less then threshold/4*3 and threshold/4*3
func (s *SplitSpanChecker) chooseMergedSpans(batchSize int) ([]SplitSpanCheckResult, []*splitSpanStatus) {
	log.Info("chooseMergedSpans try to choose merge spans", zap.Any("changefeedID", s.changefeedID), zap.Any("groupID", s.groupID))
	results := make([]SplitSpanCheckResult, 0)

	spanStatus := make([]*splitSpanStatus, 0, len(s.allTasks))
	for _, status := range s.allTasks {
		spanStatus = append(spanStatus, status)
	}

	// sort all spans based on the start key
	sort.Slice(spanStatus, func(i, j int) bool {
		return bytes.Compare(spanStatus[i].Span.StartKey, spanStatus[j].Span.StartKey) < 0
	})

	mergeSpans := make([]*SpanReplication, 0)
	prev := spanStatus[0]
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
	for idx < len(spanStatus) {
		cur := spanStatus[idx]
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
			log.Info("choose Merged Spans",
				zap.Any("total traffic", traffic),
				zap.Any("cur traffic", cur.lastThreeTraffic[latestTrafficIndex]))
			submitAndClear(cur)
			prev = cur
			idx++
			continue
		}

		// prev and cur can merged together
		regionCount += cur.regionCount
		log.Info("traffic",
			zap.Any("total traffic", traffic),
			zap.Any("cur traffic", cur.lastThreeTraffic[latestTrafficIndex]),
		)
		traffic += cur.lastThreeTraffic[latestTrafficIndex]
		mergeSpans = append(mergeSpans, cur.SpanReplication)

		prev = cur
		idx++

		if len(results) >= batchSize {
			return results, spanStatus
		}
	}

	if len(mergeSpans) > 1 {
		log.Info("chooseMergedSpans merge spans",
			zap.String("changefeed", s.changefeedID.String()),
			zap.Int64("group", int64(s.groupID)),
			zap.Any("mergeSpans", mergeSpans),
			zap.Any("node", mergeSpans[0].GetNodeID()),
		)
		results = append(results, SplitSpanCheckResult{
			OpType:     OpMerge,
			MergeSpans: mergeSpans,
		})
	}

	return results, spanStatus
}

// check whether the whole dispatchers should be merged together.
// only when all spans' total region count and traffic are less then threshold/2, we can merge them together
// consider we only support to merge the spans in the same node, we first do move, then merge
func (s *SplitSpanChecker) checkMergeWhole(totalRegionCount int, lastThreeTrafficSum []float64, lastThreeTrafficPerNode map[node.ID][]float64) []SplitSpanCheckResult {
	log.Info("checkMergeWhole try to merge whole spans",
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
	log.Info("SplitSpanChecker try to choose split spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID))
	totalRegionCount := 0
	results := make([]SplitSpanCheckResult, 0)
	for _, status := range s.allTasks {
		// Accumulate statistics for traffic balancing and node distribution
		totalRegionCount += status.regionCount
		lastThreeTrafficSum[0] += status.lastThreeTraffic[0]
		lastThreeTrafficSum[1] += status.lastThreeTraffic[1]
		lastThreeTrafficSum[2] += status.lastThreeTraffic[2]

		nodeID := status.GetNodeID()
		if nodeID == "" {
			log.Panic("split span checker: node id is empty, please check the node id", zap.String("changefeed", s.changefeedID.Name()), zap.String("dispatcherID", status.ID.String()), zap.String("span", status.Span.String()))
		}

		lastThreeTrafficPerNode[nodeID][0] += status.lastThreeTraffic[0]
		lastThreeTrafficPerNode[nodeID][1] += status.lastThreeTraffic[1]
		lastThreeTrafficPerNode[nodeID][2] += status.lastThreeTraffic[2]
		taskMap[nodeID] = append(taskMap[nodeID], status)

		if s.writeThreshold > 0 {
			if status.trafficScore > trafficScoreThreshold {
				log.Info("chooseSplitSpans split span by traffic",
					zap.String("changefeed", s.changefeedID.String()),
					zap.Int64("group", int64(s.groupID)),
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
					SplitTargetNodes: splitTargetNodes,
				})
				continue
			}
		}

		if s.regionThreshold > 0 {
			if status.regionCount > s.regionThreshold {
				log.Info("chooseSplitSpans split span by region",
					zap.String("changefeed", s.changefeedID.String()),
					zap.Int64("group", int64(s.groupID)),
					zap.String("splitSpan", status.SpanReplication.ID.String()),
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
	log.Info("checkBalanceTraffic try to balance traffic",
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
	sort.Slice(aliveNodeIDs, func(i, j int) bool {
		return lastThreeTrafficPerNode[aliveNodeIDs[i]][0] < lastThreeTrafficPerNode[aliveNodeIDs[j]][0]
	})

	minTrafficNodeID = aliveNodeIDs[0]
	maxTrafficNodeID = aliveNodeIDs[nodeCount-1]

	log.Info("minTrafficNodeID", zap.Any("minTrafficNodeID", minTrafficNodeID))
	log.Info("maxTrafficNodeID", zap.Any("maxTrafficNodeID", maxTrafficNodeID))

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
		if traffic > avgLastThreeTraffic[idx]*0.8 {
			shouldBalance = false
			balanceCauseByMinNode = false
			break
		}
	}

	if !shouldBalance {
		shouldBalance = true
		for idx, traffic := range lastThreeTrafficPerNode[maxTrafficNodeID] {
			if traffic < avgLastThreeTraffic[idx]*1.2 {
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
		if s.balanceCondition.balanceScore < balanceScoreThreshold {
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
			zap.Int64("group", int64(s.groupID)),
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
		zap.String("changefeed", s.changefeedID.String()),
		zap.Int64("group", int64(s.groupID)),
		zap.String("splitSpan", span.SpanReplication.ID.String()),
		zap.Any("splitTargetNodes", []node.ID{minTrafficNodeID, maxTrafficNodeID}),
	)
	results = append(results, SplitSpanCheckResult{
		OpType:           OpSplit,
		SplitSpan:        span.SpanReplication,
		SpanNum:          2,
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
func SetMinTrafficBalanceThreshold(threshold float64) {
	minTrafficBalanceThreshold = threshold
}
