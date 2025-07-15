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
	"sort"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const latestTrafficIndex = 0

type SplitSpanChecker struct {
	changefeedID common.ChangeFeedID
	groupID      replica.GroupID
	allTasks     map[common.DispatcherID]*splitSpanStatus

	// when writeThreshold is 0, we don't check the traffic
	writeThreshold int
	// when regionThreshold is 0, we don't check the region count
	regionThreshold int

	regionCache RegionCache
	nodeManager *watcher.NodeManager
	pdClock     pdutil.Clock
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
		changefeedID:    changefeedID,
		groupID:         groupID,
		allTasks:        make(map[common.DispatcherID]*splitSpanStatus),
		writeThreshold:  schedulerCfg.WriteKeyThreshold,
		regionThreshold: schedulerCfg.RegionThreshold,
		regionCache:     regionCache,
		nodeManager:     appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		pdClock:         appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
	}
}

func (s *SplitSpanChecker) AddReplica(replica *SpanReplication) {
	s.allTasks[replica.ID] = &splitSpanStatus{
		SpanReplication:  replica,
		regionCheckTime:  time.Now(),
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

	log.Info("split span checker update status",
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

	MergeSpans []*SpanReplication

	MoveSpans  []*SpanReplication
	TargetNode node.ID
}

// return some actions for scheduling the split spans
func (s *SplitSpanChecker) Check(batch int) replica.GroupCheckResult {
	log.Info("SplitSpanChecker try to check",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("batch", batch))
	results := make([]SplitSpanCheckResult, 0)

	aliveNodeIDs := s.nodeManager.GetAliveNodeIDs()

	lastThreeTrafficPerNode := make(map[node.ID][]float64)
	totalRegionCount := 0
	lastThreeTrafficSum := make([]float64, 3)
	// nodeID -> []*splitSpanStatus
	taskMap := make(map[node.ID][]*splitSpanStatus)

	for _, nodeID := range aliveNodeIDs {
		lastThreeTrafficPerNode[nodeID] = make([]float64, 3)
		taskMap[nodeID] = make([]*splitSpanStatus, 0)
	}

	// step1. check whether the split spans should be split again
	//        if a span's region count or traffic exceeds threshold, we should split it again
	results = s.chooseSplitSpans(totalRegionCount, lastThreeTrafficPerNode, lastThreeTrafficSum, taskMap)
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

	// sortedSpans is the sorted spans by the start key
	results, sortedSpans := s.chooseMergedSpans(batch)
	if len(results) > 0 {
		return results
	}

	// step5. try to check whether we need move dispatchers to make merge possible
	//        if the span count is smaller than the upper limit, we don't need merge anymore.
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

	return s.chooseMoveSpans(minTrafficNodeID, maxTrafficNodeID, taskMap[minTrafficNodeID], sortedSpans, lastThreeTrafficPerNode)
}

// chooseMoveSpans chooses the spans to move to make we can merge the spans later.
// we try to find some span move to the minTrafficNodeID
// we each time just choose one span or one pair spans to move.
func (s *SplitSpanChecker) chooseMoveSpans(minTrafficNodeID node.ID, maxTrafficNodeID node.ID, nodeSpans []*splitSpanStatus, sortedSpans []*splitSpanStatus, lastThreeTrafficPerNode map[node.ID][]float64) []SplitSpanCheckResult {
	log.Info("chooseMoveSpans try to choose move spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID),
		zap.Any("minTrafficNodeID", minTrafficNodeID),
		zap.Any("maxTrafficNodeID", maxTrafficNodeID),
		zap.Any("lastThreeTrafficPerNode", lastThreeTrafficPerNode))
	results := make([]SplitSpanCheckResult, 0)

	idx := 0
	for idx+1 < len(sortedSpans) {
		cur := sortedSpans[idx]
		if cur.GetNodeID() != minTrafficNodeID {
			idx++
			continue
		}

		next := sortedSpans[idx+1]
		if next.GetNodeID() == minTrafficNodeID {
			idx++
			continue
		}

		if s.regionThreshold > 0 && cur.regionCount+next.regionCount > s.regionThreshold/4*3 {
			idx++
			continue
		}

		if s.writeThreshold > 0 && cur.lastThreeTraffic[latestTrafficIndex]+next.lastThreeTraffic[latestTrafficIndex] > float64(s.writeThreshold)/4*3 {
			idx++
			continue
		}

		// we can move the spans to the minTrafficNodeID
		// these two spans can be merged if they are in the same node
		nextNodeID := next.GetNodeID()
		trafficNextSpan := next.lastThreeTraffic[latestTrafficIndex]

		// if the span traffic + minTrafficNodeID's traffic is less than the nextNodeID's traffic - this span traffic, we can move the span to the minTrafficNodeID directly
		if trafficNextSpan+lastThreeTrafficPerNode[minTrafficNodeID][latestTrafficIndex] <= lastThreeTrafficPerNode[nextNodeID][latestTrafficIndex]-trafficNextSpan {
			results = append(results, SplitSpanCheckResult{
				OpType: OpMove,
				MoveSpans: []*SpanReplication{
					next.SpanReplication,
				},
				TargetNode: minTrafficNodeID,
			})
			return results
		}
		// we need to also find span in minTrafficNodeID to move to nextNodeID to balance the whole traffic in node.
		traffic := lastThreeTrafficPerNode[nextNodeID][latestTrafficIndex]

		diffToMaxNode := lastThreeTrafficPerNode[maxTrafficNodeID][latestTrafficIndex] - traffic
		diffToMinNode := traffic - lastThreeTrafficPerNode[minTrafficNodeID][latestTrafficIndex]

		maxTraffic := next.lastThreeTraffic[latestTrafficIndex] + diffToMaxNode
		minTraffic := next.lastThreeTraffic[latestTrafficIndex] - diffToMinNode

		for _, span := range nodeSpans {
			if span.ID == cur.ID {
				continue
			}
			if span.lastThreeTraffic[latestTrafficIndex] >= minTraffic && span.lastThreeTraffic[latestTrafficIndex] <= maxTraffic {
				results = append(results, SplitSpanCheckResult{
					OpType: OpMove,
					MoveSpans: []*SpanReplication{
						span.SpanReplication,
					},
					TargetNode: nextNodeID,
				})
				results = append(results, SplitSpanCheckResult{
					OpType: OpMove,
					MoveSpans: []*SpanReplication{
						next.SpanReplication,
					},
					TargetNode: minTrafficNodeID,
				})
				return results
			}
		}
		idx++
	}

	return results
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
	for _, spanStatusItem := range spanStatus {
		log.Info("sorted spanStatus", zap.Any("span", common.FormatTableSpan(spanStatusItem.Span)), zap.Any("nodeID", spanStatusItem.GetNodeID()))
	}

	mergeSpans := make([]*SpanReplication, 0)
	prev := spanStatus[0]
	regionCount := prev.regionCount
	traffic := prev.lastThreeTraffic[latestTrafficIndex]
	mergeSpans = append(mergeSpans, prev.SpanReplication)

	submitAndClear := func(cur *splitSpanStatus) {
		if len(mergeSpans) > 1 {
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
		log.Info("idx", zap.Any("idx", idx), zap.Any("len of mergeSpans", len(mergeSpans)))
		for _, mergeSpan := range mergeSpans {
			log.Info("mergeSpan", zap.Any("span", common.FormatTableSpan(mergeSpan.Span)))
		}
		cur := spanStatus[idx]
		if !bytes.Equal(prev.Span.EndKey, cur.Span.StartKey) {
			// just panic for debug
			log.Panic("unexpected error: span is not continuous",
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Any("prev", prev.Span.String()),
				zap.Any("cur", cur.Span.String()),
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
			return results, spanStatus
		}
	}

	if len(mergeSpans) > 1 {
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
	totalRegionCount int,
	lastThreeTrafficPerNode map[node.ID][]float64,
	lastThreeTrafficSum []float64,
	taskMap map[node.ID][]*splitSpanStatus,
) []SplitSpanCheckResult {
	log.Info("SplitSpanChecker try to choose split spans",
		zap.Any("changefeedID", s.changefeedID),
		zap.Any("groupID", s.groupID))
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
				results = append(results, SplitSpanCheckResult{
					OpType:           OpSplit,
					SplitSpan:        status.SpanReplication,
					SplitTargetNodes: []node.ID{status.GetNodeID(), status.GetNodeID()}, // split span to two spans, and store them in the same node
				})
				continue
			}
		}

		if s.regionThreshold > 0 {
			if status.regionCount > s.regionThreshold {
				results = append(results, SplitSpanCheckResult{
					OpType:           OpSplit,
					SplitSpan:        status.SpanReplication,
					SplitTargetNodes: []node.ID{status.GetNodeID(), status.GetNodeID()}, // split span to two spans, and store them in the same node
				})
			}
		}
	}

	return results
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
		zap.Any("lastThreeTrafficPerNode", lastThreeTrafficPerNode))
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

	// to avoid the fluctuation of traffic, we check traffic in last three time.
	// only when each time, the min traffic is less than 80% of the avg traffic,
	// or the max traffic is larger than 120% of the avg traffic,
	// we consider the traffic is imbalanced.
	shouldBalance := true
	for idx, traffic := range lastThreeTrafficPerNode[minTrafficNodeID] {
		if traffic > avgLastThreeTraffic[idx]*0.8 {
			shouldBalance = false
			break
		}
	}

	if !shouldBalance {
		shouldBalance = true
		for idx, traffic := range lastThreeTrafficPerNode[maxTrafficNodeID] {
			if traffic < avgLastThreeTraffic[idx]*1.2 {
				shouldBalance = false
				break
			}
		}
	}

	if !shouldBalance {
		return
	}

	// calculate the diff traffic between the avg traffic and the min/max traffic
	// we try to move spans, whose total traffic is close to diffTraffic,
	// from the max node to min node
	diffInMinNode := avgLastThreeTraffic[latestTrafficIndex] - lastThreeTrafficPerNode[minTrafficNodeID][latestTrafficIndex]
	diffInMaxNode := lastThreeTrafficPerNode[maxTrafficNodeID][latestTrafficIndex] - avgLastThreeTraffic[latestTrafficIndex]
	diffTraffic := math.Min(diffInMinNode, diffInMaxNode)

	log.Info("diffTraffic", zap.Float64("diffTraffic", diffTraffic))

	sort.Slice(taskMap[maxTrafficNodeID], func(i, j int) bool {
		return taskMap[maxTrafficNodeID][i].lastThreeTraffic[latestTrafficIndex] < taskMap[maxTrafficNodeID][j].lastThreeTraffic[latestTrafficIndex]
	})
	moveSpans := make([]*SpanReplication, 0)

	sortedSpans := taskMap[maxTrafficNodeID]
	// select spans need to move from maxTrafficNodeID to minTrafficNodeID
	for len(sortedSpans) > 0 {
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
		results = append(results, SplitSpanCheckResult{
			OpType:     OpMove,
			MoveSpans:  moveSpans,
			TargetNode: minTrafficNodeID,
		})
		return
	}

	// no available existing spans, so we need to split span first
	// we choose to split a span near 2 * diffTraffic
	_, span := findClosestSmaller(taskMap[maxTrafficNodeID], 2*diffTraffic)
	// if the min traffic span also larger than 2 * diffTraffic, we just choose the first span
	if span == nil {
		span = taskMap[maxTrafficNodeID][0]
	}

	results = append(results, SplitSpanCheckResult{
		OpType:           OpSplit,
		SplitSpan:        span.SpanReplication,
		SplitTargetNodes: []node.ID{minTrafficNodeID, maxTrafficNodeID}, // split the span, and one in minTrafficNode, one in maxTrafficNode, to balance traffic
	})
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
