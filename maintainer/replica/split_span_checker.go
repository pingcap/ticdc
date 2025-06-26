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
	"math"
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type SplitSpanChecker struct {
	changefeedID common.ChangeFeedID
	allTasks     map[common.DispatcherID]*splitSpanStatus

	writeThreshold  int
	regionThreshold int
	regionCache     RegionCache
	nodeManager     *watcher.NodeManager
}

type splitSpanStatus struct {
	*SpanReplication
	trafficScore    int
	regionCount     int
	regionCheckTime time.Time

	// record the traffic of the span for last three times
	lastThreeTraffic []float64
}

func NewSplitSpanChecker(changefeedID common.ChangeFeedID, schedulerCfg *config.ChangefeedSchedulerConfig) *SplitSpanChecker {
	if schedulerCfg == nil {
		log.Panic("scheduler config is nil, please check the config", zap.String("changefeed", changefeedID.Name()))
	}
	regionCache := appcontext.GetService[RegionCache](appcontext.RegionCache)
	return &SplitSpanChecker{
		changefeedID:    changefeedID,
		allTasks:        make(map[common.DispatcherID]*splitSpanStatus),
		writeThreshold:  schedulerCfg.WriteKeyThreshold,
		regionThreshold: schedulerCfg.RegionThreshold,
		regionCache:     regionCache,
		nodeManager:     appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
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
		log.Panic("default span split checker: replica not found", zap.String("changefeed", s.changefeedID.Name()), zap.String("replica", replica.ID.String()))
	}
	if status.GetStatus().ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	// check traffic first
	if status.GetStatus().EventSizePerSecond != 0 {
		if status.GetStatus().EventSizePerSecond < float32(s.writeThreshold) {
			status.trafficScore = 0
		} else {
			status.trafficScore++
		}

		status.lastThreeTraffic[0] = status.lastThreeTraffic[1]
		status.lastThreeTraffic[1] = status.lastThreeTraffic[2]
		status.lastThreeTraffic[2] = float64(status.GetStatus().EventSizePerSecond)
	}

	// check region count, because the change of region count is not frequent, so we can check less frequently
	if time.Since(status.regionCheckTime) > regionCheckInterval {
		regions, err := s.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(context.Background(), 500), status.Span.StartKey, status.Span.EndKey)
		if err != nil {
			log.Warn("list regions failed, skip check region count", zap.String("changefeed", s.changefeedID.Name()), zap.String("span", status.Span.String()), zap.Error(err))
		} else {
			status.regionCount = len(regions)
		}
		status.regionCheckTime = time.Now()
	}
}

type SplitSpanCheckResult struct {
	OpType     OpType
	SplitType  SplitType
	MergeSpans []*SpanReplication
	SplitSpan  *SpanReplication
	MoveSpans  []*SpanReplication
	TargetNode node.ID
}

// return some actions
func (s *SplitSpanChecker) Check(batch int) replica.GroupCheckResult {
	results := make([]SplitSpanCheckResult, 0)
	totalRegionCount := 0
	lastThreeTraffic := make([]float64, 0, 3)
	lastThreeTrafficPerNode := make(map[node.ID][]float64)
	taskMap := make(map[node.ID][]*splitSpanStatus)
	aliveNodeIDs := s.nodeManager.GetAliveNodeIDs()
	nodeCount := len(aliveNodeIDs)

	for _, nodeID := range aliveNodeIDs {
		lastThreeTrafficPerNode[nodeID] = make([]float64, 3)
		taskMap[nodeID] = make([]*splitSpanStatus, 0)
	}

	for _, status := range s.allTasks {
		if status.trafficScore > trafficScoreThreshold {
			results = append(results, SplitSpanCheckResult{
				OpType:    OpSplit,
				SplitType: SplitByTraffic,
				SplitSpan: status.SpanReplication,
			})
		} else if status.regionCount > s.regionThreshold {
			results = append(results, SplitSpanCheckResult{
				OpType:    OpSplit,
				SplitType: SplitByRegion,
				SplitSpan: status.SpanReplication,
			})
		}
		totalRegionCount += status.regionCount
		lastThreeTraffic[0] += status.lastThreeTraffic[0]
		lastThreeTraffic[1] += status.lastThreeTraffic[1]
		lastThreeTraffic[2] += status.lastThreeTraffic[2]
		if status.GetNodeID() == "" {
			log.Panic("split span checker: node id is empty, please check the node id", zap.String("changefeed", s.changefeedID.Name()), zap.String("dispatcherID", status.ID.String()), zap.String("span", status.Span.String()))
		}
		lastThreeTrafficPerNode[status.GetNodeID()][0] += status.lastThreeTraffic[0]
		lastThreeTrafficPerNode[status.GetNodeID()][1] += status.lastThreeTraffic[1]
		lastThreeTrafficPerNode[status.GetNodeID()][2] += status.lastThreeTraffic[2]
		taskMap[status.GetNodeID()] = append(taskMap[status.GetNodeID()], status)
	}

	if len(results) > 0 {
		// If some spans need to split, we just return the results
		return results
	}

	// check whether the whole dispatchers should be merged together.
	if s.shouldMergeWhole(totalRegionCount, lastThreeTraffic) {
		ret := SplitSpanCheckResult{
			OpType:     OpMerge,
			MergeSpans: make([]*SpanReplication, 0, len(s.allTasks)),
		}
		for _, status := range s.allTasks {
			ret.MergeSpans = append(ret.MergeSpans, status.SpanReplication)
		}
		results = append(results, ret)
		return results
	}

	// check whether the traffic is balance for each nodes
	avgLastThreeTraffic := make([]float64, 3)
	for idx, traffic := range lastThreeTraffic {
		avgLastThreeTraffic[idx] = traffic / float64(nodeCount)
	}

	// check whether we should balance the traffic for each node
	sort.Slice(aliveNodeIDs, func(i, j int) bool {
		return lastThreeTrafficPerNode[aliveNodeIDs[i]][0] < lastThreeTrafficPerNode[aliveNodeIDs[j]][0]
	})

	minTafficNodeID := aliveNodeIDs[0]
	maxTrafficNodeID := aliveNodeIDs[nodeCount-1]

	shouldBalance := true
	for idx, traffic := range lastThreeTrafficPerNode[minTafficNodeID] {
		if traffic > avgLastThreeTraffic[idx]*0.9 {
			shouldBalance = false
			break
		}
	}
	for idx, traffic := range lastThreeTrafficPerNode[maxTrafficNodeID] {
		if traffic < avgLastThreeTraffic[idx]*1.1 {
			shouldBalance = false
			break
		}
	}

	if shouldBalance {
		diffInMinNode := avgLastThreeTraffic[0] - lastThreeTrafficPerNode[minTafficNodeID][0]
		diffInMaxNode := lastThreeTrafficPerNode[maxTrafficNodeID][0] - avgLastThreeTraffic[0]
		diffTraffic := math.Min(diffInMinNode, diffInMaxNode)

		// we try to move diffTraffic from the max node to min node

		// find the spans' traffic which is the most close to diffTraffic
		sort.Slice(taskMap[maxTrafficNodeID], func(i, j int) bool {
			return taskMap[maxTrafficNodeID][i].lastThreeTraffic[0] < taskMap[maxTrafficNodeID][j].lastThreeTraffic[0]
		})
		moveSpans := make([]*SpanReplication, 0)

		// select spans need to move from maxTrafficNodeID to minTrafficNodeID
		for true {
			idx, span := findClosestSmaller(taskMap[maxTrafficNodeID], diffTraffic)
			if span != nil {
				moveSpans = append(moveSpans, span.SpanReplication)
			} else {
				break
			}
			newTasks := make([]*splitSpanStatus, len(taskMap[maxTrafficNodeID])-1)
			copy(newTasks, taskMap[maxTrafficNodeID][:idx])
			copy(newTasks[idx:], taskMap[maxTrafficNodeID][idx+1:])
			taskMap[maxTrafficNodeID] = newTasks

			diffTraffic -= span.lastThreeTraffic[0]
			if diffTraffic <= 0 {
				log.Panic("unexpected error: diffTraffic is less than 0",
					zap.Float64("diffTraffic", diffTraffic),
					zap.String("changefeed", s.changefeedID.Name()),
					zap.Any("moveSpans", moveSpans),
					zap.Any("taskMap", taskMap),
				)
			}
		}

		if len(moveSpans) > 0 {
			results := append(results, SplitSpanCheckResult{
				OpType:     OpMove,
				MoveSpans:  moveSpans,
				TargetNode: minTafficNodeID,
			})
			return results
		} else {
			// no available existing spans, so we need to split span first
			// we choose to split a span near 2 * diffTraffic
			_, span := findClosestSmaller(taskMap[maxTrafficNodeID], 2*diffTraffic)
			// if the min traffic span also larger than 2 * diffTraffic, we just choose the first span
			if span == nil {
				span = taskMap[maxTrafficNodeID][0]
			}

			results = append(results, SplitSpanCheckResult{
				OpType:    OpSplit,
				SplitType: SplitByTraffic,
				SplitSpan: span.SpanReplication,
			})
			return results
		}
	}

	// check whether we need to do merge

	return results
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
	return -1, nil
}

func (s *SplitSpanChecker) shouldMergeWhole(totalRegionCount int, lastThreeTraffic []float64) bool {
	// we make a more strict condition to merge whole together
	if totalRegionCount > s.regionThreshold/2 {
		return false
	}
	for _, traffic := range lastThreeTraffic {
		if traffic > float64(s.writeThreshold)/2 {
			return false
		}
	}
	return true
}

func (s *SplitSpanChecker) Stat() string {
}
