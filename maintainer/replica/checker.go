// Copyright 2024 PingCAP, Inc.
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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

type OpType int

const (
	OpSplit         OpType = iota // Split one span to multiple subspans
	OpMerge                       // merge multiple spans to one span
	OpMergeAndSplit               // remove old spans and split to multiple subspans
)

const (
	HotSpanWriteThreshold = 1024 * 1024 // 1MB per second
	HotSpanScoreThreshold = 3           // TODO: bump to 10 befroe release
	DefaultScoreThreshold = 3

	defaultHardImbalanceThreshold = float64(1.35) // used to trigger the rebalance
	clearTimeout                  = 300           // seconds
)

var MinSpanNumberCoefficient = 2

type CheckResult struct {
	OpType       OpType
	Replications []*SpanReplication
}

func (c CheckResult) String() string {
	opStr := ""
	switch c.OpType {
	case OpSplit:
		opStr = "split"
	case OpMerge:
		opStr = "merge"
	case OpMergeAndSplit:
		opStr = "merge and split"
	default:
		panic("unknown op type")
	}
	return fmt.Sprintf("OpType: %s, ReplicationSize: %d", opStr, len(c.Replications))
}

func getNewGroupChecker(
	cfID common.ChangeFeedID, enableTableAcrossNodes bool,
) func(replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication] {
	if !enableTableAcrossNodes {
		return replica.NewEmptyChecker[common.DispatcherID, *SpanReplication]
	}
	return func(groupID replica.GroupID) replica.GroupChecker[common.DispatcherID, *SpanReplication] {
		groupType := replica.GetGroupType(groupID)
		switch groupType {
		case replica.GroupDefault:
			return newHotSpanChecker(cfID)
		case replica.GroupTable:
			return newImbalanceChecker(cfID)
		}
		log.Panic("unknown group type", zap.String("changefeed", cfID.Name()), zap.Int8("groupType", int8(groupType)))
		return nil
	}
}

type hotSpanChecker struct {
	changefeedID   common.ChangeFeedID
	hotTasks       map[common.DispatcherID]*hotSpanStatus
	writeThreshold float32
	scoreThreshold int
}

func newHotSpanChecker(cfID common.ChangeFeedID) *hotSpanChecker {
	return &hotSpanChecker{
		changefeedID:   cfID,
		hotTasks:       make(map[common.DispatcherID]*hotSpanStatus),
		writeThreshold: HotSpanWriteThreshold,
		scoreThreshold: HotSpanScoreThreshold,
	}
}

func (s *hotSpanChecker) Name() string {
	return "hot span checker"
}

func (s *hotSpanChecker) AddReplica(replica *SpanReplication) {
	// only track the hot span dynamically
	return
}

func (s *hotSpanChecker) RemoveReplica(span *SpanReplication) {
	delete(s.hotTasks, span.ID)
}

func (s *hotSpanChecker) UpdateStatus(span *SpanReplication) {
	status := span.GetStatus()
	if status.ComponentStatus != heartbeatpb.ComponentState_Working {
		if _, ok := s.hotTasks[span.ID]; ok {
			delete(s.hotTasks, span.ID)
			log.Debug("remove unworking hot span", zap.String("changefeed", s.changefeedID.Name()), zap.String("span", span.ID.String()))
		}
		return
	}

	if status.EventSizePerSecond < s.writeThreshold {
		if hotSpan, ok := s.hotTasks[span.ID]; ok {
			hotSpan.score--
			if hotSpan.score == 0 {
				delete(s.hotTasks, span.ID)
			}
		}
		return
	}

	hotSpan, ok := s.hotTasks[span.ID]
	if !ok {
		// add the new hot span
		hotSpan = &hotSpanStatus{
			SpanReplication: span,
			score:           0,
		}
		s.hotTasks[span.ID] = hotSpan
	}
	hotSpan.score++
	hotSpan.lastUpdateTime = time.Now()
}

func (s *hotSpanChecker) Check(batchSize int) replica.GroupCheckResult {
	cache := make([]CheckResult, 0)

	for _, hotSpan := range s.hotTasks {
		if time.Since(hotSpan.lastUpdateTime) > clearTimeout*time.Second {
			// should not happen
			log.Panic("remove hot span since it is outdated",
				zap.String("changefeed", s.changefeedID.Name()), zap.String("span", hotSpan.ID.String()))
			// s.RemoveReplica(hotSpan.SpanReplication)
		} else if hotSpan.score >= s.scoreThreshold {
			cache = append(cache, CheckResult{
				OpType:       OpSplit,
				Replications: []*SpanReplication{hotSpan.SpanReplication},
			})
			if len(cache) >= batchSize {
				break
			}
		}
	}
	return cache
}

func (s *hotSpanChecker) Stat() string {
	var res strings.Builder
	cnts := make([]int, s.scoreThreshold+1)
	for _, hotSpan := range s.hotTasks {
		score := min(s.scoreThreshold, hotSpan.score)
		cnts[score]++
	}
	for i, cnt := range cnts {
		if cnt == 0 {
			continue
		}
		res.WriteString("score ")
		res.WriteString(strconv.Itoa(i))
		res.WriteString("->")
		res.WriteString(strconv.Itoa(cnt))
		res.WriteString(";")
		if i < len(cnts)-1 {
			res.WriteString(" ")
		}
	}
	if res.Len() == 0 {
		return "No hot spans"
	}
	return res.String()
}

type hotSpanStatus struct {
	*SpanReplication
	HintMaxSpanNum uint64
	// score add 1 when the eventSizePerSecond is larger than writeThreshold*imbalanceCoefficient
	score          int
	lastUpdateTime time.Time
}

type rebalanceChecker struct {
	changefeedID common.ChangeFeedID
	allTasks     map[common.DispatcherID]*hotSpanStatus
	nodeManager  *watcher.NodeManager

	// fast check, rebalance immediately when both the total load and imbalance ratio is high
	hardWriteThreshold     float32
	hardImbalanceThreshold float64
	// slow check, rebalance only if the imbalance condition has lasted for a period of time
	softWriteThreshold     float32
	softImbalanceThreshold float64

	// score measures the duration of the condition
	softRebalanceScore          int // add 1 when the load is not balanced
	softRebalanceScoreThreshold int
	softMergeScore              int // add 1 when the total load is lowwer than the softWriteThreshold
	softMergeScoreThreshold     int
}

func newImbalanceChecker(cfID common.ChangeFeedID) *rebalanceChecker {
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	return &rebalanceChecker{
		changefeedID:           cfID,
		allTasks:               make(map[common.DispatcherID]*hotSpanStatus),
		nodeManager:            nodeManager,
		hardWriteThreshold:     10 * HotSpanWriteThreshold,
		hardImbalanceThreshold: defaultHardImbalanceThreshold,

		softWriteThreshold:          HotSpanWriteThreshold,
		softImbalanceThreshold:      2 * defaultHardImbalanceThreshold,
		softRebalanceScoreThreshold: DefaultScoreThreshold,
		softMergeScoreThreshold:     DefaultScoreThreshold,
	}
}

func (s *rebalanceChecker) Name() string {
	return "table rebalance checker"
}

func (s *rebalanceChecker) AddReplica(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; ok {
		log.Panic("add duplicated replica", zap.String("changefeed", s.changefeedID.Name()),
			zap.String("replica", replica.ID.String()))
	}
	s.allTasks[replica.ID] = &hotSpanStatus{
		SpanReplication: replica,
	}
}

func (s *rebalanceChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)
}

func (s *rebalanceChecker) UpdateStatus(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; !ok {
		log.Panic("update unexist replica", zap.String("changefeed", s.changefeedID.Name()),
			zap.String("replica", replica.ID.String()))
	}
}

func (s *rebalanceChecker) Check(_ int) replica.GroupCheckResult {
	nodeLoads := make(map[node.ID]float64)
	replications := []*SpanReplication{}
	totalEventSizePerSecond := float32(0)
	for _, span := range s.allTasks {
		status := span.GetStatus()
		nodeID := span.GetNodeID()
		if status.ComponentStatus != heartbeatpb.ComponentState_Working || nodeID == "" {
			log.Warn("skip rebalance since the span is not working",
				zap.String("changefeed", s.changefeedID.Name()), zap.String("span", span.ID.String()))
			return nil
		}
		totalEventSizePerSecond += status.EventSizePerSecond
		nodeLoads[span.GetNodeID()] += float64(status.EventSizePerSecond)
		replications = append(replications, span.SpanReplication)
	}

	// check merge
	if totalEventSizePerSecond < s.softWriteThreshold {
		s.softRebalanceScore = 0
		s.softMergeScore++
		if s.softMergeScore >= s.softMergeScoreThreshold {
			s.softMergeScore = 0
			return []CheckResult{
				{
					OpType:       OpMerge,
					Replications: replications,
				},
			}
		}
		return nil
	}
	s.softMergeScore = 0

	return s.checkRebalance(nodeLoads, replications)
}

func (s *rebalanceChecker) checkRebalance(
	nodeLoads map[node.ID]float64, replications []*SpanReplication,
) []CheckResult {
	ret := []CheckResult{
		{
			OpType:       OpMergeAndSplit,
			Replications: replications,
		},
	}
	// case 1: too much nodes, need split more spans
	allNodes := s.nodeManager.GetAliveNodes()
	if len(s.allTasks) < len(allNodes)*MinSpanNumberCoefficient {
		return ret
	}
	if len(nodeLoads) != len(allNodes) {
		// wait for tasks balanced across all nodes
		log.Warn("skip rebalance since tasks are not balanced", zap.String("changefeed", s.changefeedID.Name()),
			zap.Int("nodesWithTasks", len(nodeLoads)), zap.Int("allNodes", len(allNodes)))
		return nil
	}

	maxLoad, minLoad := float64(0.0), math.MaxFloat64
	for _, load := range nodeLoads {
		maxLoad = math.Max(maxLoad, load)
		minLoad = math.Min(minLoad, load)
	}
	minLoad = math.Max(minLoad, float64(s.softWriteThreshold))

	// case 2: check hard rebalance
	if maxLoad-minLoad >= float64(s.hardWriteThreshold) && maxLoad/minLoad > s.hardImbalanceThreshold {
		s.softRebalanceScore = 0
		return ret
	}

	// case 3: check soft rebalance
	if maxLoad/minLoad >= s.softImbalanceThreshold {
		s.softRebalanceScore++
	} else {
		s.softRebalanceScore = max(s.softRebalanceScore-1, 0)
	}
	if s.softRebalanceScore >= s.softRebalanceScoreThreshold {
		s.softRebalanceScore = 0
		return ret
	}

	// default case: no need to rebalance
	return nil
}

func (s *rebalanceChecker) Stat() string {
	res := strings.Builder{}
	res.WriteString(fmt.Sprintf("total tasks: %d; hard: [writeThreshold: %f, imbalanceThreshold: %f];",
		len(s.allTasks), s.hardWriteThreshold, s.hardImbalanceThreshold))
	res.WriteString(fmt.Sprintf("soft: [writeThreshold: %f, imbalanceThreshold: %f, rebalanceScoreThreshold: %d, mergeScoreThreshold: %d];",
		s.softWriteThreshold, s.softImbalanceThreshold, s.softRebalanceScoreThreshold, s.softMergeScoreThreshold))
	res.WriteString(fmt.Sprintf("softScore: [rebalance: %d, merge: %d]", s.softRebalanceScore, s.softMergeScore))
	return res.String()
}

// TODO: implement the dynamic merge and split checker
type dynamicMergeSplitChecker struct {
	changefeedID common.ChangeFeedID
	allTasks     map[common.DispatcherID]*hotSpanStatus
	nodeManager  *watcher.NodeManager
}
