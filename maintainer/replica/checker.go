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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

type OpType int

const (
	OpSplit         OpType = iota // Split one span to multiple subspans
	OpMerge                       // merge multiple spans to one span
	OpMergeAndSplit               // remove old spans and split to multiple subspans
)

type CheckResult struct {
	OpType       OpType
	Replications []*SpanReplication
}

type hotSpanChecker struct {
	hotTasks       map[common.DispatcherID]*hotSpanStatus
	writeThreshold float32
	scoreThreshold int
}

func newHotSpanChecker() *hotSpanChecker {
	return &hotSpanChecker{
		hotTasks: make(map[common.DispatcherID]*hotSpanStatus),
	}
}

func (s *hotSpanChecker) AddReplica(replica *SpanReplication) {
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
			log.Debug("remove unworking hot span", zap.String("span", span.ID.String()))
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
		hotSpan = &hotSpanStatus{
			SpanReplication: span,
			writeThreshold:  s.writeThreshold,
			score:           0,
		}
		s.hotTasks[span.ID] = hotSpan
	}
	hotSpan.score++
	hotSpan.lastUpdateTime = time.Now()
}

func (s *hotSpanChecker) Check(batchSize int) any {
	cache := make([]CheckResult, 0)

	for _, hotSpan := range s.hotTasks {
		if time.Since(hotSpan.lastUpdateTime) > clearTimeout*time.Second {
			log.Warn("remove hot span since it is outdated", zap.String("span", hotSpan.ID.String()))
			s.RemoveReplica(hotSpan.SpanReplication)
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

	eventSizePerSecond   float32
	writeThreshold       float32 // maybe a dynamic value
	imbalanceCoefficient int     // fixed value for each group
	// score add 1 when the eventSizePerSecond is larger than writeThreshold*imbalanceCoefficient
	score          int
	lastUpdateTime time.Time
}

type imbalanceChecker struct {
	allTasks               map[common.DispatcherID]*hotSpanStatus
	nodeManager            *watcher.NodeManager
	softWriteThreshold     float32
	softImbalanceThreshold int
	softScore              int
	softMergeScore         int
	softScoreThreshold     int

	hardWriteThreshold     float32
	hardImbalanceThreshold int
}

func newImbalanceChecker() *imbalanceChecker {
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	return &imbalanceChecker{
		allTasks:    make(map[common.DispatcherID]*hotSpanStatus),
		nodeManager: nodeManager,
	}
}

func (s *imbalanceChecker) AddReplica(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; ok {
		log.Panic("add duplicated replica", zap.String("replica", replica.ID.String()))
	}
	s.allTasks[replica.ID] = &hotSpanStatus{
		SpanReplication: replica,
	}
}

func (s *imbalanceChecker) RemoveReplica(replica *SpanReplication) {
	delete(s.allTasks, replica.ID)
}

func (s *imbalanceChecker) UpdateStatus(replica *SpanReplication) {
	if _, ok := s.allTasks[replica.ID]; !ok {
		log.Panic("update unexist replica", zap.String("replica", replica.ID.String()))
	}
}

func (s *imbalanceChecker) Check(_ int) any {
	nodeLoads := make(map[node.ID]float64)
	replications := []*SpanReplication{}
	totalEventSizePerSecond := float32(0)
	for _, span := range s.allTasks {
		status := span.GetStatus()
		totalEventSizePerSecond += status.EventSizePerSecond
		nodeLoads[span.GetNodeID()] += float64(status.EventSizePerSecond)
		replications = append(replications, span.SpanReplication)
	}

	if totalEventSizePerSecond <= s.softWriteThreshold {
		s.softMergeScore++
		if s.softMergeScore >= s.softScoreThreshold {
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
	ret := []CheckResult{
		{
			OpType:       OpMergeAndSplit,
			Replications: replications,
		},
	}
	if len(s.allTasks) < len(s.nodeManager.GetAliveNodes()) {
		return ret
	}

	maxLoad, minLoad := float64(0.0), math.MaxFloat64
	for _, load := range nodeLoads {
		maxLoad = math.Max(maxLoad, load)
		minLoad = math.Min(minLoad, load)
	}

	minLoad = math.Max(minLoad, float64(s.softWriteThreshold))
	if maxLoad-minLoad > float64(s.hardWriteThreshold) {
		if int(maxLoad/minLoad) > s.hardImbalanceThreshold {
			s.softScore = 0
			return ret
		}
	}

	if maxLoad/minLoad > float64(s.softImbalanceThreshold) {
		s.softScore++
	} else {
		s.softScore = max(s.softScore-1, 0)
	}
	if s.softScore >= s.softScoreThreshold {
		s.softScore = 0
		return ret
	}
	return nil
}
