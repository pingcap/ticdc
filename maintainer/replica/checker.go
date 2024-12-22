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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
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
	Replications []SpanReplication
}

// define the check strategy
// soft/hard threadhold
// split/merge/mergeAndSplit result

type hotSpanChecker struct {
	lock          sync.Mutex
	hotSpanGroups map[replica.GroupID]map[common.DispatcherID]*hotSpan
}

type hotSpan struct {
	*SpanReplication
	HintMaxSpanNum uint64

	eventSizePerSecond   float32
	writeThreshold       float32 // maybe a dynamic value
	imbalanceCoefficient int     // fixed value for each group
	// score add 1 when the eventSizePerSecond is larger than writeThreshold*imbalanceCoefficient
	score          int
	lastUpdateTime time.Time
}

func NewhotSpans() *hotSpanChecker {
	s := &hotSpanChecker{
		hotSpanGroups: make(map[replica.GroupID]map[common.DispatcherID]*hotSpan),
	}
	return s
}

func (s *hotSpanChecker) getOrCreateGroup(groupID replica.GroupID) map[common.DispatcherID]*hotSpan {
	group, ok := s.hotSpanGroups[groupID]
	if !ok {
		group = make(map[common.DispatcherID]*hotSpan)
		s.hotSpanGroups[groupID] = group
	}
	return group
}

func (s *hotSpanChecker) getBatchByGroup(groupID replica.GroupID, batchSize int) []replica.CheckResult[common.DispatcherID, *SpanReplication] {
	cache := make([]replica.CheckResult[common.DispatcherID, *SpanReplication], 0, batchSize)

	outdatedSpans := make([]*SpanReplication, 0)
	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(groupID)
	if EnableDynamicThreshold && groupID != replica.DefaultGroupID {
		totalEventSizePerSecond := float32(0)
		for _, span := range hotSpanCache {
			totalEventSizePerSecond += span.eventSizePerSecond
		}
		if totalEventSizePerSecond > 0 {
			avg := float32(totalEventSizePerSecond / float32(len(hotSpanCache)))
			for _, span := range hotSpanCache {
				span.writeThreshold = avg
				span.HintMaxSpanNum = uint64(span.eventSizePerSecond / avg)
			}
		}
	}

	for _, span := range hotSpanCache {
		if time.Since(span.lastUpdateTime) > clearTimeout*time.Second {
			outdatedSpans = append(outdatedSpans, span.SpanReplication)
		} else if span.score >= HotSpanScoreThreshold {
			cache = append(cache, replica.CheckResult[common.DispatcherID, *SpanReplication]{
				OpType:       replica.OpSplit,
				Replications: []*SpanReplication{span.SpanReplication},
			})
			if len(cache) >= batchSize {
				break
			}
		}
	}
	s.doClear(groupID, outdatedSpans...)

	if len(cache) == 0 && groupID != replica.DefaultGroupID && len(hotSpanCache) > 0 {
		// maybe merge all spans
		totalEventSizePerSecond := float32(0)
		result := replica.CheckResult[common.DispatcherID, *SpanReplication]{
			OpType:       replica.OpMerge,
			Replications: make([]*SpanReplication, 0, len(hotSpanCache)),
		}
		for _, span := range hotSpanCache {
			totalEventSizePerSecond += span.GetStatus().EventSizePerSecond
			result.Replications = append(result.Replications, span.SpanReplication)
		}
		if totalEventSizePerSecond < HotSpanScoreThreshold {
			cache = append(cache, result)
		}
	}
	return cache
}

func (s *hotSpanChecker) updatehotSpan(span *SpanReplication, status *heartbeatpb.TableSpanStatus) {
	if status.ComponentStatus != heartbeatpb.ComponentState_Working {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()
	hotSpanCache := s.getOrCreateGroup(span.groupID)
	if status.EventSizePerSecond < HotSpanScoreThreshold {
		if span, ok := hotSpanCache[span.ID]; ok && status.EventSizePerSecond < span.writeThreshold {
			if span.score > 0 {
				span.score--
			}
			if span.groupID == replica.DefaultGroupID && span.score == 0 {
				delete(hotSpanCache, span.ID)
			}
		}
		return
	}

	cache, ok := hotSpanCache[span.ID]
	if !ok {
		cache = &hotSpan{
			SpanReplication:      span,
			writeThreshold:       HotSpanScoreThreshold,
			imbalanceCoefficient: getImbalanceThreshold(span.groupID),
			score:                0,
		}
		hotSpanCache[span.ID] = cache
	}
	if status.EventSizePerSecond >= cache.writeThreshold*float32(cache.imbalanceCoefficient) {
		cache.score++
		cache.lastUpdateTime = time.Now()
	}
}

func (s *hotSpanChecker) clearhotSpansByGroup(groupID replica.GroupID, spans ...*SpanReplication) {
	// extract needClear method to simply the clear behavior
	if groupID != replica.DefaultGroupID {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.doClear(groupID, spans...)
}

func (s *hotSpanChecker) doClear(groupID replica.GroupID, spans ...*SpanReplication) {
	log.Info("clear hot spans", zap.String("group", replica.GetGroupName(groupID)), zap.Int("count", len(spans)))
	hotSpanCache := s.getOrCreateGroup(groupID)
	for _, span := range spans {
		delete(hotSpanCache, span.ID)
	}
}

func (s *hotSpanChecker) stat() string {
	s.lock.Lock()
	defer s.lock.Unlock()

	var res strings.Builder
	total := 0
	for groupID, hotSpanCache := range s.hotSpanGroups {
		if total > 0 {
			res.WriteString(" ")
		}
		res.WriteString(replica.GetGroupName(groupID))
		res.WriteString(": [")
		cnts := [HotSpanScoreThreshold + 1]int{}
		for _, span := range hotSpanCache {
			score := min(HotSpanScoreThreshold, span.score)
			cnts[score]++
			total++
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
		res.WriteString("] ")
	}
	if total == 0 {
		return "No hot spans"
	}
	return res.String()
}
