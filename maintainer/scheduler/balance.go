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

package scheduler

import (
	"context"
	"maps"
	"math/rand"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	pkgReplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	pkgreplica "github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
)

// balanceScheduler mainly focus on two types of operations:
// 1. Split operations: splits the one large span for the whole table into smaller ones.
// 2. Move operations: balances node traffic and span counts without splitting tables.
type balanceScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager

	splitter             *split.Splitter
	checkBalanceInterval time.Duration

	random *rand.Rand
	mode   int64

	drainState *DrainState

	drainBalanceBlockedUntil time.Time
	trafficMovePending       bool
	trafficBalanceNotBefore  time.Time
}

func NewBalanceScheduler(
	changefeedID common.ChangeFeedID,
	splitter *split.Splitter,
	oc *operator.Controller,
	sc *span.Controller,
	checkBalanceInterval time.Duration,
	mode int64,
	drainState *DrainState,
	moveBatchSize int,
) *balanceScheduler {
	return &balanceScheduler{
		changefeedID:         changefeedID,
		batchSize:            moveBatchSize,
		random:               rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404
		operatorController:   oc,
		spanController:       sc,
		nodeManager:          appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		splitter:             splitter,
		checkBalanceInterval: checkBalanceInterval,
		mode:                 mode,
		drainState:           drainState,
	}
}

func (s *balanceScheduler) Execute() time.Time {
	now := time.Now()
	nextCheckTime := now.Add(s.checkBalanceInterval)
	if s.spanController.NodeLoadTracker() != nil && s.checkBalanceInterval > 0 {
		// Independent maintainers should not repeatedly act on the same sample
		// at the same time after bootstrap or a cluster-wide workload change.
		nextCheckTime = nextCheckTime.Add(time.Duration(s.random.Int63n(int64(s.checkBalanceInterval/2) + 1)))
	}
	failpoint.Inject("StopBalanceScheduler", func() {
		failpoint.Return(nextCheckTime)
	})
	state := s.drainState.snapshot()
	if shouldPauseBalanceForDrain(state, now, &s.drainBalanceBlockedUntil) {
		// Pause regular balance scheduling while dispatcher drain is active
		// and keep a cooldown window after drain completion to avoid churn.
		return nextCheckTime
	}
	// TODO: consider to ignore split tables' dispatcher basic schedule operator to decide whether we can make balance schedule
	if s.operatorController.OperatorSize() > 0 || s.spanController.GetAbsentSize() > 0 {
		// not in stable schedule state, skip balance
		return nextCheckTime
	}
	if s.trafficMovePending {
		// Start the observation window after the operator finishes, so a slow
		// move cannot consume the cooldown before its new placement is sampled.
		s.trafficMovePending = false
		s.trafficBalanceNotBefore = now.Add(max(s.checkBalanceInterval, replica.NodeLoadStaleThreshold))
	}

	// 1. check whether we have spans in defaultGroupID need to be splitted.
	//    we only consider the not splitted span here.
	checkResults := s.spanController.CheckByGroup(pkgReplica.DefaultGroupID, s.batchSize)
	count := s.doSplit(checkResults)

	// to many split operators, do move operator later
	if count >= s.batchSize {
		return nextCheckTime
	}

	moveBudget := s.batchSize - count

	// 2. do balance for the spans in defaultGroupID
	s.schedulerDefaultGroup(moveBudget, state)

	return nextCheckTime
}

func (s *balanceScheduler) Name() string {
	if common.IsRedoMode(s.mode) {
		return pkgScheduler.RedoBalanceScheduler
	}
	return pkgScheduler.BalanceScheduler
}

func (s *balanceScheduler) schedulerDefaultGroup(
	maxSize int,
	state drainStateSnapshot,
) int {
	if maxSize <= 0 || time.Now().Before(s.trafficBalanceNotBefore) {
		return 0
	}
	nodes := s.nodeManager.GetAliveNodes()
	nodes = filterAliveNodesByDrainTarget(nodes, state)
	if len(nodes) < 2 {
		return 0
	}
	group := pkgreplica.DefaultGroupID
	now := time.Now()
	loads, status := s.spanController.NodeLoadTracker().Snapshot(nodes, now)
	if status == heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE {
		return 0
	}
	moveSize := pkgScheduler.CheckBalanceStatus(s.spanController.GetTaskSizePerNodeByGroup(group), nodes)
	average, overloaded := float64(0), false
	if status == heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE {
		for id := range nodes {
			average += loads[id]
		}
		average /= float64(len(nodes))
		for id := range nodes {
			if loads[id] > average*1.2 {
				overloaded = true
				break
			}
		}
	}
	if moveSize <= 0 && !overloaded {
		// The common balanced or idle case must not scan every dispatcher.
		return 0
	}
	replicas := s.spanController.GetReplicatingByGroup(group)
	if status == heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE {
		// Project only this maintainer's accepted moves. Other maintainers keep
		// using the coordinator snapshot until the next node sample arrives.
		loads = maps.Clone(loads)
		if overloaded && s.balanceTraffic(nodes, replicas, loads, average, now) {
			s.trafficMovePending = true
			return 1
		}
	}
	// fast path, check the balance status
	if moveSize <= 0 {
		return 0
	}
	return pkgScheduler.Balance(maxSize, s.random, nodes, replicas, func(r *replica.SpanReplication, target node.ID) bool {
		if status == heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE {
			rate := r.TrafficRate(now)
			if rate == 0 || (rate > 1 && !improvesNodeLoad(loads[r.GetNodeID()], loads[target], rate)) {
				return false
			}
			if rate == 1 {
				rate = 0 // The idle sentinel contributes no actual traffic.
			}
			if !s.doMove(r, target) {
				return false
			}
			loads[r.GetNodeID()] = max(0, loads[r.GetNodeID()]-rate)
			loads[target] += rate
			return true
		}
		return s.doMove(r, target)
	})
}

// improvesNodeLoad requires a meaningful reduction in the pair's imbalance.
// A single indivisible hot table must not bounce between otherwise idle nodes.
func improvesNodeLoad(source, target, rate float64) bool {
	return rate > 0 && rate < (source-target)*0.9
}

func (s *balanceScheduler) balanceTraffic(
	nodes map[node.ID]*node.Info,
	replicas []*replica.SpanReplication,
	loads map[node.ID]float64,
	average float64,
	now time.Time,
) bool {
	// Randomize both dispatcher and destination choice so independent
	// maintainers can spread their moves across eligible idle nodes.
	s.random.Shuffle(len(replicas), func(i, j int) { replicas[i], replicas[j] = replicas[j], replicas[i] })
	for _, r := range replicas {
		source := r.GetNodeID()
		if _, alive := nodes[source]; !alive || !r.IsWorking() || loads[source] <= average*1.2 {
			continue
		}
		rate := r.TrafficRate(now)
		if rate <= 1 {
			continue
		}
		var target node.ID
		candidates := 0
		for id := range nodes {
			if id == source || loads[id] >= average || !improvesNodeLoad(loads[source], loads[id], rate) {
				continue
			}
			candidates++
			if s.random.Intn(candidates) == 0 {
				target = id
			}
		}
		if candidates > 0 && s.doMove(r, target) {
			return true
		}
	}
	return false
}

func (s *balanceScheduler) doSplit(results pkgReplica.GroupCheckResult) int {
	if results == nil {
		return 0
	}
	checkResults := results.([]replica.DefaultSpanSplitCheckResult)
	splitCount := 0
	for _, result := range checkResults {
		spansNum := max(result.SpanNum, len(s.nodeManager.GetAliveNodes())*2)
		splitSpans := s.splitter.Split(context.Background(), result.Span.Span, spansNum, result.SpanType)
		if len(splitSpans) > 1 {
			op := operator.NewSplitDispatcherOperator(
				s.spanController,
				result.Span,
				splitSpans,
				[]node.ID{},
				s.operatorController.MaintainerEpoch(),
				nil,
			)
			ret := s.operatorController.AddOperator(op)
			if ret {
				splitCount++
			}
		}
	}

	return splitCount
}

func (s *balanceScheduler) doMove(replication *replica.SpanReplication, id node.ID) bool {
	op := s.operatorController.NewMoveOperator(replication, replication.GetNodeID(), id)
	return s.operatorController.AddOperator(op)
}
