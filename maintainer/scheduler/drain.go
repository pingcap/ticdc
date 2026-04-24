// Copyright 2026 PingCAP, Inc.
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
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// maxDrainMovePerRound is the minimum concurrent in-flight drain move limit
// for one drain epoch. Larger targets may use a higher fixed limit derived from
// the dispatcher count observed when the current epoch first becomes schedulable.
const maxDrainMovePerRound = 10

// drainScheduler evacuates dispatchers from the active drain target node.
// It only creates move operators after the changefeed maintainer itself is no
// longer hosted on the target node, which preserves the intended drain order.
type drainScheduler struct {
	// changefeedID is used in logs so drain activity can be tied back to one
	// changefeed during node evacuation.
	changefeedID common.ChangeFeedID
	// batchSize caps how many new drain move operators one Execute round may
	// create even when the fixed drain concurrency limit is larger.
	batchSize int

	// operatorController owns move operator lifecycle and exposes the current
	// in-flight drain move count for the target node.
	operatorController *operator.Controller
	// spanController provides the current dispatcher placement that drain
	// scheduling reads and updates through move operators.
	spanController *span.Controller
	// nodeManager provides the current alive destination nodes.
	nodeManager *watcher.NodeManager
	// mode distinguishes default and redo schedulers in shared logs.
	mode int64

	// drainState is the shared per-changefeed drain snapshot maintained by the
	// controller and read by all drain-aware schedulers.
	drainState *DrainState

	// lastDrainTarget and lastDrainEpoch identify the drain session whose fixed
	// concurrency limit is cached below.
	lastDrainTarget node.ID
	lastDrainEpoch  uint64
	// fixedDrainMoveLimit is computed once per drain epoch from the target size
	// observed when the epoch first becomes schedulable.
	fixedDrainMoveLimit int
}

// NewDrainScheduler creates a scheduler that drains one target node at a time
// using the latest drain target snapshot supplied by the controller layer.
func NewDrainScheduler(
	changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	spanController *span.Controller,
	mode int64,
	drainState *DrainState,
) *drainScheduler {
	return &drainScheduler{
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		operatorController: oc,
		spanController:     spanController,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		mode:               mode,
		drainState:         drainState,
	}
}

// Execute advances drain in four steps:
//  1. Read one consistent drain snapshot and exit early when drain is inactive
//     or the maintainer still lives on the target node.
//  2. Derive how many additional drain moves this round may create from the
//     fixed per-epoch limit and the current in-flight drain move count.
//  3. Select alive destination nodes excluding the drain target.
//  4. Walk current target dispatchers and create move operators until the round
//     budget is exhausted.
//
// Drain semantics only require bounded progress away from the target node. The
// dispatcher iteration order is therefore not part of correctness and we keep
// the hot path free of extra sorting work.
func (s *drainScheduler) Execute() time.Time {
	state := s.drainState.snapshot()
	target, targetEpoch, active := activeDrainTarget(state)
	if !active {
		s.resetDrainSessionLimit()
		return time.Now().Add(time.Millisecond * 500)
	}
	if state.selfNodeID == target {
		// Keep per-changefeed order by moving maintainer first.
		return time.Now().Add(time.Millisecond * 500)
	}
	s.ensureDrainSessionLimit(target, targetEpoch)

	drainSlots := s.fixedDrainMoveLimit - s.operatorController.CountInflightDrainMovesFromNode(target)
	if drainSlots <= 0 {
		return time.Now().Add(time.Millisecond * 200)
	}
	availableSize := drainSlots
	if availableSize > s.batchSize {
		availableSize = s.batchSize
	}

	destCandidates := filterNodeIDsByDrainTarget(
		s.nodeManager.GetAliveNodeIDs(),
		state,
	)
	if len(destCandidates) == 0 {
		return time.Now().Add(time.Second)
	}

	replications := s.spanController.GetTaskByNodeID(target)
	if len(replications) == 0 {
		return time.Now().Add(time.Millisecond * 500)
	}

	nodeTaskSize := s.spanController.GetTaskSizePerNode()
	scheduled := 0
	for _, replication := range replications {
		if scheduled >= availableSize {
			break
		}
		if s.operatorController.GetOperator(replication.ID) != nil {
			continue
		}

		dest, ok := chooseLeastLoadedNode(destCandidates, nodeTaskSize)
		if !ok {
			break
		}

		if s.operatorController.AddOperator(
			operator.NewMoveDispatcherOperator(s.spanController, replication, target, dest),
		) {
			nodeTaskSize[target]--
			nodeTaskSize[dest]++
			scheduled++
		}
	}

	if scheduled > 0 {
		log.Info("drain scheduler created move operators",
			zap.Stringer("changefeedID", s.changefeedID),
			zap.Int("scheduledCount", scheduled),
			zap.String("targetNodeID", target.String()),
			zap.Uint64("targetEpoch", targetEpoch),
			zap.String("mode", common.StringMode(s.mode)))
	}
	return time.Now().Add(time.Millisecond * 200)
}

// resetDrainSessionLimit clears the cached fixed concurrency limit once drain
// is no longer active for this scheduler.
func (s *drainScheduler) resetDrainSessionLimit() {
	s.lastDrainTarget = ""
	s.lastDrainEpoch = 0
	s.fixedDrainMoveLimit = 0
}

// ensureDrainSessionLimit initializes the fixed concurrency limit for a new
// drain epoch and reuses the cached value while the same epoch remains active.
func (s *drainScheduler) ensureDrainSessionLimit(target node.ID, epoch uint64) {
	if s.lastDrainTarget == target && s.lastDrainEpoch == epoch && s.fixedDrainMoveLimit > 0 {
		return
	}

	initialTargetDispatcherCount := s.spanController.GetTaskSizeByNodeID(target)
	s.lastDrainTarget = target
	s.lastDrainEpoch = epoch
	s.fixedDrainMoveLimit = calculateDrainMoveLimit(initialTargetDispatcherCount)

	log.Info("drain scheduler initialized fixed move limit",
		zap.Stringer("changefeedID", s.changefeedID),
		zap.String("targetNodeID", target.String()),
		zap.Uint64("targetEpoch", epoch),
		zap.Int("initialTargetDispatcherCount", initialTargetDispatcherCount),
		zap.Int("fixedDrainMoveLimit", s.fixedDrainMoveLimit),
		zap.String("mode", common.StringMode(s.mode)))
}

// calculateDrainMoveLimit keeps at least maxDrainMovePerRound concurrent
// in-flight drain moves for small targets. Larger targets use the ceiling of
// 1% of the initial dispatcher count captured for the active drain epoch.
// Execute still caps each scheduling round by batchSize.
func calculateDrainMoveLimit(initialTargetDispatcherCount int) int {
	// Add 99 before dividing by 100 so integer division computes ceil(n/100),
	// which preserves roughly 1% concurrency once the target exceeds 1000
	// dispatchers instead of rounding smaller fractions down to zero.
	limit := (initialTargetDispatcherCount + 99) / 100
	if limit < maxDrainMovePerRound {
		return maxDrainMovePerRound
	}
	return limit
}

// chooseLeastLoadedNode picks the alive destination with the smallest current
// task count so the drain scheduler does not create avoidable skew while
// evacuating the target node.
func chooseLeastLoadedNode(
	destCandidates []node.ID,
	nodeTaskSize map[node.ID]int,
) (node.ID, bool) {
	minSize := math.MaxInt
	var chosen node.ID
	for _, id := range destCandidates {
		size := nodeTaskSize[id]
		if size < minSize {
			minSize = size
			chosen = id
		}
	}
	if chosen.IsEmpty() {
		return "", false
	}
	return chosen, true
}

func (s *drainScheduler) Name() string {
	if common.IsRedoMode(s.mode) {
		return pkgScheduler.RedoDrainScheduler
	}
	return pkgScheduler.DrainScheduler
}
