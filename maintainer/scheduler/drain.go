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
	"slices"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/maintainer/operator"
	"github.com/pingcap/ticdc/maintainer/replica"
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
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager
	mode               int64

	drainState *DrainState

	lastDrainTarget     node.ID
	lastDrainEpoch      uint64
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

// Execute moves a bounded number of dispatchers away from the active drain
// target. Destination selection excludes the target node and prefers the least
// loaded alive node based on the current task count snapshot.
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
	slices.SortFunc(replications, func(a, b *replica.SpanReplication) int {
		// Use a stable order so repeated drain rounds make deterministic progress
		// even when multiple spans are otherwise equivalent candidates.
		if a.ID.Less(b.ID) {
			return -1
		}
		if b.ID.Less(a.ID) {
			return 1
		}
		return 0
	})

	nodeTaskSize := s.spanController.GetTaskSizePerNode()
	scheduled := 0
	for _, replication := range replications {
		if scheduled >= availableSize {
			break
		}
		if replication.GetNodeID() != target {
			continue
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

func (s *drainScheduler) resetDrainSessionLimit() {
	s.lastDrainTarget = ""
	s.lastDrainEpoch = 0
	s.fixedDrainMoveLimit = 0
}

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

func calculateDrainMoveLimit(initialTargetDispatcherCount int) int {
	if initialTargetDispatcherCount <= 0 {
		return maxDrainMovePerRound
	}
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
