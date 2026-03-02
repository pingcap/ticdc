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

const maxDrainMovePerRound = 2

// drainScheduler moves dispatchers away from the active drain target node.
type drainScheduler struct {
	changefeedID common.ChangeFeedID
	batchSize    int

	operatorController *operator.Controller
	spanController     *span.Controller
	nodeManager        *watcher.NodeManager
	mode               int64

	getDrainTarget drainTargetGetter
	getSelfNodeID  func() node.ID
}

func NewDrainScheduler(
	changefeedID common.ChangeFeedID,
	batchSize int,
	oc *operator.Controller,
	spanController *span.Controller,
	mode int64,
	getDrainTarget drainTargetGetter,
	getSelfNodeID func() node.ID,
) *drainScheduler {
	return &drainScheduler{
		changefeedID:       changefeedID,
		batchSize:          batchSize,
		operatorController: oc,
		spanController:     spanController,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		mode:               mode,
		getDrainTarget:     getDrainTarget,
		getSelfNodeID:      getSelfNodeID,
	}
}

func (s *drainScheduler) Execute() time.Time {
	target, targetEpoch, active := snapshotDrainTarget(s.getDrainTarget)
	if !active {
		return time.Now().Add(time.Millisecond * 500)
	}
	if s.getSelfNodeID != nil && s.getSelfNodeID() == target {
		// Keep per-changefeed order by moving maintainer first.
		return time.Now().Add(time.Millisecond * 500)
	}

	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if availableSize <= 0 {
		return time.Now().Add(time.Millisecond * 200)
	}
	drainSlots := maxDrainMovePerRound - s.countInflightDrainMoves(target)
	if drainSlots <= 0 {
		return time.Now().Add(time.Millisecond * 200)
	}
	if availableSize > drainSlots {
		availableSize = drainSlots
	}

	destCandidates := s.nodeManager.GetAliveNodeIDs()
	filteredDest := destCandidates[:0]
	for _, id := range destCandidates {
		if id == target {
			continue
		}
		filteredDest = append(filteredDest, id)
	}
	destCandidates = filteredDest
	if len(destCandidates) == 0 {
		return time.Now().Add(time.Second)
	}

	replications := s.spanController.GetTaskByNodeID(target)
	if len(replications) == 0 {
		return time.Now().Add(time.Millisecond * 500)
	}
	slices.SortFunc(replications, func(a, b *replica.SpanReplication) int {
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

func (s *drainScheduler) countInflightDrainMoves(target node.ID) int {
	count := 0
	for _, op := range s.operatorController.GetAllOperators() {
		moveOp, ok := op.(*operator.MoveDispatcherOperator)
		if !ok {
			continue
		}
		if moveOp.IsFinished() {
			continue
		}
		if moveOp.OriginNode() != target {
			continue
		}
		count++
	}
	return count
}

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
