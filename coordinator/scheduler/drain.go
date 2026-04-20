// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/coordinator/operator"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// drainScheduler generates move operators to move maintainers out of draining nodes.
// It skips changefeeds that already have in-flight operators.
type drainScheduler struct {
	id        string
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager
	liveness           *drain.Controller

	// rrCursor rotates the starting draining node to avoid starving nodes later in the list.
	rrCursor int
}

// NewDrainScheduler creates a scheduler that migrates maintainers away from draining nodes.
func NewDrainScheduler(
	id string,
	batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	liveness *drain.Controller,
) *drainScheduler {
	return &drainScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		liveness:           liveness,
	}
}

// Execute schedules move operators from draining nodes to schedulable destination nodes.
// It limits drain scheduling by drain move slots and returns the next run time.
func (s *drainScheduler) Execute() time.Time {
	if s.liveness == nil {
		return time.Now().Add(time.Second)
	}

	now := time.Now()
	drainingNodes := s.liveness.GetDrainingOrStoppingNodes()
	if len(drainingNodes) == 0 {
		return now.Add(time.Second)
	}
	slices.Sort(drainingNodes)
	availableSize := s.batchSize - s.operatorController.CountMoveMaintainerOperatorsFromNodes(drainingNodes)
	if availableSize <= 0 {
		return now.Add(time.Millisecond * 200)
	}

	destCandidates := filterSchedulableNodeIDs(s.nodeManager.GetAliveNodeIDs(), s.liveness)

	if len(destCandidates) == 0 {
		log.Info("no alive destination node for drain",
			zap.String("schedulerID", s.id),
			zap.Int("drainingNodeCount", len(drainingNodes)))
		return now.Add(time.Second)
	}

	nodeTaskSize := s.changefeedDB.GetTaskSizePerNode()
	maintainersByNode := make(map[node.ID][]*changefeed.Changefeed, len(drainingNodes))
	nextMaintainerIndex := make(map[node.ID]int, len(drainingNodes))
	for _, origin := range drainingNodes {
		maintainersByNode[origin] = s.changefeedDB.GetByNodeID(origin)
	}
	scheduled := 0

	if s.rrCursor >= len(drainingNodes) {
		s.rrCursor = 0
	}

	for scheduled < availableSize {
		progress := false
		for i := 0; i < len(drainingNodes) && scheduled < availableSize; i++ {
			origin := drainingNodes[(s.rrCursor+i)%len(drainingNodes)]
			nextIndex, ok := s.scheduleOneFromNode(
				origin,
				maintainersByNode[origin],
				nextMaintainerIndex[origin],
				destCandidates,
				nodeTaskSize,
			)
			nextMaintainerIndex[origin] = nextIndex
			if ok {
				scheduled++
				progress = true
			}
		}
		s.rrCursor = (s.rrCursor + 1) % len(drainingNodes)
		if !progress {
			break
		}
	}

	if scheduled > 0 {
		log.Info("drain scheduler created move operators",
			zap.Int("scheduled", scheduled),
			zap.Int("drainingNodeCount", len(drainingNodes)))
	}

	return now.Add(time.Millisecond * 200)
}

// scheduleOneFromNode tries to schedule one maintainer move from origin,
// continuing from nextIndex within the pre-fetched maintainer slice. It skips
// changefeeds that already have in-flight operators.
func (s *drainScheduler) scheduleOneFromNode(
	origin node.ID,
	maintainers []*changefeed.Changefeed,
	nextIndex int,
	destCandidates []node.ID,
	nodeTaskSize map[node.ID]int,
) (int, bool) {
	for nextIndex < len(maintainers) {
		cf := maintainers[nextIndex]
		nextIndex++
		if s.operatorController.HasOperator(cf.ID.DisplayName) {
			continue
		}
		dest, ok := chooseLeastLoadedDest(origin, destCandidates, nodeTaskSize)
		if !ok {
			return nextIndex, false
		}
		if s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, origin, dest)) {
			nodeTaskSize[dest]++
			return nextIndex, true
		}
	}
	return nextIndex, false
}

// chooseLeastLoadedDest selects the destination with the smallest task count, excluding origin.
func chooseLeastLoadedDest(
	origin node.ID,
	destCandidates []node.ID,
	nodeTaskSize map[node.ID]int,
) (node.ID, bool) {
	minSize := math.MaxInt
	var chosen node.ID
	for _, id := range destCandidates {
		if id == origin {
			continue
		}
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

// Name returns the scheduler name used by scheduler controller and logs.
func (s *drainScheduler) Name() string {
	return "drain-scheduler"
}
