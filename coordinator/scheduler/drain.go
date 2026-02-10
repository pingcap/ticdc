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
	"sort"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

type drainingNodeChecker interface {
	IsDraining(node.ID) bool
}

// drainScheduler moves maintainers out of draining nodes.
type drainScheduler struct {
	id        string
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager
	destSelector       DestNodeSelector
	liveness           drainingNodeChecker

	nextStartIndex int
}

func NewDrainScheduler(
	id string,
	batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	nodeManager *watcher.NodeManager,
	destSelector DestNodeSelector,
	liveness drainingNodeChecker,
) *drainScheduler {
	return &drainScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        nodeManager,
		destSelector:       destSelector,
		liveness:           liveness,
	}
}

func (s *drainScheduler) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if availableSize <= 0 {
		return time.Now().Add(time.Millisecond * 200)
	}

	drainingNodes := s.getDrainingNodes()
	if len(drainingNodes) == 0 {
		return time.Now().Add(time.Millisecond * 500)
	}

	activeNodes := s.destSelector.GetSchedulableDestNodes()
	if len(activeNodes) == 0 {
		log.Info("drain scheduler has no schedulable destination nodes")
		return time.Now().Add(time.Millisecond * 500)
	}

	nodeTaskSize := s.changefeedDB.GetTaskSizePerNode()

	startIndex := 0
	if s.nextStartIndex < len(drainingNodes) {
		startIndex = s.nextStartIndex
	}

	scheduled := 0
	cursor := startIndex
	for scheduled < availableSize {
		progressed := false
		for i := 0; i < len(drainingNodes) && scheduled < availableSize; i++ {
			nodeID := drainingNodes[cursor]
			cursor++
			if cursor >= len(drainingNodes) {
				cursor = 0
			}

			if s.scheduleOneMove(nodeID, activeNodes, nodeTaskSize) {
				scheduled++
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	s.nextStartIndex = cursor

	if scheduled > 0 {
		return time.Now().Add(time.Millisecond * 200)
	}
	return time.Now().Add(time.Millisecond * 500)
}

func (s *drainScheduler) scheduleOneMove(
	drainingNode node.ID,
	activeNodes map[node.ID]*node.Info,
	nodeTaskSize map[node.ID]int,
) bool {
	changefeeds := s.changefeedDB.GetByNodeID(drainingNode)
	if len(changefeeds) == 0 {
		return false
	}

	for _, cf := range changefeeds {
		// Skip changefeeds with in-flight operators.
		if s.operatorController.GetOperator(cf.ID) != nil {
			continue
		}

		dest := selectLeastLoadedNode(activeNodes, nodeTaskSize)
		if dest.IsEmpty() {
			log.Info("drain scheduler cannot find a destination node",
				zap.Stringer("drainingNode", drainingNode))
			return false
		}

		if s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, drainingNode, dest)) {
			nodeTaskSize[dest]++
			return true
		}
	}

	return false
}

func selectLeastLoadedNode(activeNodes map[node.ID]*node.Info, nodeTaskSize map[node.ID]int) node.ID {
	selected := node.ID("")
	minLoad := math.MaxInt
	for id := range activeNodes {
		load := nodeTaskSize[id]
		if load < minLoad {
			selected = id
			minLoad = load
		}
	}
	return selected
}

func (s *drainScheduler) getDrainingNodes() []node.ID {
	all := s.nodeManager.GetAliveNodeIDs()
	if len(all) == 0 {
		return nil
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].String() < all[j].String()
	})

	draining := make([]node.ID, 0, len(all))
	for _, id := range all {
		if s.liveness.IsDraining(id) {
			draining = append(draining, id)
		}
	}
	return draining
}

func (s *drainScheduler) Name() string {
	return "drain-scheduler"
}
