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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler struct {
	id        string
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	nodeManager        *watcher.NodeManager
}

func NewBasicScheduler(
	id string, batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	nodeManager *watcher.NodeManager,
) *basicScheduler {
	return &basicScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        nodeManager,
	}
}

// Execute periodically execute the operator
func (s *basicScheduler) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if s.changefeedDB.GetAbsentSize() <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	s.doBasicSchedule(availableSize)

	return time.Now().Add(time.Millisecond * 500)
}

func (s *basicScheduler) doBasicSchedule(availableSize int) {
	id := replica.DefaultGroupID

	absentChangefeeds := s.changefeedDB.GetAbsentByGroup(id, availableSize)
	nodeSize := s.changefeedDB.GetTaskSizePerNodeByGroup(id)
	aliveNodes := s.nodeManager.GetAliveNodes()

	// Clean up nodeSize to only include alive nodes
	cleanNodeSize := make(map[node.ID]int)
	for nodeID, size := range nodeSize {
		if _, isAlive := aliveNodes[nodeID]; isAlive {
			cleanNodeSize[nodeID] = size
		}
	}

	// Add alive nodes that are not in nodeSize
	for nodeID := range aliveNodes {
		if _, ok := cleanNodeSize[nodeID]; !ok {
			cleanNodeSize[nodeID] = 0
		}
	}

	pkgScheduler.BasicSchedule(availableSize, absentChangefeeds, cleanNodeSize, func(cf *changefeed.Changefeed, nodeID node.ID) bool {
		// Double-check that the target node is still alive before creating the operator
		if _, isAlive := aliveNodes[nodeID]; !isAlive {
			log.Warn("scheduler: skip creating operator for offline node",
				zap.String("changefeed", cf.ID.String()),
				zap.String("nodeID", nodeID.String()))
			return false
		}
		return s.operatorController.AddOperator(operator.NewAddMaintainerOperator(s.changefeedDB, cf, nodeID))
	})
}

func (s *basicScheduler) Name() string {
	return "basic-scheduler"
}
