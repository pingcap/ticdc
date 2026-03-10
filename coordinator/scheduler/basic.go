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

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/node"
	pkgScheduler "github.com/pingcap/ticdc/pkg/scheduler"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
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
) *basicScheduler {
	return &basicScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		nodeManager:        appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
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
	nodeTaskSize := s.changefeedDB.GetTaskSizePerNodeByGroup(id)
	// add the absent node to the node size map
	nodeIDs := s.nodeManager.GetAliveNodeIDs()
	nodeSize := make(map[node.ID]int)
	for _, id := range nodeIDs {
		nodeSize[id] = nodeTaskSize[id]
	}

	pkgScheduler.BasicSchedule(availableSize, absentChangefeeds, nodeSize, func(cf *changefeed.Changefeed, nodeID node.ID) bool {
		return s.operatorController.AddOperator(operator.NewAddMaintainerOperator(s.changefeedDB, cf, nodeID))
	})
}

func (s *basicScheduler) Name() string {
	return "basic-scheduler"
}
