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
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
)

const drainScheduleInterval = 200 * time.Millisecond

// DrainController provides the minimal drain state and command operations
// required by drainScheduler.
type DrainController interface {
	GetDrainingNodes(now time.Time) []node.ID
	GetSchedulableDestNodes(now time.Time) map[node.ID]*node.Info
}

// drainScheduler migrates maintainers out of draining nodes.
type drainScheduler struct {
	batchSize int

	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	drainController    DrainController

	lastNodeCursor int
}

func NewDrainScheduler(
	batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	drainController DrainController,
) *drainScheduler {
	return &drainScheduler{
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		drainController:    drainController,
	}
}

func (s *drainScheduler) Name() string {
	return "drain-scheduler"
}

func (s *drainScheduler) Execute() time.Time {
	if s.batchSize <= 0 {
		return time.Now().Add(drainScheduleInterval)
	}
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if availableSize <= 0 {
		return time.Now().Add(drainScheduleInterval)
	}

	now := time.Now()
	drainingNodes := s.drainController.GetDrainingNodes(now)
	if len(drainingNodes) == 0 {
		return now.Add(drainScheduleInterval)
	}
	schedulableNodes := s.drainController.GetSchedulableDestNodes(now)
	if len(schedulableNodes) == 0 {
		return now.Add(drainScheduleInterval)
	}

	nodeLoads := s.changefeedDB.GetTaskSizePerNode()
	created := 0
	for i := 0; i < len(drainingNodes) && created < availableSize; i++ {
		nodeIdx := (s.lastNodeCursor + i) % len(drainingNodes)
		targetNode := drainingNodes[nodeIdx]

		for _, cf := range s.changefeedDB.GetByNodeID(targetNode) {
			if created >= availableSize {
				break
			}
			if s.operatorController.GetOperator(cf.ID) != nil {
				continue
			}
			dest, ok := chooseLeastLoadedNode(schedulableNodes, targetNode, nodeLoads)
			if !ok {
				break
			}
			if s.operatorController.AddOperator(operator.NewMoveMaintainerOperator(s.changefeedDB, cf, targetNode, dest)) {
				nodeLoads[dest]++
				created++
			}
		}
	}
	s.lastNodeCursor = (s.lastNodeCursor + 1) % len(drainingNodes)
	return now.Add(drainScheduleInterval)
}

func chooseLeastLoadedNode(
	schedulableNodes map[node.ID]*node.Info,
	excluded node.ID,
	nodeLoads map[node.ID]int,
) (node.ID, bool) {
	var (
		selected node.ID
		found    bool
		minLoad  int
	)
	for id := range schedulableNodes {
		if id == excluded {
			continue
		}
		load := nodeLoads[id]
		if !found || load < minLoad {
			selected = id
			minLoad = load
			found = true
		}
	}
	return selected, found
}
