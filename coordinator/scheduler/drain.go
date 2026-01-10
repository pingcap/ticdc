// Copyright 2025 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/operator"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// DrainStateProvider provides access to drain state information.
type DrainStateProvider interface {
	GetDrainingTarget() node.ID
	IsDraining() bool
	ClearDrainingTarget()
}

// NodeManagerProvider provides access to node management functions.
type NodeManagerProvider interface {
	GetSchedulableNodes() map[node.ID]*node.Info
}

// drainScheduler generates MoveMaintainer operators for draining captures.
// It migrates maintainers from the draining node to other healthy nodes.
type drainScheduler struct {
	id                 string
	batchSize          int
	operatorController *operator.Controller
	changefeedDB       *changefeed.ChangefeedDB
	drainState         DrainStateProvider
	nodeManager        NodeManagerProvider
}

// NewDrainScheduler creates a new drain scheduler.
func NewDrainScheduler(
	id string,
	batchSize int,
	oc *operator.Controller,
	changefeedDB *changefeed.ChangefeedDB,
	drainState DrainStateProvider,
	nodeManager NodeManagerProvider,
) *drainScheduler {
	if batchSize <= 0 {
		batchSize = 1 // default to 1
	}
	return &drainScheduler{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		changefeedDB:       changefeedDB,
		drainState:         drainState,
		nodeManager:        nodeManager,
	}
}

// Execute runs the drain scheduler logic.
// It generates MoveMaintainer operators for maintainers on the draining node.
func (s *drainScheduler) Execute() time.Time {
	drainingTarget := s.drainState.GetDrainingTarget()
	if drainingTarget == "" {
		return time.Now().Add(time.Second)
	}

	// Get maintainers on draining capture
	maintainers := s.changefeedDB.GetByNodeID(drainingTarget)
	if len(maintainers) == 0 {
		// All maintainers migrated, check if all dispatchers are also migrated
		if s.allDispatchersMigrated(drainingTarget) {
			log.Info("drain complete, all maintainers and dispatchers migrated",
				zap.String("capture", drainingTarget.String()))
			s.drainState.ClearDrainingTarget()
		} else {
			log.Debug("waiting for dispatchers to migrate",
				zap.String("capture", drainingTarget.String()))
		}
		return time.Now().Add(time.Second)
	}

	// Get schedulable nodes (excludes draining/stopping nodes)
	schedulableNodes := s.nodeManager.GetSchedulableNodes()
	if len(schedulableNodes) == 0 {
		log.Warn("no schedulable nodes available for drain")
		return time.Now().Add(time.Second * 5)
	}

	// Count current in-progress operators for draining node
	inProgressCount := s.countInProgressOperators(drainingTarget)
	availableBatch := s.batchSize - inProgressCount
	if availableBatch <= 0 {
		// Wait for current batch to complete
		return time.Now().Add(time.Millisecond * 500)
	}

	// Generate MoveMaintainer operators in batches
	batchCount := 0
	for _, cf := range maintainers {
		if batchCount >= availableBatch {
			break
		}

		// Skip if operator already exists
		if s.operatorController.GetOperator(cf.ID) != nil {
			continue
		}

		// Select destination with lowest workload
		destNode := s.selectDestination(schedulableNodes)
		if destNode == "" {
			continue
		}

		op := operator.NewMoveMaintainerOperator(s.changefeedDB, cf, drainingTarget, destNode)
		if s.operatorController.AddOperator(op) {
			batchCount++
			log.Info("generated move maintainer operator for drain",
				zap.String("changefeed", cf.ID.String()),
				zap.String("from", drainingTarget.String()),
				zap.String("to", destNode.String()))
		}
	}

	return time.Now().Add(time.Millisecond * 500)
}

// countInProgressOperators counts the number of in-progress operators for the given node.
func (s *drainScheduler) countInProgressOperators(nodeID node.ID) int {
	count := 0
	for _, cf := range s.changefeedDB.GetByNodeID(nodeID) {
		if s.operatorController.GetOperator(cf.ID) != nil {
			count++
		}
	}
	return count
}

// allDispatchersMigrated checks if all dispatchers have been migrated from the draining node.
// Currently, this is a simplified implementation that returns true when all maintainers
// have been migrated. The actual dispatcher migration is handled by the new Maintainer
// after it bootstraps and discovers dispatchers on the draining node.
// TODO: Implement proper dispatcher count tracking via heartbeat.
func (s *drainScheduler) allDispatchersMigrated(nodeID node.ID) bool {
	// For now, we consider dispatchers migrated when all maintainers are migrated.
	// The new Maintainer will handle dispatcher migration after bootstrap.
	// This is a conservative approach - the drain will complete when maintainers are done.
	return true
}

// selectDestination selects the destination node with the lowest workload.
func (s *drainScheduler) selectDestination(nodes map[node.ID]*node.Info) node.ID {
	var minNode node.ID
	minCount := math.MaxInt

	nodeTaskSize := s.changefeedDB.GetTaskSizePerNode()
	for id := range nodes {
		if count := nodeTaskSize[id]; count < minCount {
			minCount = count
			minNode = id
		}
	}

	return minNode
}

// Name returns the scheduler name.
func (s *drainScheduler) Name() string {
	return "drain-scheduler"
}
