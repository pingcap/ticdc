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

package coordinator

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

// DrainState tracks the drain operation state in coordinator.
// It ensures only one drain operation can be in progress at a time.
type DrainState struct {
	mu sync.RWMutex

	// drainingTarget is the capture ID being drained, empty if no drain in progress
	drainingTarget node.ID

	// startTime is when the drain operation started
	startTime time.Time

	// initialMaintainerCount is the count when drain started
	initialMaintainerCount int

	// initialDispatcherCount is the count when drain started
	initialDispatcherCount int

	// nodeManager is used to update node liveness
	nodeManager *watcher.NodeManager
}

// NewDrainState creates a new DrainState instance.
func NewDrainState(nodeManager *watcher.NodeManager) *DrainState {
	return &DrainState{
		nodeManager: nodeManager,
	}
}

// SetDrainingTarget sets the draining target and updates node liveness.
// Returns error if another drain operation is in progress.
func (s *DrainState) SetDrainingTarget(target node.ID, maintainerCount, dispatcherCount int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.drainingTarget != "" {
		return errors.New("another drain operation is in progress")
	}

	s.drainingTarget = target
	s.startTime = time.Now()
	s.initialMaintainerCount = maintainerCount
	s.initialDispatcherCount = dispatcherCount

	// Update node liveness to Draining
	s.nodeManager.SetNodeLiveness(target, node.LivenessCaptureDraining)

	log.Info("drain operation started",
		zap.String("target", target.String()),
		zap.Int("maintainerCount", maintainerCount),
		zap.Int("dispatcherCount", dispatcherCount))

	return nil
}

// GetDrainingTarget returns the current draining target.
// Returns empty string if no drain is in progress.
func (s *DrainState) GetDrainingTarget() node.ID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.drainingTarget
}

// IsDraining returns true if a drain operation is in progress.
func (s *DrainState) IsDraining() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.drainingTarget != ""
}

// ClearDrainingTarget clears the draining target and transitions node to Stopping.
// Called when drain operation completes successfully.
func (s *DrainState) ClearDrainingTarget() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.drainingTarget != "" {
		duration := time.Since(s.startTime)
		log.Info("drain operation completed, transitioning to stopping",
			zap.String("target", s.drainingTarget.String()),
			zap.Duration("duration", duration))

		// Transition to Stopping state
		s.nodeManager.SetNodeLiveness(s.drainingTarget, node.LivenessCaptureStopping)
		s.drainingTarget = ""
		s.initialMaintainerCount = 0
		s.initialDispatcherCount = 0
	}
}

// ClearDrainingTargetWithoutTransition clears the draining target without
// transitioning to Stopping state. Used when the draining node is the only
// node left in cluster and needs to reset to Alive.
func (s *DrainState) ClearDrainingTargetWithoutTransition() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.drainingTarget != "" {
		log.Warn("clearing drain state without transition, resetting to alive",
			zap.String("target", s.drainingTarget.String()))

		// Reset to Alive state
		s.nodeManager.SetNodeLiveness(s.drainingTarget, node.LivenessCaptureAlive)
		s.drainingTarget = ""
		s.initialMaintainerCount = 0
		s.initialDispatcherCount = 0
	}
}

// GetDrainProgress returns the drain progress information.
func (s *DrainState) GetDrainProgress() (target node.ID, startTime time.Time, initialMaintainer, initialDispatcher int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.drainingTarget, s.startTime, s.initialMaintainerCount, s.initialDispatcherCount
}

// ClearDrainingTargetOnNodeRemove clears the drain state when the draining node
// is removed from the cluster (e.g., crashed).
func (s *DrainState) ClearDrainingTargetOnNodeRemove(removedNode node.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.drainingTarget == removedNode {
		log.Info("draining node removed from cluster, clearing drain state",
			zap.String("target", s.drainingTarget.String()))
		s.drainingTarget = ""
		s.initialMaintainerCount = 0
		s.initialDispatcherCount = 0
	}
}
