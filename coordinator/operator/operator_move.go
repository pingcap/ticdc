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

package operator

import (
	"fmt"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// moveMaintainerState describes the origin-first handoff flow for a maintainer move.
type moveMaintainerState int

const (
	moveMaintainerStateRemoveOrigin moveMaintainerState = iota
	moveMaintainerStateOriginStopped
	moveMaintainerStateAddTarget
	moveMaintainerStateDoneSuccess
	moveMaintainerStateDoneNoPostFinish
)

// MoveMaintainerOperator is an operator to move a maintainer to the destination node
type MoveMaintainerOperator struct {
	changefeed *changefeed.Changefeed
	db         *changefeed.ChangefeedDB
	origin     node.ID
	target     node.ID

	originMaintainerEpoch uint64
	state                 moveMaintainerState
	// originRemoved records node-liveness removal for the origin node. The move
	// may still be waiting on target removal when this flag is set.
	originRemoved bool

	lck sync.Mutex
}

// NewMoveMaintainerOperator creates an operator that moves a maintainer from origin to dest.
func NewMoveMaintainerOperator(db *changefeed.ChangefeedDB, changefeed *changefeed.Changefeed,
	origin, dest node.ID,
) *MoveMaintainerOperator {
	return &MoveMaintainerOperator{
		changefeed: changefeed,
		origin:     origin,
		target:     dest,
		db:         db,
		// The move first removes the origin maintainer and then adds the
		// destination. The remove must use the epoch the origin already owns.
		originMaintainerEpoch: changefeed.GetInfo().Epoch,
	}
}

// Check observes maintainer status and advances the move handoff state.
func (m *MoveMaintainerOperator) Check(from node.ID, status *heartbeatpb.MaintainerStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if status == nil {
		return
	}
	if m.isFinishedLocked() {
		return
	}

	if from == m.origin &&
		m.state == moveMaintainerStateRemoveOrigin &&
		common.MaintainerEpochMatches(status.MaintainerEpoch, m.originMaintainerEpoch) &&
		status.State != heartbeatpb.ComponentState_Working {
		log.Info("changefeed removed from origin node",
			zap.String("changefeed", m.changefeed.ID.String()))
		m.state = moveMaintainerStateOriginStopped
	}
	if m.state == moveMaintainerStateAddTarget &&
		from == m.target &&
		common.MaintainerEpochMatches(status.MaintainerEpoch, m.changefeed.GetInfo().Epoch) &&
		status.State == heartbeatpb.ComponentState_Working &&
		status.BootstrapDone {
		log.Info("changefeed added to dest node",
			zap.String("dest", m.target.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		m.state = moveMaintainerStateDoneSuccess
	}
}

// Schedule returns the next remove or add command needed by the current move state.
func (m *MoveMaintainerOperator) Schedule() *messaging.TargetMessage {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.isFinishedLocked() {
		return nil
	}

	switch m.state {
	case moveMaintainerStateOriginStopped:
		m.enterAddTargetLocked()
		return m.changefeed.NewAddMaintainerMessage(m.target)
	case moveMaintainerStateAddTarget:
		return m.changefeed.NewAddMaintainerMessage(m.target)
	case moveMaintainerStateRemoveOrigin:
		return changefeed.RemoveMaintainerMessage(
			m.changefeed.GetKeyspaceID(),
			m.changefeed.ID,
			m.origin,
			false,
			false,
			m.originMaintainerEpoch,
		)
	default:
		return nil
	}
}

// OnNodeRemove updates the move when either the origin or target node goes offline.
func (m *MoveMaintainerOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.state == moveMaintainerStateDoneNoPostFinish {
		return
	}

	if n == m.origin {
		m.originRemoved = true
	}
	if n == m.target {
		m.onTargetNodeRemovedLocked()
		return
	}
	if n == m.origin {
		log.Info("origin node is stopped",
			zap.String("origin", m.origin.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		switch m.state {
		case moveMaintainerStateRemoveOrigin:
			m.state = moveMaintainerStateOriginStopped
		case moveMaintainerStateOriginStopped:
		}
	}
}

// onTargetNodeRemovedLocked handles target loss without creating two live owners.
func (m *MoveMaintainerOperator) onTargetNodeRemovedLocked() {
	switch m.state {
	case moveMaintainerStateRemoveOrigin:
		if m.target == m.origin {
			m.finishAsAbsentLocked()
			return
		}
		log.Info("destination node removed before origin maintainer stopped",
			zap.String("dest", m.target.String()),
			zap.String("origin", m.origin.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		// Keep removing the old origin maintainer first. The new owner can only
		// be added back to origin after the old epoch reports stopped.
		m.target = m.origin
	case moveMaintainerStateOriginStopped:
		if m.target == m.origin {
			m.finishAsAbsentLocked()
			return
		}
		log.Info("destination node removed after origin maintainer stopped",
			zap.String("dest", m.target.String()),
			zap.String("origin", m.origin.String()),
			zap.String("changefeed", m.changefeed.ID.String()))
		if m.originRemoved {
			m.finishAsAbsentLocked()
			return
		}
		m.target = m.origin
	case moveMaintainerStateAddTarget, moveMaintainerStateDoneSuccess:
		// Once the add request may have reached target, rebinding to origin can
		// create two new-epoch maintainers. Mark absent and let scheduler retry
		// with a fresh ownership epoch.
		m.finishAsAbsentLocked()
	}
}

// enterAddTargetLocked binds the changefeed to the target before sending add requests.
func (m *MoveMaintainerOperator) enterAddTargetLocked() {
	m.db.BindChangefeedToNode(m.origin, m.target, m.changefeed)
	m.state = moveMaintainerStateAddTarget
}

// finishAsAbsentLocked finishes the move without PostFinish side effects.
func (m *MoveMaintainerOperator) finishAsAbsentLocked() {
	log.Info("move maintainer operator aborted, mark changefeed absent",
		zap.String("changefeed", m.changefeed.ID.String()),
		zap.String("origin", m.origin.String()),
		zap.String("target", m.target.String()))
	m.db.MarkMaintainerAbsent(m.changefeed)
	m.state = moveMaintainerStateDoneNoPostFinish
}

func (m *MoveMaintainerOperator) isFinishedLocked() bool {
	return m.state == moveMaintainerStateDoneSuccess || m.state == moveMaintainerStateDoneNoPostFinish
}

func (m *MoveMaintainerOperator) isOriginStopTargetLocked() bool {
	return m.state == moveMaintainerStateRemoveOrigin ||
		m.state == moveMaintainerStateOriginStopped
}

// AffectedNodes returns the origin and current target nodes touched by this move.
func (m *MoveMaintainerOperator) AffectedNodes() []node.ID {
	m.lck.Lock()
	defer m.lck.Unlock()

	return []node.ID{m.origin, m.target}
}

// OriginNode returns the source node of the move.
func (m *MoveMaintainerOperator) OriginNode() node.ID {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.origin
}

// originStopTarget returns the origin maintainer until the destination owner
// has been bound. The origin keeps fencing close requests by the epoch it
// already owns, even after it has reported stopped, because no new owner exists
// before the move sends the destination add request.
func (m *MoveMaintainerOperator) originStopTarget() (node.ID, uint64, bool) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if !m.isOriginStopTargetLocked() {
		return "", 0, false
	}
	return m.origin, m.originMaintainerEpoch, true
}

// ID returns the changefeed ID this operator works on.
func (m *MoveMaintainerOperator) ID() common.ChangeFeedID {
	return m.changefeed.ID
}

// IsFinished reports whether the move has reached a terminal state.
func (m *MoveMaintainerOperator) IsFinished() bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.isFinishedLocked()
}

// OnTaskRemoved stops the move when the changefeed task is removed.
func (m *MoveMaintainerOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("changefeed removed, mark move changefeed operator finished",
		zap.String("changefeed", m.changefeed.ID.String()))
	m.state = moveMaintainerStateDoneNoPostFinish
}

// Start marks the changefeed as scheduling before the first move command is sent.
func (m *MoveMaintainerOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.isFinishedLocked() {
		return
	}
	m.db.MarkMaintainerScheduling(m.changefeed)
}

// PostFinish marks a successfully moved maintainer as replicating.
func (m *MoveMaintainerOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.state != moveMaintainerStateDoneSuccess {
		return
	}

	log.Info("move changefeed operator finished",
		zap.String("changefeed", m.changefeed.ID.String()))
	m.db.MarkMaintainerReplicating(m.changefeed)
}

// String returns a human-readable description of the operator.
func (m *MoveMaintainerOperator) String() string {
	m.lck.Lock()
	defer m.lck.Unlock()

	return fmt.Sprintf("move maintainer operator: %s, origin:%s, dest:%s",
		m.changefeed.ID, m.origin, m.target)
}

// Type returns the operator type used by metrics and logs.
func (m *MoveMaintainerOperator) Type() string {
	return "move"
}

// BlockTsForward indicates whether this operator blocks changefeed checkpoint forwarding.
func (m *MoveMaintainerOperator) BlockTsForward() bool {
	log.Panic("unreachable code")
	return false
}
