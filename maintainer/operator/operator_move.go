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
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type moveState int

const (
	// moveStateRemoveOrigin removes the dispatcher from the origin node and waits until
	// the origin dispatcher reports a non-working state (typically Stopped), or the origin
	// node is removed.
	moveStateRemoveOrigin moveState = iota
	// moveStateAddDest binds the span to the destination node and creates the dispatcher
	// on that node. The move succeeds once the destination dispatcher reports Working.
	moveStateAddDest
	// moveStateAbortRemoveOrigin indicates the destination node is offline before the move
	// completes. The operator must keep removing the origin dispatcher and only mark the
	// span absent after the origin dispatcher is confirmed stopped (or the origin node is removed),
	// to avoid creating duplicate dispatchers.
	moveStateAbortRemoveOrigin
	// moveStateDoneSuccess indicates the move is finished successfully. PostFinish will mark
	// the span as replicating.
	moveStateDoneSuccess
	// moveStateDoneNoPostFinish indicates the operator is finished and PostFinish must be a no-op,
	// because the span has already been transitioned to another state (for example, absent).
	moveStateDoneNoPostFinish
)

// MoveDispatcherOperator is an operator to move a table span to the destination dispatcher
type MoveDispatcherOperator struct {
	replicaSet     *replica.SpanReplication
	spanController *span.Controller
	origin         node.ID
	dest           node.ID

	// State transitions:
	//   removeOrigin --(origin stopped)-> addDest --(dest working)-> doneSuccess
	//        |                           |
	//        | (dest offline)            | (dest offline)
	//        v                           v
	//   abortRemoveOrigin --(origin stopped/origin offline)-> doneNoPostFinish (span absent)
	//
	// Note: state is protected by lck.
	state moveState

	sendThrottler sendThrottler

	lck sync.Mutex
}

// isFinished reports whether the operator has reached a terminal state.
// Caller must hold m.lck.
func (m *MoveDispatcherOperator) isFinished() bool {
	return m.state == moveStateDoneSuccess || m.state == moveStateDoneNoPostFinish
}

// enterAddDest switches the operator into add-dest phase and updates the span binding.
// Caller must hold m.lck.
func (m *MoveDispatcherOperator) enterAddDest() {
	m.spanController.BindSpanToNode(m.origin, m.dest, m.replicaSet)
	m.state = moveStateAddDest
}

// finishAsAbsent aborts the move and marks the span absent for rescheduling.
// Caller must hold m.lck.
func (m *MoveDispatcherOperator) finishAsAbsent() {
	log.Info("move dispatcher operator aborted, mark span absent for rescheduling",
		zap.Stringer("changefeedID", m.replicaSet.ChangefeedID),
		zap.String("dispatcherID", m.replicaSet.ID.String()),
		zap.String("origin", m.origin.String()),
		zap.String("dest", m.dest.String()))
	m.spanController.MarkSpanAbsent(m.replicaSet)
	m.state = moveStateDoneNoPostFinish
}

func NewMoveDispatcherOperator(spanController *span.Controller, replicaSet *replica.SpanReplication, origin, dest node.ID) *MoveDispatcherOperator {
	return &MoveDispatcherOperator{
		replicaSet:     replicaSet,
		origin:         origin,
		dest:           dest,
		spanController: spanController,
		sendThrottler:  newSendThrottler(),
	}
}

func (m *MoveDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.isFinished() {
		return
	}

	if from == m.origin && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		log.Info("replica set removed from origin node",
			zap.String("replicaSet", m.replicaSet.ID.String()))

		// reset last send message time
		m.sendThrottler.reset()

		switch m.state {
		case moveStateAbortRemoveOrigin:
			// Dest node is offline. Abort the move and let basic scheduler reschedule the span.
			m.finishAsAbsent()
		case moveStateRemoveOrigin:
			m.enterAddDest()
		}
	}
	if m.state == moveStateAddDest && from == m.dest && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		log.Info("replica set added to dest node",
			zap.String("dest", m.dest.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.state = moveStateDoneSuccess
	}
}

func (m *MoveDispatcherOperator) Schedule() *messaging.TargetMessage {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.isFinished() {
		return nil
	}

	if !m.sendThrottler.shouldSend() {
		return nil
	}

	switch m.state {
	case moveStateAddDest:
		return m.replicaSet.NewAddDispatcherMessage(m.dest, heartbeatpb.OperatorType_O_Move)
	case moveStateRemoveOrigin, moveStateAbortRemoveOrigin:
		return m.replicaSet.NewRemoveDispatcherMessage(m.origin, heartbeatpb.OperatorType_O_Move)
	default:
		return nil
	}
}

func (m *MoveDispatcherOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.isFinished() {
		log.Info("move dispatcher operator is finished, no need to handle node remove",
			zap.String("replicaSet", m.replicaSet.ID.String()),
			zap.String("origin", m.origin.String()),
			zap.String("dest", m.dest.String()))
		return
	}

	if n == m.dest {
		// Abort the move when dest node is offline.
		//
		// Note: We must not mark the span absent immediately when dest goes offline in remove-origin
		// state. The origin dispatcher may still be running and basic scheduler could schedule a new
		// dispatcher elsewhere, causing duplicate dispatchers. Instead, we keep removing the origin
		// dispatcher and only mark the span absent after the origin dispatcher is confirmed stopped
		// (or origin node is offline).
		log.Info("dest node is offline, abort move and reschedule span",
			zap.String("dest", m.dest.String()),
			zap.String("origin", m.origin.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))

		switch m.state {
		case moveStateAddDest:
			// Origin dispatcher is already stopped, safe to mark the span absent.
			m.finishAsAbsent()
		case moveStateRemoveOrigin:
			m.state = moveStateAbortRemoveOrigin
		}
		return
	}
	if n == m.origin {
		log.Info("origin node is stopped",
			zap.String("origin", m.origin.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		// Allow sending the next message immediately since move phase changes.
		m.sendThrottler.reset()

		switch m.state {
		case moveStateAbortRemoveOrigin:
			m.finishAsAbsent()
		case moveStateRemoveOrigin:
			m.enterAddDest()
		}
	}
}

// AffectedNodes returns the nodes affected by the operator
func (m *MoveDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.origin, m.dest}
}

func (m *MoveDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *MoveDispatcherOperator) IsFinished() bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.isFinished()
}

func (m *MoveDispatcherOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("replicaset is removed, mark move dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
	m.spanController.MarkSpanReplicating(m.replicaSet)
	m.state = moveStateDoneNoPostFinish
}

func (m *MoveDispatcherOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.dest == m.origin && m.origin != "" {
		log.Info("origin and dest are the same, no need to move",
			zap.String("origin", m.origin.String()),
			zap.String("dest", m.dest.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.state = moveStateDoneSuccess
		return
	}

	m.spanController.MarkSpanScheduling(m.replicaSet)
	m.state = moveStateRemoveOrigin
}

func (m *MoveDispatcherOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	if m.state != moveStateDoneSuccess {
		return
	}

	log.Info("move dispatcher operator finished",
		zap.Stringer("changefeedID", m.replicaSet.ChangefeedID),
		zap.String("dispatcherID", m.replicaSet.ID.String()))
	m.spanController.MarkSpanReplicating(m.replicaSet)
}

func (m *MoveDispatcherOperator) String() string {
	return fmt.Sprintf("move dispatcher operator: %s, origin:%s, dest:%s",
		m.replicaSet.ID, m.origin, m.dest)
}

func (m *MoveDispatcherOperator) Type() string {
	return "move"
}

func (m *MoveDispatcherOperator) BlockTsForward() bool {
	return true
}
