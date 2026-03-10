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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// removeDispatcherOperator is an operator to remove a table span from a dispatcher
// and remove it from the replication db
type removeDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	nodeID     node.ID
	finished   atomic.Bool
	// removed is set when the underlying span replication task is deleted (for example by a DDL).
	//
	// Why we need it:
	// A task removal can race with failover/bootstrap and cause the barrier to enqueue the same remove
	// operator multiple times. The operator controller replaces the old operator and calls OnTaskRemoved
	// before PostFinish. For remove operators that carry a postFinish hook (e.g. the remove phase of
	// move/split which would MarkSpanAbsent), running that hook during task deletion can reintroduce a
	// ghost span or wipe the nodeID needed by other removals.
	//
	// When removed is true we treat the operator as canceled and skip postFinish.
	removed        atomic.Bool
	postFinish     func()
	spanController *span.Controller
	// operatorType is copied into ScheduleDispatcherRequest.OperatorType when we send remove requests to
	// dispatcher managers.
	//
	// Why we need it:
	// Dispatcher managers include unfinished scheduling requests in their bootstrap responses so a new maintainer
	// can restore in-flight operators after failover. operatorType tells the maintainer which high-level operator
	// the removal belongs to (standalone Remove vs being part of Move/Split, etc.), while the dispatcher manager
	// itself only performs Create/Remove actions.
	//
	// Note: Some operators do not carry operatorType because they use dedicated request types instead of
	// ScheduleDispatcherRequest (for example merge-related messages).
	operatorType heartbeatpb.OperatorType

	sendThrottler sendThrottler
}

func NewRemoveDispatcherOperator(
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	operatorType heartbeatpb.OperatorType,
	postFinish func(),
) *removeDispatcherOperator {
	return &removeDispatcherOperator{
		replicaSet:     replicaSet,
		nodeID:         replicaSet.GetNodeID(),
		spanController: spanController,
		postFinish:     postFinish,
		operatorType:   operatorType,
		sendThrottler:  newSendThrottler(),
	}
}

func newRemoveDispatcherOperator(spanController *span.Controller, replicaSet *replica.SpanReplication, operatorType heartbeatpb.OperatorType) *removeDispatcherOperator {
	return &removeDispatcherOperator{
		replicaSet:     replicaSet,
		nodeID:         replicaSet.GetNodeID(),
		spanController: spanController,
		operatorType:   operatorType,
		sendThrottler:  newSendThrottler(),
	}
}

func (m *removeDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	// Only treat terminal states as removal completed.
	// During merge, a dispatcher can temporarily be in non-working states (e.g. WaitingMerge),
	// which should not complete the remove operator, otherwise the dispatcher can be leaked.
	if !m.finished.Load() &&
		from == m.nodeID &&
		(status.ComponentStatus == heartbeatpb.ComponentState_Stopped ||
			status.ComponentStatus == heartbeatpb.ComponentState_Removed) {
		m.replicaSet.UpdateStatus(status)
		log.Info("dispatcher report non-working status",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
	}
}

func (m *removeDispatcherOperator) Schedule() *messaging.TargetMessage {
	// Once finished, stop emitting remove requests; otherwise we may reintroduce a stale in-flight
	// operator into dispatcher manager bootstrap responses after the dispatcher is already cleaned up.
	if !m.sendThrottler.shouldSend() || m.finished.Load() {
		return nil
	}

	return m.replicaSet.NewRemoveDispatcherMessage(m.nodeID, m.operatorType)
}

// OnNodeRemove is called when node offline, and the replicaset has been removed from spanController, so it's ok.
func (m *removeDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.nodeID {
		m.finished.Store(true)
	}
}

// AffectedNodes returns the nodes that the operator will affect
func (m *removeDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.nodeID}
}

func (m *removeDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *removeDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *removeDispatcherOperator) OnTaskRemoved() {
	// Task removal means the replica set is no longer managed by the scheduler (e.g. DROP TABLE).
	// Mark the operator finished to stop scheduling, and skip postFinish hooks that could mutate
	// spanController state for a task that is being deleted.
	m.removed.Store(true)
	m.finished.Store(true)
}

func (m *removeDispatcherOperator) Start() {
	log.Info("start remove dispatcher operator",
		zap.String("replicaSet", m.replicaSet.ID.String()))
}

func (m *removeDispatcherOperator) PostFinish() {
	log.Info("remove dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()),
		zap.Bool("taskRemoved", m.removed.Load()))

	// If the task is removed, the span will be cleaned up by spanController, and we must not run
	// post-finish hooks that may mark it absent and reintroduce a ghost entry.
	if !m.removed.Load() && m.postFinish != nil {
		m.postFinish()
	}
}

func (m *removeDispatcherOperator) String() string {
	return fmt.Sprintf("remove dispatcher operator: %s, dest %s",
		m.replicaSet.ID, m.nodeID)
}

func (m *removeDispatcherOperator) Type() string {
	return "remove"
}

func (m *removeDispatcherOperator) BlockTsForward() bool {
	return false
}
