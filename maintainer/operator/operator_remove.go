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
	replicaSet     *replica.SpanReplication
	finished       atomic.Bool
	spanController *span.Controller
}

func newRemoveDispatcherOperator(spanController *span.Controller, replicaSet *replica.SpanReplication) *removeDispatcherOperator {
	return &removeDispatcherOperator{
		replicaSet:     replicaSet,
		spanController: spanController,
	}
}

func (m *removeDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if !m.finished.Load() && from == m.replicaSet.GetNodeID() &&
		status.ComponentStatus != heartbeatpb.ComponentState_Working {
		m.replicaSet.UpdateStatus(status)
		log.Info("dispatcher report non-working status",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
	}
}

func (m *removeDispatcherOperator) Schedule() *messaging.TargetMessage {
	return m.replicaSet.NewRemoveDispatcherMessage(m.replicaSet.GetNodeID())
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *removeDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.replicaSet.GetNodeID() {
		m.finished.Store(true)
	}
}

// AffectedNodes returns the nodes that the operator will affect
func (m *removeDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.replicaSet.GetNodeID()}
}

func (m *removeDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *removeDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *removeDispatcherOperator) OnTaskRemoved() {
	panic("unreachable")
	// m.finished.Store(true)
}

func (m *removeDispatcherOperator) Start() {
	log.Info("start remove dispatcher operator",
		zap.String("replicaSet", m.replicaSet.ID.String()))
}

func (m *removeDispatcherOperator) PostFinish() {
	log.Info("remove dispatcher operator finished",
		zap.String("replicaSet", m.replicaSet.ID.String()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()))
}

func (m *removeDispatcherOperator) String() string {
	return fmt.Sprintf("remove dispatcher operator: %s, dest %s",
		m.replicaSet.ID, m.replicaSet.GetNodeID())
}

func (m *removeDispatcherOperator) Type() string {
	return "remove"
}
