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

package operator

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// OccupyDispatcherOperator is an operator to occupy a replica set not evolving.
type OccupyDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	finished   atomic.Bool
	removed    atomic.Bool
	db         *replica.ReplicationDB
}

func NewOccupyDispatcherOperator(
	db *replica.ReplicationDB,
	replicaSet *replica.SpanReplication,
) *OccupyDispatcherOperator {
	return &OccupyDispatcherOperator{
		replicaSet: replicaSet,
		db:         db,
	}
}

func (m *OccupyDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
}

func (m *OccupyDispatcherOperator) Schedule() *messaging.TargetMessage {
	return nil
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *OccupyDispatcherOperator) OnNodeRemove(n node.ID) {
}

// AffectedNodes returns the nodes affected by the operator
func (m *OccupyDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{}
}

func (m *OccupyDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *OccupyDispatcherOperator) SetFinished() {
	m.finished.Store(true)
}

func (m *OccupyDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *OccupyDispatcherOperator) OnTaskRemoved() {
	m.finished.Store(true)
	m.removed.Store(true)
}

func (m *OccupyDispatcherOperator) Start() {
}

func (m *OccupyDispatcherOperator) PostFinish() {
	log.Info("occupy dispatcher operator finished",
		zap.Any("removed", m.removed.Load()),
		zap.Any("finished", m.finished.Load()),
		zap.String("changefeed", m.replicaSet.ChangefeedID.String()),
		zap.String("replicaSet", m.replicaSet.ID.String()))
}

func (m *OccupyDispatcherOperator) String() string {
	return fmt.Sprintf("occupy dispatcher operator: %s", m.replicaSet.ID)
}

func (m *OccupyDispatcherOperator) Type() string {
	return "occupy"
}
