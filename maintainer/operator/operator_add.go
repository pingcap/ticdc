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

// AddDispatcherOperator is an operator to schedule a table span to a dispatcher
//
// State transitions:
//   - Start(): bind the span to dest and move it to scheduling.
//   - Check(dest, Working): finish successfully and PostFinish marks the span replicating.
//   - Check(dest, Removed) / OnNodeRemove(dest) / OnTaskRemoved(): finish as removed and PostFinish
//     marks the span absent (if it still exists in spanController) for rescheduling.
type AddDispatcherOperator struct {
	replicaSet *replica.SpanReplication
	dest       node.ID
	finished   atomic.Bool
	// removed means the add operation should not be completed successfully. It can be set when:
	//   - The dest dispatcher reports Removed.
	//   - The dest node is removed.
	//   - The task is removed (for example, due to DDL).
	removed        atomic.Bool
	spanController *span.Controller
	// This add operator may be a part of move/split operator
	operatorType heartbeatpb.OperatorType

	sendThrottler sendThrottler
}

func NewAddDispatcherOperator(
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	dest node.ID,
	operatorType heartbeatpb.OperatorType,
) *AddDispatcherOperator {
	return &AddDispatcherOperator{
		replicaSet:     replicaSet,
		dest:           dest,
		spanController: spanController,
		operatorType:   operatorType,
		sendThrottler:  newSendThrottler(),
	}
}

func (m *AddDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if m.finished.Load() || from != m.dest {
		return
	}

	switch status.ComponentStatus {
	case heartbeatpb.ComponentState_Working:
		log.Info("dispatcher report working status",
			zap.String("changefeed", m.replicaSet.ChangefeedID.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
	case heartbeatpb.ComponentState_Removed:
		log.Info("dispatcher report removed status",
			zap.String("changefeed", m.replicaSet.ChangefeedID.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
		m.removed.Store(true)
	case heartbeatpb.ComponentState_Stopped:
		log.Warn("dispatcher report unexpected stopped status, ignore",
			zap.String("changefeed", m.replicaSet.ChangefeedID.String()),
			zap.String("replicaSet", m.replicaSet.ID.String()))
	}
}

func (m *AddDispatcherOperator) Schedule() *messaging.TargetMessage {
	if m.finished.Load() || m.removed.Load() {
		return nil
	}

	if !m.sendThrottler.shouldSend() {
		return nil
	}
	return m.replicaSet.NewAddDispatcherMessage(m.dest, m.operatorType)
}

// OnNodeRemove is called when node offline, and the replicaset must already move to absent status and will be scheduled again
func (m *AddDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.dest {
		m.OnTaskRemoved()
	}
}

// AffectedNodes returns the nodes affected by the operator
func (m *AddDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.dest}
}

func (m *AddDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *AddDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *AddDispatcherOperator) OnTaskRemoved() {
	m.finished.Store(true)
	m.removed.Store(true)
}

func (m *AddDispatcherOperator) Start() {
	if m.removed.Load() || m.finished.Load() {
		return
	}
	m.spanController.BindSpanToNode("", m.dest, m.replicaSet)
}

func (m *AddDispatcherOperator) PostFinish() {
	if !m.removed.Load() {
		m.spanController.MarkSpanReplicating(m.replicaSet)
	} else {
		// Only mark span absent if it still exists in spanController. When a DDL removes the task,
		// spanController may have already deleted it. Marking an already removed span absent would
		// reintroduce a ghost entry into the scheduling state.
		if m.spanController.GetTaskByID(m.replicaSet.ID) != nil {
			m.spanController.MarkSpanAbsent(m.replicaSet)
		}
	}
}

func (m *AddDispatcherOperator) String() string {
	return fmt.Sprintf("add dispatcher operator: %s, dest:%s",
		m.replicaSet.ID, m.dest)
}

func (m *AddDispatcherOperator) Type() string {
	return "add"
}

func (m *AddDispatcherOperator) BlockTsForward() bool {
	return true
}
