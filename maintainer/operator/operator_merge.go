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
	"encoding/hex"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MergeDispatcherOperator is an operator to remove multiple spans belonging to the same table with consecutive ranges in a same node
// and create a new span to the replication db to the same node.
type MergeDispatcherOperator struct {
	spanController *span.Controller
	node           node.ID
	id             common.DispatcherID
	dispatcherIDs  []*heartbeatpb.DispatcherID

	removed  atomic.Bool
	finished atomic.Bool

	toMergedReplicaSets []*replica.SpanReplication
	newReplicaSet       *replica.SpanReplication
	mergedSpanInfo      string
	checkpointTs        uint64

	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]

	sendThrottler sendThrottler
}

func NewMergeDispatcherOperator(
	spanController *span.Controller,
	toMergedReplicaSets []*replica.SpanReplication,
	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus],
) *MergeDispatcherOperator {
	toMergedSpans := make([]*heartbeatpb.TableSpan, 0, len(toMergedReplicaSets))
	for _, replicaSet := range toMergedReplicaSets {
		toMergedSpans = append(toMergedSpans, replicaSet.Span)
	}

	nodeID := toMergedReplicaSets[0].GetNodeID()

	newDispatcherID := common.NewDispatcherID()

	dispatcherIDs := make([]*heartbeatpb.DispatcherID, 0, len(toMergedReplicaSets))
	for _, replicaSet := range toMergedReplicaSets {
		dispatcherIDs = append(dispatcherIDs, replicaSet.ID.ToPB())
	}

	spansInfo := ""
	for _, span := range toMergedSpans {
		spansInfo += fmt.Sprintf("[%s,%s,%d]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey), span.TableID)
	}

	// bind the new replica set to the node.
	mergeTableSpan := &heartbeatpb.TableSpan{
		TableID:  toMergedSpans[0].TableID,
		StartKey: toMergedSpans[0].StartKey,
		EndKey:   toMergedSpans[len(toMergedSpans)-1].EndKey,
	}

	newReplicaSet := replica.NewSpanReplication(
		toMergedReplicaSets[0].ChangefeedID,
		newDispatcherID,
		toMergedReplicaSets[0].GetSchemaID(),
		mergeTableSpan,
		1, // use a fake checkpointTs here.
		toMergedReplicaSets[0].GetMode())

	spanController.AddSchedulingReplicaSet(newReplicaSet, nodeID)

	op := &MergeDispatcherOperator{
		spanController:      spanController,
		node:                nodeID,
		id:                  newDispatcherID,
		dispatcherIDs:       dispatcherIDs,
		toMergedReplicaSets: toMergedReplicaSets,
		checkpointTs:        0,
		mergedSpanInfo:      spansInfo,
		occupyOperators:     occupyOperators,
		newReplicaSet:       newReplicaSet,
		sendThrottler:       newSendThrottler(),
	}
	return op
}

func setOccupyOperatorsFinished(occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	for _, occupyOperator := range occupyOperators {
		occupyOperator.(*OccupyDispatcherOperator).SetFinished()
	}
}

func (m *MergeDispatcherOperator) Start() {
	for _, replicaSet := range m.toMergedReplicaSets {
		m.spanController.MarkSpanScheduling(replicaSet)
	}
}

func (m *MergeDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.node {
		log.Info("origin node is removed",
			zap.Any("toMergedReplicaSets", m.toMergedReplicaSets))

		m.OnTaskRemoved()
	}
}

func (m *MergeDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.node}
}

func (m *MergeDispatcherOperator) ID() common.DispatcherID {
	return m.id
}

func (m *MergeDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *MergeDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.node && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.checkpointTs = status.CheckpointTs
		log.Info("new merged replica set created",
			zap.Uint64("checkpointTs", m.checkpointTs),
			zap.String("dispatcherID", m.id.String()))
		m.finished.Store(true)
	}
}

func (m *MergeDispatcherOperator) Schedule() *messaging.TargetMessage {
	if m.finished.Load() || m.removed.Load() {
		return nil
	}

	if !m.sendThrottler.shouldSend() {
		return nil
	}

	return messaging.NewSingleTargetMessage(m.node,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.MergeDispatcherRequest{
			ChangefeedID:       m.toMergedReplicaSets[0].ChangefeedID.ToPB(),
			DispatcherIDs:      m.dispatcherIDs,
			MergedDispatcherID: m.id.ToPB(),
			Mode:               m.newReplicaSet.GetMode(),
		})
}

// OnTaskRemoved is called when the task is removed by ddl
func (m *MergeDispatcherOperator) OnTaskRemoved() {
	log.Info("task removed", zap.String("replicaSet", m.newReplicaSet.ID.String()))
	m.removed.Store(true)
	m.finished.Store(true)
}

func (m *MergeDispatcherOperator) PostFinish() {
	setOccupyOperatorsFinished(m.occupyOperators)
	if m.removed.Load() {
		// if removed, we set the toMergedReplicaSet to be absent, to ignore the merge operation
		for _, replicaSet := range m.toMergedReplicaSets {
			m.spanController.MarkSpanAbsent(replicaSet)
		}
		m.spanController.RemoveReplicatingSpan(m.newReplicaSet)
		log.Info("merge dispatcher operator finished due to removed", zap.String("id", m.id.String()))
		return
	}

	for _, replicaSet := range m.toMergedReplicaSets {
		m.spanController.RemoveReplicatingSpan(replicaSet)
	}

	m.spanController.MarkSpanReplicating(m.newReplicaSet)
	log.Info("merge dispatcher operator finished", zap.String("id", m.id.String()))
}

func (m *MergeDispatcherOperator) String() string {
	return fmt.Sprintf("merge dispatcher operator new dispatcherID: %s, mergedSpanInfo: %s", m.id, m.mergedSpanInfo)
}

func (m *MergeDispatcherOperator) Type() string {
	return "merge"
}

// dispatcher manager ensure the checkpointTs calculation correctly during the merge operation
func (m *MergeDispatcherOperator) BlockTsForward() bool {
	return false
}
