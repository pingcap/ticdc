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
	db            *replica.ReplicationDB
	node          node.ID
	id            common.DispatcherID
	dispatcherIDs []*heartbeatpb.DispatcherID

	removed  atomic.Bool
	finished atomic.Bool

	toMergedReplicaSets []*replica.SpanReplication
	newReplicaSet       *replica.SpanReplication
	mergedSpanInfo      string
	checkpointTs        uint64

	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]
}

func NewMergeDispatcherOperator(
	db *replica.ReplicationDB,
	toMergedReplicaSets []*replica.SpanReplication,
	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus],
) *MergeDispatcherOperator {
	// Step1: ensure toMergedSpans and affectedReplicaSets belong to the same table with consecutive ranges in a same node
	if len(toMergedReplicaSets) < 2 {
		log.Info("toMergedReplicaSets is less than 2, skip merge",
			zap.Any("toMergedReplicaSets", toMergedReplicaSets))
		setOccupyOperatorsFinished(occupyOperators)
		return nil
	}

	toMergedSpans := make([]*heartbeatpb.TableSpan, 0, len(toMergedReplicaSets))
	for _, replicaSet := range toMergedReplicaSets {
		toMergedSpans = append(toMergedSpans, replicaSet.Span)
	}

	prevTableSpan := toMergedSpans[0]
	nodeID := toMergedReplicaSets[0].GetNodeID()
	for idx := 1; idx < len(toMergedSpans); idx++ {
		currentTableSpan := toMergedSpans[idx]
		if !common.IsTableSpanConsecutive(prevTableSpan, currentTableSpan) {
			log.Info("toMergedSpans is not consecutive, skip merge", zap.String("prevTableSpan", common.FormatTableSpan(prevTableSpan)), zap.String("currentTableSpan", common.FormatTableSpan(currentTableSpan)))
			setOccupyOperatorsFinished(occupyOperators)
			return nil
		}
		prevTableSpan = currentTableSpan
		if toMergedReplicaSets[idx].GetNodeID() != nodeID {
			log.Info("toMergedSpans is not in the same node, skip merge", zap.Any("toMergedReplicaSets", toMergedReplicaSets))
			setOccupyOperatorsFinished(occupyOperators)
			return nil
		}
	}

	// Step2: generate a new dispatcherID as the merged span's dispatcherID
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
		1) // use a fake checkpointTs here.

	db.AddSchedulingReplicaSet(newReplicaSet, nodeID)

	op := &MergeDispatcherOperator{
		db:                  db,
		node:                nodeID,
		id:                  newDispatcherID,
		dispatcherIDs:       dispatcherIDs,
		toMergedReplicaSets: toMergedReplicaSets,
		checkpointTs:        0,
		mergedSpanInfo:      spansInfo,
		occupyOperators:     occupyOperators,
		newReplicaSet:       newReplicaSet,
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
		m.db.MarkSpanScheduling(replicaSet)
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
	return messaging.NewSingleTargetMessage(m.node,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.MergeDispatcherRequest{
			ChangefeedID:       m.toMergedReplicaSets[0].ChangefeedID.ToPB(),
			DispatcherIDs:      m.dispatcherIDs,
			MergedDispatcherID: m.id.ToPB(),
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
			m.db.MarkAbsentWithoutLock(replicaSet)
		}
		m.db.RemoveReplicatingSpan(m.newReplicaSet)
		log.Info("merge dispatcher operator finished due to removed", zap.String("id", m.id.String()))
		return
	}

	for _, replicaSet := range m.toMergedReplicaSets {
		m.db.RemoveReplicatingSpan(replicaSet)
	}

	m.db.MarkReplicatingWithoutLock(m.newReplicaSet)
	log.Info("merge dispatcher operator finished", zap.String("id", m.id.String()))
}

func (m *MergeDispatcherOperator) String() string {
	return fmt.Sprintf("merge dispatcher operator new dispatcherID: %s, mergedSpanInfo: %s", m.id, m.mergedSpanInfo)
}

func (m *MergeDispatcherOperator) Type() string {
	return "merge"
}
