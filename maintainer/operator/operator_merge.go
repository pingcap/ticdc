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
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MergeDispatcherOperator is an operator to remove multiple spans belonging to the same table with adjacent ranges in a same node
// and create a new span to the replication db to the same node.
type MergeDispatcherOperator struct {
	db               *replica.ReplicationDB
	node             node.ID
	id               common.DispatcherID
	dispatcherIDs    []*heartbeatpb.DispatcherID
	originReplicaSet *replica.SpanReplication

	removed  atomic.Bool
	finished atomic.Bool

	toMergedReplicaSets []*replica.SpanReplication
	toMergedSpans       []*heartbeatpb.TableSpan
	mergedSpanInfo      string
	checkpointTs        uint64
}

func NewMergeDispatcherOperator(
	db *replica.ReplicationDB,
	toMergedReplicaSets []*replica.SpanReplication,
	toMergedSpans []*heartbeatpb.TableSpan,
) *MergeDispatcherOperator {
	// Step1: ensure toMergedSpans and affectedReplicaSets belong to the same table with adjacent ranges in a same node
	if len(toMergedSpans) < 2 || len(toMergedReplicaSets) != len(toMergedSpans) {
		log.Info("toMergedSpans is less than 2 or toMergedReplicaSets is not equal to toMergedSpans, skip merge",
			zap.Any("toMergedSpans", toMergedSpans),
			zap.Any("toMergedReplicaSets", toMergedReplicaSets))
		return nil
	}
	prevTableSpan := toMergedSpans[0]
	nodeID := toMergedReplicaSets[0].GetNodeID()
	for idx := 1; idx < len(toMergedSpans); idx++ {
		currentTableSpan := toMergedSpans[idx]
		if !common.IsTableSpanAdjacent(prevTableSpan, currentTableSpan) {
			log.Info("toMergedSpans is not adjacent, skip merge", zap.Any("toMergedSpans", toMergedSpans))
			return nil
		}
		prevTableSpan = currentTableSpan
		if toMergedReplicaSets[idx].GetNodeID() != nodeID {
			log.Info("toMergedSpans is not in the same node, skip merge", zap.Any("toMergedReplicaSets", toMergedReplicaSets))
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
		spansInfo += fmt.Sprintf("[%s,%s]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey))
	}

	op := &MergeDispatcherOperator{
		db:                  db,
		node:                nodeID,
		id:                  newDispatcherID,
		dispatcherIDs:       dispatcherIDs,
		toMergedReplicaSets: toMergedReplicaSets,
		toMergedSpans:       toMergedSpans,
		checkpointTs:        0,
		mergedSpanInfo:      spansInfo,
	}
	return op
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

		m.finished.Store(true)
		m.removed.Store(true)
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
	log.Info("task removed", zap.String("replicaSet", m.originReplicaSet.ID.String()))
	m.removed.Store(true)
	m.finished.Store(true)
}

func (m *MergeDispatcherOperator) PostFinish() {
	if m.removed.Load() {
		// if removed, we set the toMergedReplicaSet to be absent, to ignore the merge operation
		for _, replicaSet := range m.toMergedReplicaSets {
			m.db.MarkAbsentWithoutLock(replicaSet)
		}
		log.Info("merge dispatcher operator finished due to removed", zap.String("id", m.id.String()))
		return
	}

	for _, replicaSet := range m.toMergedReplicaSets {
		m.db.MarkAbsentWithoutLock(replicaSet)
	}
	mergeTableSpan := &heartbeatpb.TableSpan{
		TableID:  m.toMergedSpans[0].TableID,
		StartKey: m.toMergedSpans[0].StartKey,
		EndKey:   m.toMergedSpans[len(m.toMergedSpans)-1].EndKey,
	}

	newReplicaSet := replica.NewSpanReplication(
		m.toMergedReplicaSets[0].ChangefeedID,
		m.id, m.toMergedReplicaSets[0].GetPDClock(),
		m.toMergedReplicaSets[0].GetSchemaID(),
		mergeTableSpan,
		m.checkpointTs)

	m.db.MarkReplicatingWithoutLock(newReplicaSet)

	log.Info("merge dispatcher operator finished", zap.String("id", m.id.String()))
}

func (m *MergeDispatcherOperator) String() string {
	return fmt.Sprintf("merge dispatcher operator new dispatcherID: %s, mergedSpanInfo: %s", m.id, m.mergedSpanInfo)
}

func (m *MergeDispatcherOperator) Type() string {
	return "merge"
}
