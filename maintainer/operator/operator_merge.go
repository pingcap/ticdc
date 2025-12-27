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
	"strings"

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
//
// State transitions:
//   - NewMergeDispatcherOperator(): create the merged replicaSet in scheduling state on originNode.
//   - Start(): mark all toMergedReplicaSets as scheduling to block other schedulers.
//   - Schedule(): send MergeDispatcherRequest to originNode.
//   - Check(originNode, Working): finish successfully and PostFinish replaces old replicas with the merged replica.
//   - OnNodeRemove(originNode): abort merge, mark old replicas absent, and remove the merged replica.
//   - OnTaskRemoved(): abort merge due to DDL and clean up without clearing node binding of old replicas.
type MergeDispatcherOperator struct {
	spanController *span.Controller
	originNode     node.ID
	id             common.DispatcherID
	dispatcherIDs  []*heartbeatpb.DispatcherID

	// aborted indicates the merge should not be applied successfully. It can be set by OnNodeRemove
	// or OnTaskRemoved. When aborted is true, PostFinish follows the abort path.
	aborted      atomic.Bool
	abortedByDDL atomic.Bool
	finished     atomic.Bool

	toMergedReplicaSets []*replica.SpanReplication
	newReplicaSet       *replica.SpanReplication
	mergedSpanInfo      string
	checkpointTs        uint64

	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]

	sendThrottler sendThrottler
}

func buildMergedSpanInfo(toMergedSpans []*heartbeatpb.TableSpan) string {
	var spansInfo strings.Builder
	for _, span := range toMergedSpans {
		spansInfo.WriteString(fmt.Sprintf("[%s,%s,%d]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey), span.TableID))
	}
	return spansInfo.String()
}

func buildDispatcherIDs(toMergedReplicaSets []*replica.SpanReplication) []*heartbeatpb.DispatcherID {
	dispatcherIDs := make([]*heartbeatpb.DispatcherID, 0, len(toMergedReplicaSets))
	for _, replicaSet := range toMergedReplicaSets {
		dispatcherIDs = append(dispatcherIDs, replicaSet.ID.ToPB())
	}
	return dispatcherIDs
}

func minCheckpointTs(toMergedReplicaSets []*replica.SpanReplication) uint64 {
	// Initialize the merged replica with a safe checkpointTs. Using an artificially small
	// value can regress the changefeed checkpoint because scheduling/absent replicas are
	// included in the global checkpoint calculation.
	checkpointTs := toMergedReplicaSets[0].GetStatus().GetCheckpointTs()
	for idx := 1; idx < len(toMergedReplicaSets); idx++ {
		ts := toMergedReplicaSets[idx].GetStatus().GetCheckpointTs()
		if ts < checkpointTs {
			checkpointTs = ts
		}
	}
	// Ensure checkpointTs is at least 1.
	if checkpointTs == 0 {
		checkpointTs = 1
	}
	return checkpointTs
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

	dispatcherIDs := buildDispatcherIDs(toMergedReplicaSets)
	spansInfo := buildMergedSpanInfo(toMergedSpans)

	// bind the new replica set to the node.
	mergeTableSpan := &heartbeatpb.TableSpan{
		TableID:    toMergedSpans[0].TableID,
		StartKey:   toMergedSpans[0].StartKey,
		EndKey:     toMergedSpans[len(toMergedSpans)-1].EndKey,
		KeyspaceID: spanController.GetkeyspaceID(),
	}

	checkpointTs := minCheckpointTs(toMergedReplicaSets)

	newReplicaSet := replica.NewSpanReplication(
		toMergedReplicaSets[0].ChangefeedID,
		newDispatcherID,
		toMergedReplicaSets[0].GetSchemaID(),
		mergeTableSpan,
		checkpointTs,
		toMergedReplicaSets[0].GetMode(),
		toMergedReplicaSets[0].IsSplitEnabled())

	spanController.AddSchedulingReplicaSet(newReplicaSet, nodeID)

	op := &MergeDispatcherOperator{
		spanController:      spanController,
		originNode:          nodeID,
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

func NewRestoredMergeDispatcherOperator(
	spanController *span.Controller,
	toMergedReplicaSets []*replica.SpanReplication,
	mergedReplicaSet *replica.SpanReplication,
	occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus],
) *MergeDispatcherOperator {
	toMergedSpans := make([]*heartbeatpb.TableSpan, 0, len(toMergedReplicaSets))
	for _, replicaSet := range toMergedReplicaSets {
		toMergedSpans = append(toMergedSpans, replicaSet.Span)
	}

	dispatcherIDs := buildDispatcherIDs(toMergedReplicaSets)
	spansInfo := buildMergedSpanInfo(toMergedSpans)

	return &MergeDispatcherOperator{
		spanController:      spanController,
		originNode:          toMergedReplicaSets[0].GetNodeID(),
		id:                  mergedReplicaSet.ID,
		dispatcherIDs:       dispatcherIDs,
		toMergedReplicaSets: toMergedReplicaSets,
		checkpointTs:        0,
		mergedSpanInfo:      spansInfo,
		occupyOperators:     occupyOperators,
		newReplicaSet:       mergedReplicaSet,
		sendThrottler:       newSendThrottler(),
	}
}

func setOccupyOperatorsFinished(occupyOperators []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	for _, occupyOperator := range occupyOperators {
		// occupyOperators are created by AddMergeOperator and are guaranteed to be OccupyDispatcherOperator.
		occupyOperator.(*OccupyDispatcherOperator).SetFinished()
	}
}

func (m *MergeDispatcherOperator) Start() {
	for _, replicaSet := range m.toMergedReplicaSets {
		m.spanController.MarkSpanScheduling(replicaSet)
	}
}

func (m *MergeDispatcherOperator) OnNodeRemove(n node.ID) {
	if n == m.originNode {
		log.Info("origin node is removed",
			zap.Any("toMergedReplicaSets", m.toMergedReplicaSets))

		m.aborted.Store(true)
		m.finished.Store(true)
	}
}

func (m *MergeDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.originNode}
}

func (m *MergeDispatcherOperator) ID() common.DispatcherID {
	return m.id
}

func (m *MergeDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *MergeDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	if from == m.originNode && status.ComponentStatus == heartbeatpb.ComponentState_Working {
		m.checkpointTs = status.CheckpointTs
		log.Info("new merged replica set created",
			zap.Uint64("checkpointTs", m.checkpointTs),
			zap.String("dispatcherID", m.id.String()))
		m.finished.Store(true)
	}
}

func (m *MergeDispatcherOperator) Schedule() *messaging.TargetMessage {
	if m.finished.Load() || m.aborted.Load() {
		return nil
	}

	if !m.sendThrottler.shouldSend() {
		return nil
	}

	return messaging.NewSingleTargetMessage(m.originNode,
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
	m.abortedByDDL.Store(true)
	m.aborted.Store(true)
	m.finished.Store(true)
}

func (m *MergeDispatcherOperator) PostFinish() {
	setOccupyOperatorsFinished(m.occupyOperators)
	if m.aborted.Load() {
		if !m.abortedByDDL.Load() {
			// if removed by node offline, we set the toMergedReplicaSet to be absent, to ignore the merge operation
			for _, replicaSet := range m.toMergedReplicaSets {
				m.spanController.MarkSpanAbsent(replicaSet)
			}
		} else {
			// If removed by ddl, avoid marking these replicas absent:
			// MarkSpanAbsent clears replica's nodeID, and RemoveTasksByTableIDs iterates a map where
			// the operator removal order is not deterministic. If the merge operator finishes first,
			// subsequent remove operators might snapshot an empty nodeID and leak dispatchers.
			for _, replicaSet := range m.toMergedReplicaSets {
				m.spanController.MarkSpanReplicating(replicaSet)
			}
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
