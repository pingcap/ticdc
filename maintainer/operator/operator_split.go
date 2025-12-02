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
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// SplitDispatcherOperator support two kinds of split operator:
// 1. just split the span to some new spans. It does not determine in which node the new span will be stored.
// 2. split the span to some new spans, and still store the span in the origin node.
//
// The first kind of split operator is used when splited span when it exceed the threshold and split table span when the changefeed created.
// The second kind of split operator is used in the split balance scheduler, to split table for more balanced traffic.
type SplitDispatcherOperator struct {
	spanController *span.Controller
	replicaSet     *replica.SpanReplication
	originNode     node.ID

	splitSpans       []*heartbeatpb.TableSpan
	splitSpanInfo    string
	splitTargetNodes []node.ID
	postFinish       func(span *replica.SpanReplication, node node.ID) bool

	checkpointTs uint64
	finished     atomic.Bool
	removed      atomic.Bool

	lck sync.Mutex

	sendThrottler sendThrottler
}

// NewSplitDispatcherOperator creates a new SplitDispatcherOperator
func NewSplitDispatcherOperator(
	spanController *span.Controller,
	replicaSet *replica.SpanReplication,
	splitSpans []*heartbeatpb.TableSpan,
	splitTargetNodes []node.ID,
	postFinish func(span *replica.SpanReplication, node node.ID) bool,
) *SplitDispatcherOperator {
	spansInfo := ""
	for _, span := range splitSpans {
		spansInfo += fmt.Sprintf("[%s,%s]",
			hex.EncodeToString(span.StartKey), hex.EncodeToString(span.EndKey))
	}
	op := &SplitDispatcherOperator{
		replicaSet:       replicaSet,
		originNode:       replicaSet.GetNodeID(),
		splitSpans:       splitSpans,
		checkpointTs:     replicaSet.GetStatus().GetCheckpointTs(),
		spanController:   spanController,
		splitSpanInfo:    spansInfo,
		splitTargetNodes: splitTargetNodes,
		postFinish:       postFinish,
		sendThrottler:    newSendThrottler(),
	}
	return op
}

func (m *SplitDispatcherOperator) Start() {
	m.lck.Lock()
	defer m.lck.Unlock()

	m.spanController.MarkSpanScheduling(m.replicaSet)
}

func (m *SplitDispatcherOperator) OnNodeRemove(n node.ID) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if n == m.originNode {
		log.Info("origin node is removed",
			zap.String("replicaSet", m.replicaSet.ID.String()))
		m.finished.Store(true)
		m.removed.Store(true)
	}
}

// AffectedNodes returns the nodes that the operator will affect
func (m *SplitDispatcherOperator) AffectedNodes() []node.ID {
	return []node.ID{m.originNode}
}

func (m *SplitDispatcherOperator) ID() common.DispatcherID {
	return m.replicaSet.ID
}

func (m *SplitDispatcherOperator) IsFinished() bool {
	return m.finished.Load()
}

func (m *SplitDispatcherOperator) Check(from node.ID, status *heartbeatpb.TableSpanStatus) {
	m.lck.Lock()
	defer m.lck.Unlock()

	if from == m.originNode && status.ComponentStatus != heartbeatpb.ComponentState_Working {
		if status.CheckpointTs > m.checkpointTs {
			m.checkpointTs = status.CheckpointTs
		}
		log.Info("replica set removed from origin node",
			zap.Uint64("checkpointTs", m.checkpointTs),
			zap.String("replicaSet", m.replicaSet.ID.String()))

		m.finished.Store(true)
	}
}

func (m *SplitDispatcherOperator) Schedule() *messaging.TargetMessage {
	if !m.sendThrottler.shouldSend() {
		return nil
	}
	return m.replicaSet.NewRemoveDispatcherMessage(m.originNode)
}

// OnTaskRemoved is called when the task is removed by ddl
func (m *SplitDispatcherOperator) OnTaskRemoved() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("task removed", zap.String("replicaSet", m.replicaSet.ID.String()))
	m.finished.Store(true)
	m.removed.Store(true)
}

func (m *SplitDispatcherOperator) PostFinish() {
	m.lck.Lock()
	defer m.lck.Unlock()

	log.Info("split dispatcher operator begin post finish", zap.String("id", m.replicaSet.ID.String()))

	if m.removed.Load() {
		m.spanController.MarkSpanAbsent(m.replicaSet)
		return
	}

	newReplicaSets := m.spanController.ReplaceReplicaSet([]*replica.SpanReplication{m.replicaSet}, m.splitSpans, m.checkpointTs, m.splitTargetNodes)

	if m.postFinish != nil {
		for idx, span := range newReplicaSets {
			ret := m.postFinish(span, m.splitTargetNodes[idx])
			if !ret {
				log.Error("post finish in split dispatcher operator failed, set the span absent",
					zap.String("id", m.replicaSet.ID.String()),
					zap.String("span", span.ID.String()),
					zap.String("targetNode", m.splitTargetNodes[idx].String()))
				m.spanController.MarkSpanAbsent(span)
			}
		}
	}

	log.Info("split dispatcher operator post finish finished", zap.String("dispatcherID", m.replicaSet.ID.String()))
}

func (m *SplitDispatcherOperator) String() string {
	return fmt.Sprintf("split dispatcher operator: %s, splitSpans:%s",
		m.replicaSet.ID, m.splitSpanInfo)
}

func (m *SplitDispatcherOperator) Type() string {
	return "split"
}

func (m *SplitDispatcherOperator) BlockTsForward() bool {
	return true
}
