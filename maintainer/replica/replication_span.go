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

package replica

import (
	"bytes"
	"encoding/hex"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// SpanReplication is responsible for a table span replication status
// It is used to manage the replication status of a table span,
// the status is updated by the heartbeat collector
// It implements the replica.Replication interface
type SpanReplication struct {
	ID           common.DispatcherID
	Span         *heartbeatpb.TableSpan
	ChangefeedID common.ChangeFeedID
	// whether the span is enabled to split.
	// if the sink is mysql-sink and the table have only one primary key and no uk, the span is enabled to split.
	// if the sink is other sink, the span is enabled to split.
	enabledSplit bool

	schemaID    int64
	nodeIDMutex sync.Mutex // mutex for nodeID
	nodeID      node.ID
	groupID     replica.GroupID
	status      *atomic.Pointer[heartbeatpb.TableSpanStatus]
	blockState  *atomic.Pointer[heartbeatpb.State]
}

func NewSpanReplication(cfID common.ChangeFeedID,
	id common.DispatcherID,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	checkpointTs uint64,
	mode int64,
) *SpanReplication {
	r := newSpanReplication(cfID, id, SchemaID, span)
	r.initStatus(&heartbeatpb.TableSpanStatus{
		ID:           id.ToPB(),
		CheckpointTs: checkpointTs,
		Mode:         mode,
	})
	log.Info("new span replication created",
		zap.Stringer("changefeedID", cfID),
		zap.String("id", id.String()),
		zap.Int64("schemaID", SchemaID),
		zap.Int64("tableID", span.TableID),
		zap.String("groupID", replica.GetGroupName(r.groupID)),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func NewWorkingSpanReplication(
	cfID common.ChangeFeedID,
	id common.DispatcherID,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	status *heartbeatpb.TableSpanStatus,
	nodeID node.ID,
) *SpanReplication {
	r := newSpanReplication(cfID, id, SchemaID, span)
	// Must set Node ID when creating a working span replication
	r.SetNodeID(nodeID)
	r.initStatus(status)
	log.Info("new working span replication created",
		zap.Stringer("changefeedID", cfID),
		zap.String("dispatcherID", id.String()),
		zap.String("nodeID", nodeID.String()),
		zap.Uint64("checkpointTs", status.CheckpointTs),
		zap.String("componentStatus", status.ComponentStatus.String()),
		zap.Int64("schemaID", SchemaID),
		zap.Int64("tableID", span.TableID),
		zap.Int64("groupID", r.groupID),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func newSpanReplication(cfID common.ChangeFeedID, id common.DispatcherID, SchemaID int64, span *heartbeatpb.TableSpan) *SpanReplication {
	r := &SpanReplication{
		ID:           id,
		schemaID:     SchemaID,
		Span:         span,
		ChangefeedID: cfID,
		status:       atomic.NewPointer[heartbeatpb.TableSpanStatus](nil),
		blockState:   atomic.NewPointer[heartbeatpb.State](nil),
	}
	r.initGroupID()
	return r
}

func (r *SpanReplication) initStatus(status *heartbeatpb.TableSpanStatus) {
	if status == nil || status.CheckpointTs == 0 {
		log.Panic("add replica with invalid checkpoint ts",
			zap.Stringer("changefeedID", r.ChangefeedID),
			zap.String("dispatcherID", r.ID.String()),
			zap.Uint64("checkpointTs", status.CheckpointTs),
		)
	}
	r.status.Store(status)
}

func (r *SpanReplication) initGroupID() {
	r.groupID = replica.DefaultGroupID
	span := heartbeatpb.TableSpan{
		TableID:    r.Span.TableID,
		StartKey:   r.Span.StartKey,
		EndKey:     r.Span.EndKey,
		KeyspaceID: r.Span.KeyspaceID,
	}
	// check if the table is split
	totalSpan := common.TableIDToComparableSpan(span.KeyspaceID, span.TableID)
	if !common.IsSubSpan(span, totalSpan) {
		log.Warn("invalid span range",
			zap.Stringer("changefeedID", r.ChangefeedID),
			zap.String("dispatcherID", r.ID.String()), zap.Int64("tableID", span.TableID),
			zap.String("totalSpan", totalSpan.String()),
			zap.String("start", hex.EncodeToString(span.StartKey)),
			zap.String("end", hex.EncodeToString(span.EndKey)))
	}
	if !bytes.Equal(span.StartKey, totalSpan.StartKey) || !bytes.Equal(span.EndKey, totalSpan.EndKey) {
		r.groupID = replica.GenGroupID(replica.GroupTable, span.TableID)
	}
	log.Info("init groupID",
		zap.Stringer("changefeedID", r.ChangefeedID), zap.Any("groupID", r.groupID),
		zap.Any("span", common.FormatTableSpan(&span)), zap.Any("totalSpan", common.FormatTableSpan(&totalSpan)))
}

func (r *SpanReplication) GetStatus() *heartbeatpb.TableSpanStatus {
	return r.status.Load()
}

func (r *SpanReplication) GetMode() int64 {
	return r.status.Load().Mode
}

// UpdateStatus updates the replication status with the following rules:
//
//	The new status is only stored if its checkpointTs is greater than or equal to
//	the current status's checkpointTs.
func (r *SpanReplication) UpdateStatus(newStatus *heartbeatpb.TableSpanStatus) {
	if newStatus != nil {
		oldStatus := r.status.Load()
		if newStatus.CheckpointTs >= oldStatus.CheckpointTs {
			r.status.Store(newStatus)
		}
	}
}

// ShouldRun always returns true.
func (r *SpanReplication) ShouldRun() bool {
	return true
}

func (r *SpanReplication) IsWorking() bool {
	status := r.status.Load()
	return status.ComponentStatus == heartbeatpb.ComponentState_Working
}

func (r *SpanReplication) UpdateBlockState(newState heartbeatpb.State) {
	r.blockState.Store(&newState)
}

func (r *SpanReplication) GetSchemaID() int64 {
	return r.schemaID
}

func (r *SpanReplication) SetSchemaID(schemaID int64) {
	r.schemaID = schemaID
}

func (r *SpanReplication) SetNodeID(n node.ID) {
	r.nodeIDMutex.Lock()
	defer r.nodeIDMutex.Unlock()
	r.nodeID = n
}

func (r *SpanReplication) GetID() common.DispatcherID {
	return r.ID
}

func (r *SpanReplication) GetNodeID() node.ID {
	r.nodeIDMutex.Lock()
	defer r.nodeIDMutex.Unlock()
	return r.nodeID
}

// IsScheduled returns true if the span is scheduled to a node
func (r *SpanReplication) IsScheduled() bool {
	r.nodeIDMutex.Lock()
	defer r.nodeIDMutex.Unlock()
	return r.nodeID != ""
}

func (r *SpanReplication) GetGroupID() replica.GroupID {
	return r.groupID
}

func (r *SpanReplication) NewAddDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
				SchemaID:     r.schemaID,
				Span:         r.Span,
				StartTs:      r.status.Load().CheckpointTs,
				Mode:         r.GetMode(),
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		})
}

func (r *SpanReplication) NewRemoveDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return NewRemoveDispatcherMessage(server, r.ChangefeedID, r.ID.ToPB(), r.GetMode())
}

func NewRemoveDispatcherMessage(server node.ID, cfID common.ChangeFeedID, dispatcherID *heartbeatpb.DispatcherID, mode int64) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: cfID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: dispatcherID,
				Mode:         mode,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}
