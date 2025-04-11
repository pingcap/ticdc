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
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
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

	schemaID    int64
	nodeIDMutex sync.Mutex // mutex for nodeID
	nodeID      node.ID
	groupID     replica.GroupID
	status      *atomic.Pointer[heartbeatpb.TableSpanStatus]
	blockState  *atomic.Pointer[heartbeatpb.State]

	pdClock pdutil.Clock
}

func NewSpanReplication(cfID common.ChangeFeedID,
	id common.DispatcherID,
	pdClock pdutil.Clock,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	checkpointTs uint64,
) *SpanReplication {
	r := newSpanReplication(cfID, id, pdClock, SchemaID, span)
	r.initStatus(&heartbeatpb.TableSpanStatus{
		ID:           id.ToPB(),
		CheckpointTs: checkpointTs,
	})
	log.Info("new span replication created",
		zap.String("changefeedID", cfID.Name()),
		zap.String("id", id.String()),
		zap.Int64("schemaID", SchemaID),
		zap.Int64("tableID", span.TableID),
		zap.String("groupID", replica.GetGroupName(r.groupID)),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func NewWorkingSpanReplication(
	cfID common.ChangeFeedID,
	id common.DispatcherID,
	pdClock pdutil.Clock,
	SchemaID int64,
	span *heartbeatpb.TableSpan,
	status *heartbeatpb.TableSpanStatus,
	nodeID node.ID,
) *SpanReplication {
	r := newSpanReplication(cfID, id, pdClock, SchemaID, span)
	// Must set Node ID when creating a working span replication
	r.SetNodeID(nodeID)
	r.initStatus(status)
	log.Info("new working span replication created",
		zap.String("changefeedID", cfID.Name()),
		zap.String("id", id.String()),
		zap.String("nodeID", nodeID.String()),
		zap.Uint64("checkpointTs", status.CheckpointTs),
		zap.String("componentStatus", status.ComponentStatus.String()),
		zap.Int64("schemaID", SchemaID),
		zap.Int64("tableID", span.TableID),
		zap.String("groupID", replica.GetGroupName(r.groupID)),
		zap.String("start", hex.EncodeToString(span.StartKey)),
		zap.String("end", hex.EncodeToString(span.EndKey)))
	return r
}

func newSpanReplication(cfID common.ChangeFeedID, id common.DispatcherID, pdClock pdutil.Clock, SchemaID int64, span *heartbeatpb.TableSpan) *SpanReplication {
	r := &SpanReplication{
		ID:           id,
		pdClock:      pdClock,
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
			zap.String("changefeedID", r.ChangefeedID.Name()),
			zap.String("id", r.ID.String()),
			zap.Uint64("checkpointTs", status.CheckpointTs),
		)
	}
	r.status.Store(status)
}

func (r *SpanReplication) initGroupID() {
	r.groupID = replica.DefaultGroupID
	span := tablepb.Span{TableID: r.Span.TableID, StartKey: r.Span.StartKey, EndKey: r.Span.EndKey}
	// check if the table is split
	totalSpan := spanz.TableIDToComparableSpan(span.TableID)
	if !spanz.IsSubSpan(span, totalSpan) {
		log.Warn("invalid span range", zap.String("changefeedID", r.ChangefeedID.Name()),
			zap.String("id", r.ID.String()), zap.Int64("tableID", span.TableID),
			zap.String("totalSpan", totalSpan.String()),
			zap.String("start", hex.EncodeToString(span.StartKey)),
			zap.String("end", hex.EncodeToString(span.EndKey)))
	}
	if !bytes.Equal(span.StartKey, totalSpan.StartKey) || !bytes.Equal(span.EndKey, totalSpan.EndKey) {
		r.groupID = replica.GenGroupID(replica.GroupTable, span.TableID)
	}
}

func (r *SpanReplication) GetStatus() *heartbeatpb.TableSpanStatus {
	return r.status.Load()
}

// UpdateStatus updates the replication status with the following rules:
//  1. If there is a block state in WAITING stage and its blockTs is less than or equal to
//     the new status's checkpointTs, the update is **skipped** to prevent checkpoint advancement
//     during an ongoing block event.
//  2. The new status is only stored if its checkpointTs is greater than or equal to
//     the current status's checkpointTs.
func (r *SpanReplication) UpdateStatus(newStatus *heartbeatpb.TableSpanStatus) {
	if newStatus != nil {
		blockState := r.blockState.Load()
		if blockState != nil && blockState.Stage == heartbeatpb.BlockStage_WAITING && blockState.BlockTs <= newStatus.CheckpointTs {
			return
		}
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

func (r *SpanReplication) GetPDClock() pdutil.Clock {
	return r.pdClock
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

func (r *SpanReplication) NewAddDispatcherMessage(server node.ID) (*messaging.TargetMessage, error) {
	ts := r.pdClock.CurrentTS()

	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: r.ChangefeedID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: r.ID.ToPB(),
				SchemaID:     r.schemaID,
				Span:         r.Span,
				StartTs:      r.status.Load().CheckpointTs,
				CurrentPdTs:  ts,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Create,
		}), nil
}

func (r *SpanReplication) NewRemoveDispatcherMessage(server node.ID) *messaging.TargetMessage {
	return NewRemoveDispatcherMessage(server, r.ChangefeedID, r.ID.ToPB())
}

func NewRemoveDispatcherMessage(server node.ID, cfID common.ChangeFeedID, dispatcherID *heartbeatpb.DispatcherID) *messaging.TargetMessage {
	return messaging.NewSingleTargetMessage(server,
		messaging.HeartbeatCollectorTopic,
		&heartbeatpb.ScheduleDispatcherRequest{
			ChangefeedID: cfID.ToPB(),
			Config: &heartbeatpb.DispatcherConfig{
				DispatcherID: dispatcherID,
			},
			ScheduleAction: heartbeatpb.ScheduleAction_Remove,
		})
}
