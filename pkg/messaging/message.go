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

package messaging

import (
	"fmt"
	"slices"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type IOType int32

var LogServiceEventTypes = []IOType{
	TypeBatchDMLEvent,
	TypeDDLEvent,
	TypeBatchResolvedTs,
	TypeSyncPointEvent,
	TypeHandshakeEvent,
	TypeReadyEvent,
	TypeNotReusableEvent,
}

func (t IOType) IsLogServiceEvent() bool {
	return slices.Contains(LogServiceEventTypes, t)
}

const (
	TypeInvalid IOType = iota
	// LogService related
	TypeBatchDMLEvent
	TypeDDLEvent
	TypeBatchResolvedTs
	TypeSyncPointEvent
	TypeHandshakeEvent
	TypeReadyEvent
	TypeNotReusableEvent

	// LogCoordinator related
	TypeLogCoordinatorBroadcastRequest
	TypeEventStoreState
	TypeReusableEventServiceRequest
	TypeReusableEventServiceResponse
	TypeLogCoordinatorResolvedTsRequest

	// EventCollector related
	TypeHeartBeatRequest
	TypeHeartBeatResponse
	TypeScheduleDispatcherRequest
	TypeDispatcherRequest
	TypeCheckpointTsMessage
	TypeBlockStatusRequest
	TypeDispatcherHeartbeat
	TypeDispatcherHeartbeatResponse
	TypeRedoMessage
	TypeMergeDispatcherRequest
	TypeCongestionControl

	// Coordinator related
	TypeCoordinatorBootstrapRequest
	TypeCoordinatorBootstrapResponse
	TypeAddMaintainerRequest
	TypeRemoveMaintainerRequest
	TypeMaintainerHeartbeatRequest
	TypeMaintainerBootstrapRequest
	TypeMaintainerBootstrapResponse
	TypeMaintainerPostBootstrapRequest
	TypeMaintainerPostBootstrapResponse
	TypeMaintainerCloseRequest
	TypeMaintainerCloseResponse
	TypeLogCoordinatorResolvedTsResponse

	TypeMessageHandShake

	// used to upload changefeed metrics from event store to log coordinator
	TypeLogCoordinatorChangefeedStates
)

func (t IOType) String() string {
	switch t {
	case TypeBatchDMLEvent:
		return "BatchDMLEvent"
	case TypeDDLEvent:
		return "DDLEvent"
	case TypeSyncPointEvent:
		return "SyncPointEvent"
	case TypeBatchResolvedTs:
		return "BatchResolvedTs"
	case TypeHandshakeEvent:
		return "HandshakeEvent"
	case TypeReadyEvent:
		return "TypeReadyEvent"
	case TypeNotReusableEvent:
		return "TypeNotReusableEvent"
	case TypeLogCoordinatorBroadcastRequest:
		return "TypeLogCoordinatorBroadcastRequest"
	case TypeLogCoordinatorResolvedTsRequest:
		return "TypeLogCoordinatorResolvedTsRequest"
	case TypeLogCoordinatorResolvedTsResponse:
		return "TypeLogCoordinatorResolvedTsResponse"
	case TypeReusableEventServiceRequest:
		return "TypeReusableEventServiceRequest"
	case TypeReusableEventServiceResponse:
		return "TypeReusableEventServiceResponse"
	case TypeEventStoreState:
		return "TypeEventStoreState"
	case TypeHeartBeatRequest:
		return "HeartBeatRequest"
	case TypeHeartBeatResponse:
		return "HeartBeatResponse"
	case TypeBlockStatusRequest:
		return "BlockStatusRequest"
	case TypeScheduleDispatcherRequest:
		return "ScheduleDispatcherRequest"
	case TypeCoordinatorBootstrapRequest:
		return "CoordinatorBootstrapRequest"
	case TypeAddMaintainerRequest:
		return "AddMaintainerRequest"
	case TypeRemoveMaintainerRequest:
		return "RemoveMaintainerRequest"
	case TypeMaintainerHeartbeatRequest:
		return "MaintainerHeartbeatRequest"
	case TypeCoordinatorBootstrapResponse:
		return "CoordinatorBootstrapResponse"
	case TypeDispatcherRequest:
		return "DispatcherRequest"
	case TypeMaintainerBootstrapRequest:
		return "MaintainerBootstrapRequest"
	case TypeMaintainerBootstrapResponse:
		return "MaintainerBootstrapResponse"
	case TypeMaintainerPostBootstrapRequest:
		return "MaintainerPostBootstrapRequest"
	case TypeMaintainerPostBootstrapResponse:
		return "MaintainerPostBootstrapResponse"
	case TypeMaintainerCloseRequest:
		return "MaintainerCloseRequest"
	case TypeMaintainerCloseResponse:
		return "MaintainerCloseResponse"
	case TypeMessageHandShake:
		return "MessageHandShake"
	case TypeCheckpointTsMessage:
		return "CheckpointTsMessage"
	case TypeDispatcherHeartbeat:
		return "DispatcherHeartbeat"
	case TypeRedoMessage:
		return "RedoMessage"

	case TypeDispatcherHeartbeatResponse:
		return "DispatcherHeartbeatResponse"
	case TypeCongestionControl:
		return "CongestionControl"
	case TypeMergeDispatcherRequest:
		return "MergeDispatcherRequest"
	case TypeLogCoordinatorChangefeedStates:
		return "TypeLogCoordinatorChangefeedStates"
	default:
	}
	return "Unknown"
}

type DispatcherRequest struct {
	*eventpb.DispatcherRequest
}

func (r DispatcherRequest) Marshal() ([]byte, error) {
	return r.DispatcherRequest.Marshal()
}

func (r DispatcherRequest) Unmarshal(data []byte) error {
	return r.DispatcherRequest.Unmarshal(data)
}

func (r DispatcherRequest) GetID() common.DispatcherID {
	return common.NewDispatcherIDFromPB(r.DispatcherId)
}

func (r DispatcherRequest) GetClusterID() uint64 {
	return r.ClusterId
}

func (r DispatcherRequest) GetTopic() string {
	return EventCollectorTopic
}

func (r DispatcherRequest) GetServerID() string {
	return r.ServerId
}

func (r DispatcherRequest) GetTableSpan() *heartbeatpb.TableSpan {
	return r.TableSpan
}

func (r DispatcherRequest) GetStartTs() uint64 {
	return r.StartTs
}

func (r DispatcherRequest) GetChangefeedID() common.ChangeFeedID {
	return common.NewChangefeedIDFromPB(r.ChangefeedId)
}

func (r DispatcherRequest) GetFilter() filter.Filter {
	changefeedID := r.GetChangefeedID()
	filter, err := filter.
		GetSharedFilterStorage().
		GetOrSetFilter(changefeedID, r.DispatcherRequest.FilterConfig, r.GetTimezone().String())
	if err != nil {
		log.Panic("create filter failed", zap.Error(err), zap.Any("filterConfig", r.DispatcherRequest.FilterConfig))
	}
	return filter
}

func (r DispatcherRequest) SyncPointEnabled() bool {
	return r.EnableSyncPoint
}

func (r DispatcherRequest) GetSyncPointTs() uint64 {
	return r.SyncPointTs
}

func (r DispatcherRequest) GetSyncPointInterval() time.Duration {
	return time.Duration(r.SyncPointInterval) * time.Second
}

func (r DispatcherRequest) IsOnlyReuse() bool {
	return r.OnlyReuse
}

func (r DispatcherRequest) GetBdrMode() bool {
	return r.BdrMode
}

func (r DispatcherRequest) GetIntegrity() *integrity.Config {
	if r.DispatcherRequest.Integrity == nil {
		return &integrity.Config{
			IntegrityCheckLevel:   integrity.CheckLevelNone,
			CorruptionHandleLevel: integrity.CorruptionHandleLevelWarn,
		}
	}
	integrity := integrity.Config(*r.DispatcherRequest.Integrity)
	return &integrity
}

func (r DispatcherRequest) GetTimezone() *time.Location {
	tz, err := util.GetTimezone(r.DispatcherRequest.GetTimezone())
	if err != nil {
		log.Panic("Can't load time zone from dispatcher info", zap.Error(err))
	}
	return tz
}

func (r DispatcherRequest) GetEpoch() uint64 {
	return r.Epoch
}

func (r DispatcherRequest) IsOutputRawChangeEvent() bool {
	return r.OutputRawChangeEvent
}

type IOTypeT interface {
	Unmarshal(data []byte) error
	Marshal() (data []byte, err error)
}

func decodeIOType(ioType IOType, value []byte) (IOTypeT, error) {
	var m IOTypeT
	switch ioType {
	case TypeBatchDMLEvent:
		m = &commonEvent.BatchDMLEvent{}
	case TypeDDLEvent:
		m = &commonEvent.DDLEvent{}
	case TypeSyncPointEvent:
		m = &commonEvent.SyncPointEvent{}
	case TypeBatchResolvedTs:
		m = &commonEvent.BatchResolvedEvent{}
	case TypeHandshakeEvent:
		m = &commonEvent.HandshakeEvent{}
	case TypeReadyEvent:
		m = &commonEvent.ReadyEvent{}
	case TypeNotReusableEvent:
		m = &commonEvent.NotReusableEvent{}
	case TypeLogCoordinatorBroadcastRequest:
		m = &common.LogCoordinatorBroadcastRequest{}
	case TypeEventStoreState:
		m = &logservicepb.EventStoreState{}
	case TypeReusableEventServiceRequest:
		m = &logservicepb.ReusableEventServiceRequest{}
	case TypeReusableEventServiceResponse:
		m = &logservicepb.ReusableEventServiceResponse{}
	case TypeHeartBeatRequest:
		m = &heartbeatpb.HeartBeatRequest{}
	case TypeHeartBeatResponse:
		m = &heartbeatpb.HeartBeatResponse{}
	case TypeBlockStatusRequest:
		m = &heartbeatpb.BlockStatusRequest{}
	case TypeScheduleDispatcherRequest:
		m = &heartbeatpb.ScheduleDispatcherRequest{}
	case TypeCoordinatorBootstrapRequest:
		m = &heartbeatpb.CoordinatorBootstrapRequest{}
	case TypeAddMaintainerRequest:
		m = &heartbeatpb.AddMaintainerRequest{}
	case TypeRemoveMaintainerRequest:
		m = &heartbeatpb.RemoveMaintainerRequest{}
	case TypeMaintainerHeartbeatRequest:
		m = &heartbeatpb.MaintainerHeartbeat{}
	case TypeCoordinatorBootstrapResponse:
		m = &heartbeatpb.CoordinatorBootstrapResponse{}
	case TypeDispatcherRequest:
		m = &DispatcherRequest{
			DispatcherRequest: &eventpb.DispatcherRequest{},
		}
	case TypeMaintainerBootstrapResponse:
		m = &heartbeatpb.MaintainerBootstrapResponse{}
	case TypeMaintainerPostBootstrapRequest:
		m = &heartbeatpb.MaintainerPostBootstrapRequest{}
	case TypeMaintainerPostBootstrapResponse:
		m = &heartbeatpb.MaintainerPostBootstrapResponse{}
	case TypeMaintainerCloseRequest:
		m = &heartbeatpb.MaintainerCloseRequest{}
	case TypeMaintainerCloseResponse:
		m = &heartbeatpb.MaintainerCloseResponse{}
	case TypeMaintainerBootstrapRequest:
		m = &heartbeatpb.MaintainerBootstrapRequest{}
	case TypeCheckpointTsMessage:
		m = &heartbeatpb.CheckpointTsMessage{}
	case TypeDispatcherHeartbeat:
		m = &commonEvent.DispatcherHeartbeat{}
	case TypeDispatcherHeartbeatResponse:
		m = &commonEvent.DispatcherHeartbeatResponse{}
	case TypeRedoMessage:
		m = &heartbeatpb.RedoMessage{}
	case TypeCongestionControl:
		m = &commonEvent.CongestionControl{}
	case TypeMergeDispatcherRequest:
		m = &heartbeatpb.MergeDispatcherRequest{}
	case TypeLogCoordinatorChangefeedStates:
		m = &logservicepb.ChangefeedStates{}
	case TypeLogCoordinatorResolvedTsRequest:
		m = &heartbeatpb.LogCoordinatorResolvedTsRequest{}
	case TypeLogCoordinatorResolvedTsResponse:
		m = &heartbeatpb.LogCoordinatorResolvedTsResponse{}
	default:
		log.Panic("Unimplemented IOType", zap.Stringer("Type", ioType))
	}
	err := m.Unmarshal(value)
	return m, err
}

// TargetMessage is a wrapper of message to be sent to a target server.
// It contains the source server id, the target server id, the message type and the message.
type TargetMessage struct {
	From     node.ID
	To       node.ID
	Epoch    uint64
	Sequence uint64
	Topic    string
	Type     IOType
	Message  []IOTypeT
	CreateAt int64

	// Group is used to group messages into a same group.
	// Different groups can be processed in different goroutines.
	Group uint64
}

// NewSingleTargetMessage creates a new TargetMessage to be sent to a target server, with a single message.
// Group is used to group messages into a same group.
// Different groups can be processed in different goroutines by the target server.
// The Group is optional, if not specified, the Group will be 0.
func NewSingleTargetMessage(To node.ID, Topic string, Message IOTypeT, Group ...uint64) *TargetMessage {
	var ioType IOType
	switch Message.(type) {
	case *commonEvent.BatchDMLEvent:
		ioType = TypeBatchDMLEvent
	case *commonEvent.DDLEvent:
		ioType = TypeDDLEvent
	case *commonEvent.SyncPointEvent:
		ioType = TypeSyncPointEvent
	case *commonEvent.BatchResolvedEvent:
		ioType = TypeBatchResolvedTs
	case *commonEvent.HandshakeEvent:
		ioType = TypeHandshakeEvent
	case *commonEvent.ReadyEvent:
		ioType = TypeReadyEvent
	case *commonEvent.NotReusableEvent:
		ioType = TypeNotReusableEvent
	case *common.LogCoordinatorBroadcastRequest:
		ioType = TypeLogCoordinatorBroadcastRequest
	case *logservicepb.EventStoreState:
		ioType = TypeEventStoreState
	case *logservicepb.ReusableEventServiceRequest:
		ioType = TypeReusableEventServiceRequest
	case *logservicepb.ReusableEventServiceResponse:
		ioType = TypeReusableEventServiceResponse
	case *heartbeatpb.HeartBeatRequest:
		ioType = TypeHeartBeatRequest
	case *heartbeatpb.BlockStatusRequest:
		ioType = TypeBlockStatusRequest
	case *heartbeatpb.ScheduleDispatcherRequest:
		ioType = TypeScheduleDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapRequest:
		ioType = TypeMaintainerBootstrapRequest
	case *heartbeatpb.AddMaintainerRequest:
		ioType = TypeAddMaintainerRequest
	case *heartbeatpb.RemoveMaintainerRequest:
		ioType = TypeRemoveMaintainerRequest
	case *heartbeatpb.CoordinatorBootstrapRequest:
		ioType = TypeCoordinatorBootstrapRequest
	case *heartbeatpb.HeartBeatResponse:
		ioType = TypeHeartBeatResponse
	case *heartbeatpb.MaintainerHeartbeat:
		ioType = TypeMaintainerHeartbeatRequest
	case *heartbeatpb.CoordinatorBootstrapResponse:
		ioType = TypeCoordinatorBootstrapResponse
	case *DispatcherRequest:
		ioType = TypeDispatcherRequest
	case *heartbeatpb.MaintainerBootstrapResponse:
		ioType = TypeMaintainerBootstrapResponse
	case *heartbeatpb.MaintainerPostBootstrapRequest:
		ioType = TypeMaintainerPostBootstrapRequest
	case *heartbeatpb.MaintainerPostBootstrapResponse:
		ioType = TypeMaintainerPostBootstrapResponse
	case *heartbeatpb.MaintainerCloseRequest:
		ioType = TypeMaintainerCloseRequest
	case *heartbeatpb.MaintainerCloseResponse:
		ioType = TypeMaintainerCloseResponse
	case *heartbeatpb.CheckpointTsMessage:
		ioType = TypeCheckpointTsMessage
	case *commonEvent.DispatcherHeartbeat:
		ioType = TypeDispatcherHeartbeat
	case *commonEvent.DispatcherHeartbeatResponse:
		ioType = TypeDispatcherHeartbeatResponse
	case *heartbeatpb.RedoMessage:
		ioType = TypeRedoMessage
	case *commonEvent.CongestionControl:
		ioType = TypeCongestionControl
	case *heartbeatpb.MergeDispatcherRequest:
		ioType = TypeMergeDispatcherRequest
	case *logservicepb.ChangefeedStates:
		ioType = TypeLogCoordinatorChangefeedStates
	case *heartbeatpb.LogCoordinatorResolvedTsRequest:
		ioType = TypeLogCoordinatorResolvedTsRequest
	case *heartbeatpb.LogCoordinatorResolvedTsResponse:
		ioType = TypeLogCoordinatorResolvedTsResponse
	default:
		panic("unknown io type")
	}

	var group uint64
	if len(Group) > 0 {
		group = Group[0]
	}

	return &TargetMessage{
		To:       To,
		Type:     ioType,
		Topic:    Topic,
		Message:  []IOTypeT{Message},
		CreateAt: time.Now().UnixMilli(),
		Group:    group,
	}
}

func (m *TargetMessage) String() string {
	return fmt.Sprintf("From: %s, To: %s, Type: %s, Message: %v", m.From, m.To, m.Type, m.Message)
}

func (m *TargetMessage) GetGroup() uint64 {
	return m.Group
}
