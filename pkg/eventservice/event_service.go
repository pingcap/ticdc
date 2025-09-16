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

package eventservice

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type DispatcherInfo interface {
	// GetID returns the ID of the dispatcher.
	GetID() common.DispatcherID
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64
	GetTopic() string
	GetServerID() string
	GetTableSpan() *heartbeatpb.TableSpan
	GetStartTs() uint64
	GetActionType() eventpb.ActionType
	GetChangefeedID() common.ChangeFeedID
	GetFilter() filter.Filter

	// sync point related
	SyncPointEnabled() bool
	GetSyncPointTs() uint64
	GetSyncPointInterval() time.Duration

	IsOnlyReuse() bool
	GetBdrMode() bool
	GetIntegrity() *integrity.Config
	GetTimezone() *time.Location
	GetMode() int64
	GetEpoch() uint64
	IsOutputRawChangeEvent() bool
}

type DispatcherHeartBeatWithServerID struct {
	serverID  string
	heartbeat *event.DispatcherHeartbeat
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type eventService struct {
	mc          messaging.MessageCenter
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	// clusterID -> eventBroker
	brokers map[uint64]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	dispatcherInfoChan  chan DispatcherInfo
	dispatcherHeartbeat chan *DispatcherHeartBeatWithServerID
}

func New(eventStore eventstore.EventStore, schemaStore schemastore.SchemaStore) common.SubModule {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	es := &eventService{
		mc:                  mc,
		eventStore:          eventStore,
		schemaStore:         schemaStore,
		brokers:             make(map[uint64]*eventBroker),
		dispatcherInfoChan:  make(chan DispatcherInfo, 32),
		dispatcherHeartbeat: make(chan *DispatcherHeartBeatWithServerID, 32),
	}
	es.mc.RegisterHandler(messaging.EventServiceTopic, es.handleMessage)
	return es
}

func (s *eventService) Name() string {
	return appcontext.EventService
}

func (s *eventService) Run(ctx context.Context) error {
	log.Info("event service start to run")
	defer func() {
		log.Info("event service exited")
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	dispatcherChanSize := metrics.EventServiceChannelSizeGauge.WithLabelValues("dispatcherInfo")
	heartbeatChanSize := metrics.EventServiceChannelSizeGauge.WithLabelValues("heartbeat")
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			dispatcherChanSize.Set(float64(len(s.dispatcherInfoChan)))
			heartbeatChanSize.Set(float64(len(s.dispatcherHeartbeat)))
		case info := <-s.dispatcherInfoChan:
			switch info.GetActionType() {
			case eventpb.ActionType_ACTION_TYPE_REGISTER:
				s.registerDispatcher(ctx, info)
			case eventpb.ActionType_ACTION_TYPE_REMOVE:
				s.deregisterDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_PAUSE:
				s.pauseDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_RESUME:
				s.resumeDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_RESET:
				s.resetDispatcher(info)
			default:
				log.Panic("invalid action type", zap.Any("info", info))
			}
		case heartbeat := <-s.dispatcherHeartbeat:
			s.handleDispatcherHeartbeat(heartbeat)
		}
	}
}

func (s *eventService) Close(_ context.Context) error {
	log.Info("event service is closing")
	for _, c := range s.brokers {
		c.close()
	}
	log.Info("event service is closed")
	return nil
}

func (s *eventService) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeDispatcherRequest:
		infos := msgToDispatcherInfo(msg)
		for _, info := range infos {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.dispatcherInfoChan <- info:
			}
		}
	case messaging.TypeDispatcherHeartbeat:
		if len(msg.Message) != 1 {
			log.Panic("invalid dispatcher heartbeat", zap.Any("msg", msg))
		}
		heartbeat := msg.Message[0].(*event.DispatcherHeartbeat)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.dispatcherHeartbeat <- &DispatcherHeartBeatWithServerID{
			serverID:  msg.From.String(),
			heartbeat: heartbeat,
		}:
		}
	case messaging.TypeCongestionControl:
		if len(msg.Message) != 1 {
			log.Panic("invalid control message", zap.Any("msg", msg))
		}
		m := msg.Message[0].(*event.CongestionControl)
		s.handleCongestionControl(msg.From, m)
	default:
		log.Panic("unknown message type", zap.String("type", msg.Type.String()), zap.Any("message", msg))
	}
	return nil
}

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	clusterID := info.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(ctx, clusterID, s.eventStore, s.schemaStore, s.mc, info.GetTimezone(), info.GetIntegrity())
		s.brokers[clusterID] = c
	}

	// FIXME: Send message to the dispatcherManager to handle the error.
	err := c.addDispatcher(info)
	if err != nil {
		log.Error("add dispatcher to eventBroker failed", zap.Stringer("dispatcherID", info.GetID()), zap.Error(err))
	}
}

func (s *eventService) deregisterDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.removeDispatcher(dispatcherInfo)
}

func (s *eventService) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.pauseDispatcher(dispatcherInfo)
}

func (s *eventService) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.resumeDispatcher(dispatcherInfo)
}

func (s *eventService) resetDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	// TODO: handle the error
	_ = c.resetDispatcher(dispatcherInfo)
}

func (s *eventService) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	clusterID := heartbeat.heartbeat.ClusterID
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.handleDispatcherHeartbeat(heartbeat)
}

func (s *eventService) handleCongestionControl(from node.ID, m *event.CongestionControl) {
	clusterID := m.GetClusterID()
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	c.handleCongestionControl(from, m)
}

func msgToDispatcherInfo(msg *messaging.TargetMessage) []DispatcherInfo {
	res := make([]DispatcherInfo, 0, len(msg.Message))
	for _, m := range msg.Message {
		info, ok := m.(*messaging.DispatcherRequest)
		if !ok {
			log.Panic("invalid dispatcher info", zap.Any("info", m))
		}
		res = append(res, info)
	}
	return res
}
