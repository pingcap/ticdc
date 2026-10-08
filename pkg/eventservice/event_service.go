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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/atomic"
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
	IsLowLatencyMode() bool
	GetFilterConfig() *eventpb.FilterConfig
	GetTxnAtomicity() config.AtomicityLevel

	// sync point related
	SyncPointEnabled() bool
	GetSyncPointTs() uint64
	GetSyncPointInterval() time.Duration

	IsOnlyReuse() bool
	GetBdrMode() bool
	GetIntegrity() *integrity.Config
	GetMode() int64
	GetEpoch() uint64
	IsOutputRawChangeEvent() bool
	EnableIgnoreUpdateOnlyColumns() bool
}

type DispatcherHeartBeatWithServerID struct {
	serverID  string
	heartbeat *event.DispatcherHeartbeat
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type eventService struct {
	mc           messaging.MessageCenter
	eventStore   eventstore.EventStore
	schemaStore  schemastore.SchemaStore
	nodeLiveness *liveness.Liveness
	// clusterID -> eventBroker
	brokers   map[uint64]*eventBroker
	brokersMu sync.RWMutex
	// Protected by brokersMu.
	registrationsStopped bool
	// Incremented under brokersMu when admission succeeds; completion is lock-free.
	// In-flight registrations keep the drain count positive during initialization.
	registering atomic.Int64

	// TODO: use a better way to cache the acceptorInfos
	dispatcherInfoChan  chan DispatcherInfo
	dispatcherHeartbeat chan *DispatcherHeartBeatWithServerID

	tz *time.Location
}

func New(
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	nodeLiveness *liveness.Liveness,
) common.SubModule {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	serverConfig := config.GetGlobalServerConfig()
	tzName := serverConfig.TZ
	tz, err := util.GetTimezone(tzName)
	if err != nil {
		log.Panic("load timezone from server config failed",
			zap.String("timezone", tzName),
			zap.Error(err))
	}
	if serverConfig.DataDir != "" {
		spillDir := getLargeTxnInsertSpillDir()
		removed, err := cleanupLargeTxnInsertSpillFiles(spillDir)
		if err != nil {
			log.Warn("cleanup orphaned large transaction spill files failed",
				zap.String("spillDir", spillDir),
				zap.Error(err))
		} else if removed != 0 {
			log.Info("removed orphaned large transaction spill files",
				zap.String("spillDir", spillDir),
				zap.Int("removed", removed))
		}
	}
	es := &eventService{
		mc:                  mc,
		eventStore:          eventStore,
		schemaStore:         schemaStore,
		nodeLiveness:        nodeLiveness,
		tz:                  tz,
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
			// Serialize timeout removal with REGISTER/REMOVE/RESET. Store resources
			// are keyed by dispatcher ID and must be released before ID reuse.
			s.brokersMu.RLock()
			brokers := make([]*eventBroker, 0, len(s.brokers))
			for _, broker := range s.brokers {
				brokers = append(brokers, broker)
			}
			s.brokersMu.RUnlock()
			for _, broker := range brokers {
				broker.removeInactiveDispatchers()
			}
		case info := <-s.dispatcherInfoChan:
			switch info.GetActionType() {
			case eventpb.ActionType_ACTION_TYPE_REGISTER:
				s.registerDispatcher(ctx, info)
			case eventpb.ActionType_ACTION_TYPE_REMOVE:
				s.deregisterDispatcher(info)
			case eventpb.ActionType_ACTION_TYPE_RESET:
				s.resetDispatcher(info)
			default:
				log.Warn("invalid action type, ingore it", zap.Any("info", info))
			}
		case heartbeat := <-s.dispatcherHeartbeat:
			s.handleDispatcherHeartbeat(heartbeat)
		}
	}
}

func (s *eventService) Close(_ context.Context) error {
	log.Info("event service is closing")
	s.brokersMu.RLock()
	brokers := make([]*eventBroker, 0, len(s.brokers))
	for _, c := range s.brokers {
		brokers = append(brokers, c)
	}
	s.brokersMu.RUnlock()
	for _, c := range brokers {
		c.close()
	}
	log.Info("event service is closed")
	return nil
}

// GetDispatcherCount returns the number of dispatchers registered in all local
// event brokers, including table trigger dispatchers. It cannot report zero
// while an admitted registration is still in progress.
func (s *eventService) GetDispatcherCount() int {
	s.brokersMu.RLock()
	defer s.brokersMu.RUnlock()
	return s.getDispatcherCountLocked()
}

func (s *eventService) getDispatcherCountLocked() int {
	// Snapshot in-flight registrations before broker counts: completion can
	// publish a dispatcher and decrement registering without holding brokersMu.
	// Reading in the opposite order could miss the registration in both counts.
	registering := int(s.registering.Load())
	count := 0
	for _, broker := range s.brokers {
		count += int(broker.dispatcherCount.Load())
	}
	return max(count, registering)
}

// StopAcceptingRegistrations closes admission before an admission-closed report
// can report zero. Registrations admitted earlier remain visible in the count.
func (s *eventService) StopAcceptingRegistrations() {
	s.brokersMu.Lock()
	defer s.brokersMu.Unlock()
	s.stopAcceptingRegistrationsLocked()
}

func (s *eventService) stopAcceptingRegistrationsLocked() {
	s.registrationsStopped = true
	for _, broker := range s.brokers {
		broker.stopping.Store(true)
	}
}

// Reporting is handled by the message-center handler, independently of the
// request loop that may be waiting for schema initialization. Even an empty
// broker registry must explicitly report zero after closing admission.
func (s *eventService) reportDispatcherCount(target node.ID) {
	if s.nodeLiveness != nil && s.nodeLiveness.Load() == liveness.CaptureStopping {
		s.StopAcceptingRegistrations()
	}
	s.brokersMu.RLock()
	report := &logservicepb.EventBrokerDispatcherCount{
		DispatcherCount:      uint32(s.getDispatcherCountLocked()),
		RegistrationsStopped: s.registrationsStopped,
	}
	s.brokersMu.RUnlock()
	// The next LogCoordinator broadcast retries a lost report.
	_ = s.mc.SendEvent(messaging.NewSingleTargetMessage(target, messaging.LogCoordinatorTopic, report))
}

func (s *eventService) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeLogCoordinatorBroadcastRequest:
		s.reportDispatcherCount(msg.From)
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
			log.Warn("invalid dispatcher heartbeat, ignore it", zap.Any("msg", msg))
			return nil
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
			log.Warn("invalid control message, ignore it", zap.Any("msg", msg))
			return nil
		}
		m := msg.Message[0].(*event.CongestionControl)
		s.handleCongestionControl(msg.From, m)
	default:
		log.Warn("unknown message type, ignore it", zap.String("type", msg.Type.String()), zap.Any("message", msg))
	}
	return nil
}

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	clusterID := info.GetClusterID()
	s.brokersMu.Lock()
	if s.nodeLiveness != nil && s.nodeLiveness.Load() == liveness.CaptureStopping {
		s.stopAcceptingRegistrationsLocked()
	}
	if s.registrationsStopped {
		s.brokersMu.Unlock()
		if info.IsOnlyReuse() {
			topic := messaging.EventCollectorTopic
			if common.IsRedoMode(info.GetMode()) {
				topic = messaging.RedoEventCollectorTopic
			}
			rejection := event.NewNotReusableEvent(info.GetID())
			msg := messaging.NewSingleTargetMessage(node.ID(info.GetServerID()), topic, &rejection)
			// A lost rejection is retried when the pending consumer heartbeats.
			_ = s.mc.SendEvent(msg)
		}
		return
	}
	s.registering.Inc()
	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(ctx, clusterID, s.eventStore, s.schemaStore, s.mc, s.tz, info.GetIntegrity())
		s.brokers[clusterID] = c
	}
	s.brokersMu.Unlock()
	defer s.registering.Dec()

	// FIXME: Send message to the dispatcherManager to handle the error.
	err := c.addDispatcher(info)
	if err != nil {
		log.Error("add dispatcher to eventBroker failed", zap.Stringer("dispatcherID", info.GetID()), zap.Error(err))
	}
}

func (s *eventService) deregisterDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	s.brokersMu.RLock()
	c, ok := s.brokers[clusterID]
	s.brokersMu.RUnlock()
	if !ok {
		return
	}
	c.removeDispatcher(dispatcherInfo)
}

func (s *eventService) resetDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	s.brokersMu.RLock()
	c, ok := s.brokers[clusterID]
	s.brokersMu.RUnlock()
	if !ok {
		return
	}
	// TODO: handle the error
	_ = c.resetDispatcher(dispatcherInfo)
}

func (s *eventService) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	clusterID := heartbeat.heartbeat.ClusterID
	s.brokersMu.RLock()
	c, ok := s.brokers[clusterID]
	s.brokersMu.RUnlock()
	if !ok {
		response := event.NewDispatcherHeartbeatResponse()
		response.ClusterID = clusterID
		if heartbeat.heartbeat.Version >= event.DispatcherHeartbeatVersion2 {
			for _, progress := range heartbeat.heartbeat.DispatcherProgresses {
				response.Append(event.NewDispatcherState(progress.DispatcherID, event.DSStateRemoved))
			}
		} else {
			for _, progress := range heartbeat.heartbeat.DispatcherProgressesLegacy {
				response.Append(event.NewDispatcherState(progress.DispatcherID, event.DSStateRemoved))
			}
		}
		_ = s.mc.SendCommand(messaging.NewSingleTargetMessage(node.ID(heartbeat.serverID), messaging.EventCollectorTopic, response))
		return
	}
	c.handleDispatcherHeartbeat(heartbeat)
}

func (s *eventService) handleCongestionControl(from node.ID, m *event.CongestionControl) {
	clusterID := m.GetClusterID()
	s.brokersMu.RLock()
	c, ok := s.brokers[clusterID]
	s.brokersMu.RUnlock()
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
			log.Warn("invalid dispatcher info, ignore it", zap.Any("info", m))
		}
		res = append(res, info)
	}
	return res
}
