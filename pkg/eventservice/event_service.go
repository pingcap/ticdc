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
	"strconv"
	"sync"
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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	GetEpoch() uint64
}

type DispatcherHeartBeatWithServerID struct {
	serverID  string
	heartbeat *event.DispatcherHeartbeat
}

type brokerMap struct {
	mu      sync.RWMutex
	brokers map[uint64]*eventBroker
}

func newBrokerMap() *brokerMap {
	return &brokerMap{
		brokers: make(map[uint64]*eventBroker),
	}
}

func (m *brokerMap) get(clusterID uint64) (*eventBroker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	broker, ok := m.brokers[clusterID]
	return broker, ok
}

func (m *brokerMap) set(clusterID uint64, broker *eventBroker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.brokers[clusterID] = broker
}

func (m *brokerMap) delete(clusterID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.brokers, clusterID)
}

func (m *brokerMap) values() []*eventBroker {
	m.mu.RLock()
	defer m.mu.RUnlock()
	values := make([]*eventBroker, 0, len(m.brokers))
	for _, broker := range m.brokers {
		values = append(values, broker)
	}
	return values
}

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type eventService struct {
	mc          messaging.MessageCenter
	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	// clusterID -> eventBroker
	brokers *brokerMap

	// dispatcher info channels - array of 8 channels for parallel processing
	dispatcherInfoChans [8]chan DispatcherInfo
	dispatcherHeartbeat chan *DispatcherHeartBeatWithServerID

	// worker control fields
	waitGroup *errgroup.Group
}

func New(eventStore eventstore.EventStore, schemaStore schemastore.SchemaStore) common.SubModule {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	es := &eventService{
		mc:                  mc,
		eventStore:          eventStore,
		schemaStore:         schemaStore,
		brokers:             newBrokerMap(),
		dispatcherHeartbeat: make(chan *DispatcherHeartBeatWithServerID, 32),
	}

	// Initialize dispatcher info channels
	for i := 0; i < 8; i++ {
		es.dispatcherInfoChans[i] = make(chan DispatcherInfo, 1000) // Buffer size for each channel
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

	// Create errgroup for managing goroutines
	s.waitGroup, ctx = errgroup.WithContext(ctx)

	// Start dispatcher heartbeat processor
	s.waitGroup.Go(func() error {
		s.handleDispatcherHeartbeatLoop(ctx)
		return nil
	})

	// Start dispatcher workers
	for i := 0; i < 8; i++ {
		workerID := i // Capture the loop variable
		s.waitGroup.Go(func() error {
			s.dispatcherWorker(ctx, workerID)
			return nil
		})
	}

	// Wait for all goroutines to complete
	return s.waitGroup.Wait()
}

// handleDispatcherHeartbeatLoop processes dispatcher heartbeat messages
func (s *eventService) handleDispatcherHeartbeatLoop(ctx context.Context) {
	log.Info("dispatcher heartbeat processor started")
	defer log.Info("dispatcher heartbeat processor stopped")

	heartbeatChanSize := metrics.EventServiceChannelSizeGauge.WithLabelValues("heartbeat", "0")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatChanSize.Set(float64(len(s.dispatcherHeartbeat)))
		case heartbeat := <-s.dispatcherHeartbeat:
			s.handleDispatcherHeartbeat(heartbeat)
		}
	}
}

// dispatcherWorker processes dispatcher tasks from the assigned channel
func (s *eventService) dispatcherWorker(ctx context.Context, workerID int) {
	dispatcherChanSize := metrics.EventServiceChannelSizeGauge.WithLabelValues("dispatcherInfo", strconv.Itoa(workerID))
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dispatcherChanSize.Set(float64(len(s.dispatcherInfoChans[workerID])))
		case info := <-s.dispatcherInfoChans[workerID]:
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
		}
	}
}

// hashDispatcherID returns a hash value for the dispatcher ID
func hashDispatcherID(id common.DispatcherID) int {
	return int((common.GID)(id).Hash(8))
}

func (s *eventService) Close(_ context.Context) error {
	log.Info("event service is closing")
	for _, c := range s.brokers.values() {
		c.close()
	}
	log.Info("event service is closed")
	return nil
}

func (s *eventService) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	start := time.Now()
	defer func() {
		log.Info("event service: handle message", zap.Any("time cost", time.Since(start)), zap.Any("msg type", msg.Type), zap.Any("msg", msg))
	}()
	switch msg.Type {
	case messaging.TypeDispatcherRequest:
		infos := msgToDispatcherInfo(msg)
		for _, info := range infos {
			// Hash the dispatcher ID to determine which channel to use
			channelIndex := hashDispatcherID(info.GetID())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.dispatcherInfoChans[channelIndex] <- info:
			}
		}
		log.Info("event service: handle message", zap.Any("time cost", time.Since(start)))
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
	default:
		log.Panic("unknown message type", zap.String("type", msg.Type.String()), zap.Any("message", msg))
	}
	return nil
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

func (s *eventService) registerDispatcher(ctx context.Context, info DispatcherInfo) {
	log.Info("event service: register dispatcher", zap.Any("info", info), zap.Any("info dispatcherID", info.GetID()))
	clusterID := info.GetClusterID()
	c, ok := s.brokers.get(clusterID)
	if !ok {
		c = newEventBroker(ctx, clusterID, s.eventStore, s.schemaStore, s.mc, info.GetTimezone(), info.GetIntegrity())
		s.brokers.set(clusterID, c)
	}

	// FIXME: Send message to the dispatcherManager to handle the error.
	err := c.addDispatcher(info)
	if err != nil {
		log.Error("add dispatcher to eventBroker failed", zap.Stringer("dispatcherID", info.GetID()), zap.Error(err))
	}
}

func (s *eventService) deregisterDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers.get(clusterID)
	if !ok {
		return
	}
	c.removeDispatcher(dispatcherInfo)
}

func (s *eventService) pauseDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers.get(clusterID)
	if !ok {
		return
	}
	c.pauseDispatcher(dispatcherInfo)
}

func (s *eventService) resumeDispatcher(dispatcherInfo DispatcherInfo) {
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers.get(clusterID)
	if !ok {
		return
	}
	c.resumeDispatcher(dispatcherInfo)
}

func (s *eventService) resetDispatcher(dispatcherInfo DispatcherInfo) {
	log.Info("event service: reset dispatcher", zap.Any("info", dispatcherInfo), zap.Any("info dispatcherID", dispatcherInfo.GetID()))
	clusterID := dispatcherInfo.GetClusterID()
	c, ok := s.brokers.get(clusterID)
	if !ok {
		log.Info("reset dispatcher, but the broker is not found, ignore it",
			zap.Stringer("dispatcherID", dispatcherInfo.GetID()),
			zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
			zap.Uint64("startTs", dispatcherInfo.GetStartTs()),
		)
		return
	}
	c.resetDispatcher(dispatcherInfo)
}

func (s *eventService) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	clusterID := heartbeat.heartbeat.ClusterID
	c, ok := s.brokers.get(clusterID)
	if !ok {
		return
	}
	c.handleDispatcherHeartbeat(heartbeat)
}
