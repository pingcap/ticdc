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

package eventcollector

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	receiveChanSize = 1024 * 8
)

var (
	metricsHandleEventDuration = metrics.EventCollectorHandleEventDuration
	metricsDSInputChanLen      = metrics.DynamicStreamEventChanSize.WithLabelValues("event-collector")
	metricsDSPendingQueueLen   = metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-collector")
	metricsDSMaxMemoryUsage    = metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector", "max")
	metricsDSUsedMemoryUsage   = metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector", "used")
)

type DispatcherRequest struct {
	Dispatcher dispatcher.EventDispatcher
	ActionType eventpb.ActionType
	StartTs    uint64
	OnlyUse    bool
}

type DispatcherRequestWithTarget struct {
	Target node.ID
	Topic  string
	Req    DispatcherRequest
}

const (
	eventServiceTopic         = messaging.EventServiceTopic
	eventCollectorTopic       = messaging.EventCollectorTopic
	logCoordinatorTopic       = messaging.LogCoordinatorTopic
	typeRegisterDispatcherReq = messaging.TypeRegisterDispatcherRequest
)

/*
EventCollector is the relay between EventService and DispatcherManager, responsible for:
1. Send dispatcher request to EventService.
2. Collect the events from EvenService and dispatch them to different dispatchers.
EventCollector is an instance-level component.
*/
type EventCollector struct {
	serverId      node.ID
	dispatcherMap sync.Map
	mc            messaging.MessageCenter

	// dispatcherRequestChan cached dispatcher request when some error occurs.
	dispatcherRequestChan *chann.DrainableChann[DispatcherRequestWithTarget]

	logCoordinatorRequestChan *chann.DrainableChann[*logservicepb.ReusableEventServiceRequest]

	receiveChannels []chan *messaging.TargetMessage
	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]

	coordinatorInfo struct {
		sync.RWMutex
		id node.ID
	}

	wg     sync.WaitGroup
	cancel context.CancelFunc

	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
}

func New(serverId node.ID) *EventCollector {
	eventCollector := EventCollector{
		serverId:                             serverId,
		dispatcherMap:                        sync.Map{},
		dispatcherRequestChan:                chann.NewAutoDrainChann[DispatcherRequestWithTarget](),
		logCoordinatorRequestChan:            chann.NewAutoDrainChann[*logservicepb.ReusableEventServiceRequest](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		receiveChannels:                      make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency),
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
	}
	eventCollector.ds = NewEventDynamicStream(&eventCollector)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.RecvEventsMessage)

	return &eventCollector
}

func (c *EventCollector) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	for i := 0; i < config.DefaultBasicEventHandlerConcurrency; i++ {
		ch := make(chan *messaging.TargetMessage, receiveChanSize)
		c.receiveChannels[i] = ch
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runProcessMessage(ctx, ch)
		}()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.processFeedback(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.processDispatcherRequests(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.processLogCoordinatorRequest(ctx)
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.updateMetrics(ctx)
	}()
}

func (c *EventCollector) Close() {
	c.cancel()
	c.ds.Close()
	c.wg.Wait()
	log.Info("event collector is closed")
}

func (c *EventCollector) AddDispatcher(target dispatcher.EventDispatcher, memoryQuota int) {
	log.Info("add dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("add dispatcher done", zap.Stringer("dispatcher", target.GetId()))
	}()
	stat := &dispatcherStat{
		dispatcherID: target.GetId(),
		target:       target,
	}
	stat.reset()
	stat.sentCommitTs.Store(target.GetStartTs())
	c.dispatcherMap.Store(target.GetId(), stat)
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota)
	err := c.ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Info("add dispatcher to dynamic stream failed", zap.Error(err))
	}

	// TODO: handle the return error(now even it return error, it will be retried later, we can just ignore it now)
	c.mustSendDispatcherRequest(c.serverId, eventServiceTopic, DispatcherRequest{
		Dispatcher: target,
		StartTs:    target.GetStartTs(),
		ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
	})

	c.logCoordinatorRequestChan.In() <- &logservicepb.ReusableEventServiceRequest{
		ID:      target.GetId().ToPB(),
		Span:    target.GetTableSpan(),
		StartTs: target.GetStartTs(),
	}
}

func (c *EventCollector) RemoveDispatcher(target *dispatcher.Dispatcher) {
	log.Info("remove dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("remove dispatcher done", zap.Stringer("dispatcher", target.GetId()))
	}()
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		return
	}
	stat := value.(*dispatcherStat)
	stat.unregisterDispatcher(c)
	c.dispatcherMap.Delete(target.GetId())

	err := c.ds.RemovePath(target.GetId())
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
}

func (c *EventCollector) WakeDispatcher(dispatcherID common.DispatcherID) {
	c.ds.Wake(dispatcherID)
}

// resetDispatcher is used to reset the dispatcher when it receives a out-of-order event.
// It will send a reset request to the event service to reset the remote dispatcher.
// And it will reset the dispatcher stat to wait for a new handshake event.
func (c *EventCollector) resetDispatcher(d *dispatcherStat) {
	c.addDispatcherRequestToSendingQueue(
		d.eventServiceInfo.serverID,
		eventServiceTopic,
		DispatcherRequest{
			Dispatcher: d.target,
			StartTs:    d.sentCommitTs.Load(),
			ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
		})
	d.reset()
	log.Info("Send reset dispatcher request to event service",
		zap.Stringer("dispatcher", d.target.GetId()),
		zap.Uint64("startTs", d.sentCommitTs.Load()))
}

func (c *EventCollector) addDispatcherRequestToSendingQueue(serverId node.ID, topic string, req DispatcherRequest) {
	c.dispatcherRequestChan.In() <- DispatcherRequestWithTarget{
		Target: serverId,
		Topic:  topic,
		Req:    req,
	}
}

func (c *EventCollector) processFeedback(ctx context.Context) {
	log.Info("Start process feedback from dynamic stream")
	defer log.Info("Stop process feedback from dynamic stream")
	for {
		select {
		case <-ctx.Done():
			return
		case feedback := <-c.ds.Feedback():
			if feedback.IsAreaFeedback() {
				if feedback.IsPauseArea() {
					feedback.Dest.pauseChangefeed(c)
				} else {
					feedback.Dest.resumeChangefeed(c)
				}
			}

			if feedback.IsPausePath() {
				feedback.Dest.pauseDispatcher(c)
			} else {
				feedback.Dest.resumeDispatcher(c)
			}
		}
	}
}

func (c *EventCollector) processDispatcherRequests(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.dispatcherRequestChan.Out():
			if err := c.mustSendDispatcherRequest(req.Target, req.Topic, req.Req); err != nil {
				// Sleep a short time to avoid too many requests in a short time.
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (c *EventCollector) processLogCoordinatorRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.logCoordinatorRequestChan.Out():
			c.coordinatorInfo.RLock()
			targetMessage := messaging.NewSingleTargetMessage(c.coordinatorInfo.id, logCoordinatorTopic, req)
			c.coordinatorInfo.RUnlock()
			err := c.mc.SendCommand(targetMessage)
			if err != nil {
				log.Info("fail to send dispatcher request message to log coordinator, try again later", zap.Error(err))
				c.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// mustSendDispatcherRequest will keep retrying to send the dispatcher request to EventService until it succeed.
// Caller should avoid to use this method when the remote EventService is offline forever.
// And this method may be deprecated in the future.
// FIXME: Add a checking mechanism to avoid sending request to offline EventService.
// A simple way is to use a NodeManager to check if the target is online.
func (c *EventCollector) mustSendDispatcherRequest(target node.ID, topic string, req DispatcherRequest) error {
	message := &messaging.RegisterDispatcherRequest{
		RegisterDispatcherRequest: &eventpb.RegisterDispatcherRequest{
			ChangefeedId: req.Dispatcher.GetChangefeedID().ToPB(),
			DispatcherId: req.Dispatcher.GetId().ToPB(),
			ActionType:   req.ActionType,
			// ServerId is the id of the request sender.
			ServerId:  c.serverId.String(),
			TableSpan: req.Dispatcher.GetTableSpan(),
			StartTs:   req.StartTs,
			OnlyReuse: req.OnlyUse,
		},
	}

	// If the action type is register and reset, we need fill all config related fields.
	if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER ||
		req.ActionType == eventpb.ActionType_ACTION_TYPE_RESET {
		message.RegisterDispatcherRequest.FilterConfig = req.Dispatcher.GetFilterConfig()
		message.RegisterDispatcherRequest.EnableSyncPoint = req.Dispatcher.EnableSyncPoint()
		message.RegisterDispatcherRequest.SyncPointInterval = uint64(req.Dispatcher.GetSyncPointInterval().Seconds())
		message.RegisterDispatcherRequest.SyncPointTs = syncpoint.CalculateStartSyncPointTs(req.StartTs, req.Dispatcher.GetSyncPointInterval())
	}

	err := c.mc.SendCommand(&messaging.TargetMessage{
		To:      target,
		Topic:   eventServiceTopic,
		Type:    typeRegisterDispatcherReq,
		Message: []messaging.IOTypeT{message},
	})
	if err != nil {
		log.Info("failed to send dispatcher request message to event service, try again later",
			zap.String("changefeedID", req.Dispatcher.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", req.Dispatcher.GetId()),
			zap.Any("target", target.String()),
			zap.Any("request", req),
			zap.Error(err))
		// Put the request back to the channel for later retry.
		c.dispatcherRequestChan.In() <- DispatcherRequestWithTarget{
			Target: target,
			Topic:  topic,
			Req:    req,
		}
		return err
	}
	return nil
}

// RecvEventsMessage is the handler for the events message from EventService.
func (c *EventCollector) RecvEventsMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.UnixMilli(targetMessage.CreateAt)).Seconds()
	c.metricReceiveEventLagDuration.Observe(inflightDuration)
	start := time.Now()
	defer func() {
		metricsHandleEventDuration.Observe(time.Since(start).Seconds())
	}()

	// If the message is a log service event, we need to forward it to the
	// corresponding channel to handle it in multi-thread.
	if targetMessage.Type.IsLogServiceEvent() {
		c.receiveChannels[targetMessage.GetGroup()%uint64(len(c.receiveChannels))] <- targetMessage
		return nil
	}

	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			c.coordinatorInfo.Lock()
			c.coordinatorInfo.id = targetMessage.From
			c.coordinatorInfo.Unlock()
		case *logservicepb.ReusableEventServiceResponse:
			// TODO: can we handle it here?
			value, ok := c.dispatcherMap.Load(msg.(*logservicepb.ReusableEventServiceResponse).ID)
			if !ok {
				continue
			}
			value.(*dispatcherStat).setRemoteCandidates(msg.(*logservicepb.ReusableEventServiceResponse).Nodes, c)
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

func (c *EventCollector) runProcessMessage(ctx context.Context, inCh <-chan *messaging.TargetMessage) {
	for {
		select {
		case <-ctx.Done():
			return
		case targetMessage := <-inCh:
			for _, msg := range targetMessage.Message {
				switch msg.(type) {
				case commonEvent.Event:
					event := msg.(commonEvent.Event)
					switch event.GetType() {
					case commonEvent.TypeBatchResolvedEvent:
						events := event.(*commonEvent.BatchResolvedEvent).Events
						from := &targetMessage.From
						event := dispatcher.DispatcherEvent{}
						for _, e := range events {
							event.From = from
							event.Event = e
							c.ds.Push(e.DispatcherID, event)
						}
						c.metricDispatcherReceivedResolvedTsEventCount.Add(float64(len(events)))
					default:
						c.metricDispatcherReceivedKVEventCount.Inc()
						c.ds.Push(event.GetDispatcherID(), dispatcher.NewDispatcherEvent(&targetMessage.From, event))
					}
				default:
					log.Panic("invalid message type", zap.Any("msg", msg))
				}
			}
		}
	}
}

func (c *EventCollector) updateMetrics(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dsMetrics := c.ds.GetMetrics()
			metricsDSInputChanLen.Set(float64(dsMetrics.EventChanSize))
			metricsDSPendingQueueLen.Set(float64(dsMetrics.PendingQueueLen))
			metricsDSUsedMemoryUsage.Set(float64(dsMetrics.MemoryControl.UsedMemory))
			metricsDSMaxMemoryUsage.Set(float64(dsMetrics.MemoryControl.MaxMemory))
		}
	}
}

// dispatcherStat is a helper struct to manage the state of a dispatcher.
type dispatcherStat struct {
	dispatcherID common.DispatcherID
	target       dispatcher.EventDispatcher

	eventServiceInfo struct {
		sync.RWMutex
		// the server this dispatcher is currently connected to(except local event service)
		// if it is set to local event service id, ignore all messages from other event service
		serverID node.ID
		// whether has received ready signal from `serverID`
		readyEventReceived bool
		// the remote event services which may contain data this dispatcher needed
		remoteCandidates []node.ID
	}

	// lastEventSeq is the sequence number of the last received DML/DDL event.
	// It is used to ensure the order of events.
	lastEventSeq atomic.Uint64

	// waitHandshake is used to indicate whether the dispatcher is waiting for a handshake event.
	// Dispatcher will drop all data events before receiving a handshake event.
	waitHandshake atomic.Bool

	// The largest commit ts that has been sent to the dispatcher.
	sentCommitTs atomic.Uint64
}

func (d *dispatcherStat) reset() {
	if d.waitHandshake.Load() {
		return
	}
	d.lastEventSeq.Store(0)
	d.waitHandshake.Store(true)
}

func (d *dispatcherStat) checkEventSeq(event dispatcher.DispatcherEvent, eventCollector *EventCollector) bool {
	switch event.GetType() {
	case commonEvent.TypeDMLEvent,
		commonEvent.TypeDDLEvent,
		commonEvent.TypeHandshakeEvent:
		expectedSeq := d.lastEventSeq.Add(1)
		if event.GetSeq() != expectedSeq {
			log.Warn("Received an out-of-order event, reset the dispatcher",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Uint64("receivedSeq", event.GetSeq()),
				zap.Uint64("expectedSeq", expectedSeq),
				zap.Uint64("commitTs", event.GetCommitTs()))
			eventCollector.resetDispatcher(d)
			return false
		}
	}
	return true
}

func (d *dispatcherStat) shouldIgnoreDataEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) bool {
	if d.eventServiceInfo.serverID != *event.From {
		// FIXME: unregister from this invalid event service if it send events for a long time
		return true
	}
	if d.waitHandshake.Load() {
		log.Warn("Receive event before handshake event, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()))
		return true
	}
	if !d.checkEventSeq(event, eventCollector) {
		return true
	}
	// Note: a commit ts may have multiple transactions.
	// it is ok to send the same txn multiple times?
	// (we just want to avoid send old dml after new ddl)
	if event.GetCommitTs() < d.sentCommitTs.Load() {
		log.Warn("Receive a event older than sendCommitTs, ignore it",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Int64("tableID", d.target.GetTableSpan().TableID),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Any("event", event.Event),
			zap.Uint64("eventCommitTs", event.GetCommitTs()),
			zap.Uint64("sentCommitTs", d.sentCommitTs.Load()))
		return true
	}
	d.sentCommitTs.Store(event.GetCommitTs())
	return false
}

func (d *dispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeHandshakeEvent {
		log.Panic("should not happen")
	}
	if d.eventServiceInfo.serverID == "" {
		log.Panic("should not happen: server ID is not set")
	}
	if d.eventServiceInfo.serverID != *event.From {
		// check invariant: if the handshake event is not from the current event service, we must be reading from local event service.
		if d.eventServiceInfo.serverID != eventCollector.serverId {
			log.Panic("receive handshake event from remote event service, but current event service is not local event service",
				zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
				zap.Stringer("dispatcher", d.target.GetId()),
				zap.Stringer("from", event.From))
		}
		return
	}
	if !d.checkEventSeq(event, eventCollector) {
		return
	}
	d.waitHandshake.Store(false)
	d.target.SetInitialTableInfo(event.Event.(*commonEvent.HandshakeEvent).TableInfo)
}

func (d *dispatcherStat) handleReadyEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()

	if event.GetType() != commonEvent.TypeReadyEvent {
		log.Panic("should not happen")
	}
	server := *event.From
	if d.eventServiceInfo.serverID == server {
		// case 1: already received ready signal from the same server
		if d.eventServiceInfo.readyEventReceived {
			return
		}
		// case 2: first ready signal from the server
		// (must be a remote candidate, because we won't set d.eventServiceInfo.serverID to local event service until we receive ready signal)
		d.eventServiceInfo.serverID = server
		d.eventServiceInfo.readyEventReceived = true
		eventCollector.addDispatcherRequestToSendingQueue(
			server,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sentCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else if server == eventCollector.serverId {
		// case 3: received first ready signal from local event service
		if d.eventServiceInfo.serverID != "" {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.serverID,
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
				},
			)
		}
		d.eventServiceInfo.serverID = server
		d.eventServiceInfo.readyEventReceived = true
		d.eventServiceInfo.remoteCandidates = nil
		eventCollector.addDispatcherRequestToSendingQueue(
			server,
			eventServiceTopic,
			DispatcherRequest{
				Dispatcher: d.target,
				StartTs:    d.sentCommitTs.Load(),
				ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
			},
		)
	} else {
		log.Panic("should not happen: we have received ready signal from other remote server",
			zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
			zap.Stringer("dispatcher", d.target.GetId()),
			zap.Stringer("newRemote", server),
			zap.Stringer("oldRemote", d.eventServiceInfo.serverID))
	}
}

func (d *dispatcherStat) handleNotReusableEvent(event dispatcher.DispatcherEvent, eventCollector *EventCollector) {
	d.eventServiceInfo.Lock()
	defer d.eventServiceInfo.Unlock()
	if event.GetType() != commonEvent.TypeNotReusableEvent {
		log.Panic("should not happen")
	}
	if *event.From == d.eventServiceInfo.serverID {
		if len(d.eventServiceInfo.remoteCandidates) > 0 {
			eventCollector.addDispatcherRequestToSendingQueue(
				d.eventServiceInfo.remoteCandidates[0],
				eventServiceTopic,
				DispatcherRequest{
					Dispatcher: d.target,
					StartTs:    d.target.GetStartTs(),
					ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
					OnlyUse:    true,
				},
			)
			d.eventServiceInfo.serverID = d.eventServiceInfo.remoteCandidates[0]
			d.eventServiceInfo.remoteCandidates = d.eventServiceInfo.remoteCandidates[1:]
		}
	}
}

func (d *dispatcherStat) unregisterDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// must unregister from local event service
	eventCollector.mustSendDispatcherRequest(eventCollector.serverId, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
	})
	// unregister from remote event service if have
	if d.eventServiceInfo.serverID != "" && d.eventServiceInfo.serverID != eventCollector.serverId {
		eventCollector.mustSendDispatcherRequest(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
			Dispatcher: d.target,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
		})
	}
}

func (d *dispatcherStat) pauseChangefeed(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		// Just ignore the request if the dispatcher is not ready.
		return
	}

	log.Info("Send pause changefeed event to event service",
		zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
		zap.String("dispatcher", d.target.GetId().String()),
		zap.Uint64("resolvedTs", d.target.GetResolvedTs()))

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE_CHANGEFEED,
	})
}

func (d *dispatcherStat) resumeChangefeed(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		// Just ignore the request if the dispatcher is not ready.
		return
	}
	log.Info("Send resume changefeed event to event service",
		zap.String("changefeedID", d.target.GetChangefeedID().ID().String()),
		zap.Stringer("dispatcher", d.target.GetId()),
		zap.Uint64("resolvedTs", d.target.GetResolvedTs()))

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_RESUME_CHANGEFEED,
	})
}

func (d *dispatcherStat) pauseDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		// Just ignore the request if the dispatcher is not ready.
		return
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_PAUSE,
	})
}

func (d *dispatcherStat) resumeDispatcher(eventCollector *EventCollector) {
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()

	if d.eventServiceInfo.serverID == "" || !d.eventServiceInfo.readyEventReceived {
		// Just ignore the request if the dispatcher is not ready.
		return
	}

	eventCollector.addDispatcherRequestToSendingQueue(d.eventServiceInfo.serverID, eventServiceTopic, DispatcherRequest{
		Dispatcher: d.target,
		ActionType: eventpb.ActionType_ACTION_TYPE_RESUME,
	})
}

// TODO: better name
func (d *dispatcherStat) setRemoteCandidates(nodes []string, eventCollector *EventCollector) {
	if len(nodes) == 0 {
		return
	}
	d.eventServiceInfo.RLock()
	defer d.eventServiceInfo.RUnlock()
	// reading from a event service or checking remotes already, ignore
	if d.eventServiceInfo.serverID != "" {
		return
	}
	d.eventServiceInfo.serverID = node.ID(nodes[0])
	for i := 1; i < len(nodes); i++ {
		d.eventServiceInfo.remoteCandidates = append(d.eventServiceInfo.remoteCandidates, node.ID(nodes[i]))
	}

	eventCollector.addDispatcherRequestToSendingQueue(
		d.eventServiceInfo.serverID,
		eventServiceTopic,
		DispatcherRequest{
			Dispatcher: d.target,
			StartTs:    d.target.GetStartTs(),
			ActionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
			OnlyUse:    true,
		},
	)
}
