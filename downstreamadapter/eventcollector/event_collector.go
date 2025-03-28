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
	serverId        node.ID
	dispatcherMap   sync.Map // key: dispatcherID, value: dispatcherStat
	changefeedIDMap sync.Map // key: changefeedID.GID, value: changefeedID

	mc messaging.MessageCenter

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
	log.Info("event collector is closing")
	c.cancel()
	c.ds.Close()
	c.changefeedIDMap.Range(func(key, value any) bool {
		cfID := value.(common.ChangeFeedID)
		// Remove metrics for the changefeed.
		metrics.DynamicStreamMemoryUsage.DeleteLabelValues(
			"event-collector",
			"max",
			cfID.String(),
		)
		metrics.DynamicStreamMemoryUsage.DeleteLabelValues(
			"event-collector",
			"used",
			cfID.String(),
		)
		return true
	})

	c.wg.Wait()
	log.Info("event collector is closed")
}

func (c *EventCollector) AddDispatcher(target dispatcher.EventDispatcher, memoryQuota uint64) {
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
	c.changefeedIDMap.Store(target.GetChangefeedID().ID(), target.GetChangefeedID())
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota, dynstream.MemoryControlAlgorithmV2)
	err := c.ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Warn("add dispatcher to dynamic stream failed", zap.Error(err))
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
			switch feedback.FeedbackType {
			case dynstream.PauseArea, dynstream.ResumeArea:
				// Ignore it, because it is no need to pause and resume an area in event collector.
			case dynstream.PausePath:
				feedback.Dest.pauseDispatcher(c)
			case dynstream.ResumePath:
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
		message.RegisterDispatcherRequest.SyncPointTs = syncpoint.CalculateStartSyncPointTs(req.StartTs, req.Dispatcher.GetSyncPointInterval(), req.Dispatcher.GetStartTsIsSyncpoint())
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
						c.metricDispatcherReceivedResolvedTsEventCount.Add(float64(event.Len()))
					default:
						c.metricDispatcherReceivedKVEventCount.Add(float64(event.Len()))
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
			for _, areaMetric := range dsMetrics.MemoryControl.AreaMemoryMetrics {
				cfID, ok := c.changefeedIDMap.Load(areaMetric.Area())
				if !ok {
					continue
				}
				changefeedID := cfID.(common.ChangeFeedID)
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"event-collector",
					"max",
					changefeedID.String(),
				).Set(float64(areaMetric.MaxMemory()))
				metrics.DynamicStreamMemoryUsage.WithLabelValues(
					"event-collector",
					"used",
					changefeedID.String(),
				).Set(float64(areaMetric.MemoryUsage()))
			}
		}
	}
}
