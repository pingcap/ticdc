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
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/chann"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	receiveChanSize = 1024 * 8
	retryLimit      = 3 // The maximum number of retries for sending dispatcher requests and heartbeats.
)

var (
	metricsHandleEventDuration = metrics.EventCollectorHandleEventDuration
	metricsDSInputChanLen      = metrics.DynamicStreamEventChanSize.WithLabelValues("event-collector")
	metricsDSPendingQueueLen   = metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-collector")
)

// DispatcherRequest is the request send to EventService.
type DispatcherRequest struct {
	Message    *messaging.TargetMessage
	RetryCount int
}

func (d *DispatcherRequest) incrAndCheckRetry() bool {
	d.RetryCount++
	return d.RetryCount < retryLimit
}

// type DispatcherHeartbeatWithTarget struct {
// 	Target       node.ID
// 	Topic        string
// 	Heartbeat    *event.DispatcherHeartbeat
// 	retryCounter int
// }

// // shouldRetry returns true if the retry counter is less than 3.
// // We set the limit to avoid retry sending heartbeat endlessly.
// func (d *DispatcherHeartbeatWithTarget) shouldRetry() bool {
// 	return d.retryCounter < 3
// }

// func (d *DispatcherHeartbeatWithTarget) incRetryCounter() {
// 	d.retryCounter++
// }

const (
	eventServiceTopic   = messaging.EventServiceTopic
	eventCollectorTopic = messaging.EventCollectorTopic
	logCoordinatorTopic = messaging.LogCoordinatorTopic
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

	// dispatcherRequestChan buffers requests to the EventService.
	// It automatically retries failed requests up to a configured maximum retry limit.
	dispatcherRequestChan *chann.DrainableChann[DispatcherRequest]

	// dispatcherHeartbeatChan is used to send the dispatcher heartbeat to the event service.
	// dispatcherHeartbeatChan *chann.DrainableChann[*DispatcherHeartbeatWithTarget]

	// TODO: move log coordinator interaction logic to a separate component.
	logCoordinatorRequestChan *chann.DrainableChann[*logservicepb.ReusableEventServiceRequest]

	receiveChannels []chan *messaging.TargetMessage
	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]

	coordinatorInfo struct {
		value atomic.Value
	}

	wg     sync.WaitGroup
	cancel context.CancelFunc

	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer
}

func New(serverId node.ID) *EventCollector {
	receiveChannels := make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency)
	for i := 0; i < config.DefaultBasicEventHandlerConcurrency; i++ {
		receiveChannels[i] = make(chan *messaging.TargetMessage, receiveChanSize)
	}
	eventCollector := &EventCollector{
		serverId:                             serverId,
		dispatcherMap:                        sync.Map{},
		dispatcherRequestChan:                chann.NewAutoDrainChann[DispatcherRequest](),
		logCoordinatorRequestChan:            chann.NewAutoDrainChann[*logservicepb.ReusableEventServiceRequest](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		receiveChannels:                      receiveChannels,
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
	}
	eventCollector.ds = NewEventDynamicStream(eventCollector)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.RecvEventsMessage)

	return eventCollector
}

func (c *EventCollector) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	for i := 0; i < config.DefaultBasicEventHandlerConcurrency; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runProcessMessage(ctx, c.receiveChannels[i])
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
	c.wg.Wait()
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

	log.Info("event collector is closed")
}

func (c *EventCollector) AddDispatcher(target dispatcher.EventDispatcher, memoryQuota uint64) {
	c.PrepareAddDispatcher(target, memoryQuota, nil)
	if target.GetTableSpan().TableID != 0 {
		c.logCoordinatorRequestChan.In() <- &logservicepb.ReusableEventServiceRequest{
			ID:      target.GetId().ToPB(),
			Span:    target.GetTableSpan(),
			StartTs: target.GetStartTs(),
		}
	}
}

// PrepareAddDispatcher is used to prepare the dispatcher to be added to the event collector.
// It will send a register request to local event service and call `readyCallback` when local event service is ready.
func (c *EventCollector) PrepareAddDispatcher(
	target dispatcher.EventDispatcher,
	memoryQuota uint64,
	readyCallback func(),
) {
	log.Info("add dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("add dispatcher done", zap.Stringer("dispatcher", target.GetId()))
	}()
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	stat := newDispatcherStat(target, c, readyCallback)
	c.dispatcherMap.Store(target.GetId(), stat)
	c.changefeedIDMap.Store(target.GetChangefeedID().ID(), target.GetChangefeedID())

	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota, dynstream.MemoryControlAlgorithmV2, "eventCollector")
	err := c.ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Warn("add dispatcher to dynamic stream failed", zap.Error(err))
	}
	stat.run()
}

// CommitAddDispatcher notify local event service that the dispatcher is ready to receive events.
func (c *EventCollector) CommitAddDispatcher(target dispatcher.EventDispatcher, startTs uint64) {
	log.Info("commit add dispatcher", zap.Stringer("dispatcher", target.GetId()), zap.Uint64("startTs", startTs))
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		log.Warn("dispatcher not found when commit add dispatcher",
			zap.Stringer("dispatcher", target.GetId()),
			zap.Uint64("startTs", startTs))
		return
	}
	stat := value.(*dispatcherStat)
	stat.reset()
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
	stat.remove()

	err := c.ds.RemovePath(target.GetId())
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
	c.dispatcherMap.Delete(target.GetId())
}

// Queues a message for sending (best-effort, no delivery guarantee)
// Messages may be dropped if errors occur. For reliable delivery, implement retry/ack logic at caller side
func (c *EventCollector) enqueueMessageForSend(msg *messaging.TargetMessage) {
	if msg != nil {
		c.dispatcherRequestChan.In() <- DispatcherRequest{
			Message:    msg,
			RetryCount: 0,
		}
	}
}

func (c *EventCollector) wakeDispatcher(dispatcherID common.DispatcherID) {
	c.ds.Wake(dispatcherID)
}

func (c *EventCollector) getLocalServerID() node.ID {
	return c.serverId
}

func (c *EventCollector) SendDispatcherHeartbeat(heartbeat *event.DispatcherHeartbeat) {
	groupedHeartbeats := c.groupHeartbeat(heartbeat)
	for serverID, heartbeat := range groupedHeartbeats {
		msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, heartbeat)
		c.enqueueMessageForSend(msg)
	}
}

// TODO(dongmen): add unit test for this function.
// groupHeartbeat groups the heartbeat by the dispatcherStat's serverID.
func (c *EventCollector) groupHeartbeat(heartbeat *event.DispatcherHeartbeat) map[node.ID]*event.DispatcherHeartbeat {
	groupedHeartbeats := make(map[node.ID]*event.DispatcherHeartbeat)
	group := func(target node.ID, dp event.DispatcherProgress) {
		heartbeat, ok := groupedHeartbeats[target]
		if !ok {
			heartbeat = &event.DispatcherHeartbeat{
				Version:              event.DispatcherHeartbeatVersion,
				DispatcherProgresses: make([]event.DispatcherProgress, 0, 32),
			}
			groupedHeartbeats[target] = heartbeat
		}
		heartbeat.Append(dp)
	}

	for _, dp := range heartbeat.DispatcherProgresses {
		stat, ok := c.dispatcherMap.Load(dp.DispatcherID)
		if !ok {
			continue
		}
		group(stat.(*dispatcherStat).connState.getEventServiceID(), dp)
	}

	return groupedHeartbeats
}

// resetDispatcher is used to reset the dispatcher when it receives a out-of-order event.
// It will send a reset request to the event service to reset the remote dispatcher.
// And it will reset the dispatcher stat to wait for a new handshake event.
// func (c *EventCollector) resetDispatcher(d *dispatcherStat) {
// 	c.addDispatcherRequestToSendingQueue(
// 		d.eventServiceInfo.serverID,
// 		eventServiceTopic,
// 		DispatcherRequest{
// 			Dispatcher: d.target,
// 			StartTs:    d.sentCommitTs.Load(),
// 			ActionType: eventpb.ActionType_ACTION_TYPE_RESET,
// 		})
// 	d.reset()
// 	log.Info("Send reset dispatcher request to event service",
// 		zap.Stringer("dispatcher", d.target.GetId()),
// 		zap.Uint64("startTs", d.sentCommitTs.Load()))
// }

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
				feedback.Dest.pause()
			case dynstream.ResumePath:
				feedback.Dest.resume()
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
			err := c.mc.SendCommand(req.Message)
			if err != nil {
				log.Info("failed to send dispatcher request message, try again later",
					zap.String("message", req.Message.String()),
					zap.Error(err))
				if !req.incrAndCheckRetry() {
					log.Warn("dispatcher request retry limit exceeded, dropping request",
						zap.String("message", req.Message.String()))
					continue
				}
				// Put the request back to the channel for later retry.
				c.dispatcherRequestChan.In() <- req
				// Sleep a short time to avoid too many requests in a short time.
				// TODO: requests can to different EventService, so we should improve the logic here.
				time.Sleep(10 * time.Millisecond)
			}
			// case heartbeat := <-c.dispatcherHeartbeatChan.Out():
			// 	if err := c.sendDispatcherHeartbeat(heartbeat); err != nil {
			// 		// Sleep a short time to avoid too many requests in a short time.
			// 		time.Sleep(10 * time.Millisecond)
			// 	}
		}
	}
}

func (c *EventCollector) setCoordinatorInfo(id node.ID) {
	c.coordinatorInfo.value.Store(id)
}

func (c *EventCollector) getCoordinatorInfo() node.ID {
	if v := c.coordinatorInfo.value.Load(); v != nil {
		return v.(node.ID)
	}
	return ""
}

func (c *EventCollector) processLogCoordinatorRequest(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-c.logCoordinatorRequestChan.Out():
			coordinatorID := c.getCoordinatorInfo()
			if coordinatorID == "" {
				log.Info("coordinator info is empty, try send request later")
				c.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
				continue
			}
			targetMessage := messaging.NewSingleTargetMessage(coordinatorID, logCoordinatorTopic, req)
			err := c.mc.SendCommand(targetMessage)
			if err != nil {
				log.Info("fail to send dispatcher request message to log coordinator, try again later", zap.Error(err))
				c.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// // mustSendDispatcherRequest will keep retrying to send the dispatcher request to EventService until it succeed.
// // Caller should avoid to use this method when the remote EventService is offline forever.
// // And this method may be deprecated in the future.
// // FIXME: Add a checking mechanism to avoid sending request to offline EventService.
// // A simple way is to use a NodeManager to check if the target is online.
// func (c *EventCollector) mustSendDispatcherRequest(target node.ID, topic string, req DispatcherRequest) error {
// 	message := &messaging.DispatcherRequest{
// 		DispatcherRequest: &eventpb.DispatcherRequest{
// 			ChangefeedId: req.Dispatcher.GetChangefeedID().ToPB(),
// 			DispatcherId: req.Dispatcher.GetId().ToPB(),
// 			ActionType:   req.ActionType,
// 			// ServerId is the id of the request sender.
// 			ServerId:  c.serverId.String(),
// 			TableSpan: req.Dispatcher.GetTableSpan(),
// 			StartTs:   req.StartTs,
// 			OnlyReuse: req.OnlyReuse,
// 			BdrMode:   req.BDRMode,
// 		},
// 	}

// 	// If the action type is register and reset, we need fill all config related fields.
// 	if req.ActionType == eventpb.ActionType_ACTION_TYPE_REGISTER ||
// 		req.ActionType == eventpb.ActionType_ACTION_TYPE_RESET {
// 		message.DispatcherRequest.FilterConfig = req.Dispatcher.GetFilterConfig()
// 		message.DispatcherRequest.EnableSyncPoint = req.Dispatcher.EnableSyncPoint()
// 		message.DispatcherRequest.SyncPointInterval = uint64(req.Dispatcher.GetSyncPointInterval().Seconds())
// 		message.DispatcherRequest.SyncPointTs = syncpoint.CalculateStartSyncPointTs(req.StartTs, req.Dispatcher.GetSyncPointInterval(), req.Dispatcher.GetStartTsIsSyncpoint())
// 	}

// 	err := c.mc.SendCommand(messaging.NewSingleTargetMessage(target, eventServiceTopic, message))
// 	if err != nil {
// 		log.Info("failed to send dispatcher request message to event service, try again later",
// 			zap.String("changefeedID", req.Dispatcher.GetChangefeedID().ID().String()),
// 			zap.Stringer("dispatcher", req.Dispatcher.GetId()),
// 			zap.Any("target", target.String()),
// 			zap.Any("request", req),
// 			zap.Error(err))
// 		// Put the request back to the channel for later retry.
// 		c.dispatcherRequestChan.In() <- DispatcherRequestWithTarget{
// 			Target: target,
// 			Topic:  topic,
// 			Req:    req,
// 		}
// 		return err
// 	}
// 	return nil
// }

// sendDispatcherHeartbeat sends the dispatcher heartbeat to the event service.
// It will retry to send the heartbeat to the event service until it exceeds the retry limit.
// func (c *EventCollector) sendDispatcherHeartbeat(heartbeat *DispatcherHeartbeatWithTarget) error {
// 	message := messaging.NewSingleTargetMessage(heartbeat.Target, heartbeat.Topic, heartbeat.Heartbeat)
// 	err := c.mc.SendCommand(message)
// 	if err != nil {
// 		if heartbeat.shouldRetry() {
// 			heartbeat.incRetryCounter()
// 			log.Info("failed to send dispatcher heartbeat message to event service, try again later", zap.Error(err), zap.Stringer("target", heartbeat.Target))
// 			c.dispatcherHeartbeatChan.In() <- heartbeat
// 		}
// 		return err
// 	}
// 	return nil
// }

func (c *EventCollector) handleDispatcherHeartbeatResponse(targetMessage *messaging.TargetMessage) {
	if len(targetMessage.Message) != 1 {
		log.Panic("invalid dispatcher heartbeat response message", zap.Any("msg", targetMessage))
	}

	response := targetMessage.Message[0].(*event.DispatcherHeartbeatResponse)
	for _, ds := range response.DispatcherStates {
		// This means that the dispatcher is removed in the event service we have to reset it.
		if ds.State == event.DSStateRemoved {
			v, ok := c.dispatcherMap.Load(ds.DispatcherID)
			if !ok {
				continue
			}
			stat := v.(*dispatcherStat)
			// If the serverID not match, it means the dispatcher is not registered on this server now, just ignore it the response.
			if stat.connState.isCurrentEventService(targetMessage.From) {
				// register the dispatcher again
				stat.reset()
			}
		}
	}
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
			c.setCoordinatorInfo(targetMessage.From)
		case *logservicepb.ReusableEventServiceResponse:
			// TODO: can we handle it here?
			resp := msg.(*logservicepb.ReusableEventServiceResponse)
			dispatcherID := common.NewDispatcherIDFromPB(resp.ID)
			value, ok := c.dispatcherMap.Load(dispatcherID)
			if !ok {
				continue
			}
			value.(*dispatcherStat).setRemoteCandidates(msg.(*logservicepb.ReusableEventServiceResponse).Nodes)
		case *event.DispatcherHeartbeatResponse:
			c.handleDispatcherHeartbeatResponse(targetMessage)
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
				switch e := msg.(type) {
				case event.Event:
					switch e.GetType() {
					case event.TypeBatchResolvedEvent:
						events := e.(*event.BatchResolvedEvent).Events
						from := &targetMessage.From
						resolvedTsCount := int32(0)
						for _, resolvedEvent := range events {
							c.ds.Push(resolvedEvent.DispatcherID, dispatcher.NewDispatcherEvent(from, resolvedEvent))
							resolvedTsCount += resolvedEvent.Len()
						}
						c.metricDispatcherReceivedResolvedTsEventCount.Add(float64(resolvedTsCount))
					case event.TypeBatchDMLEvent:
						stat, ok := c.dispatcherMap.Load(e.GetDispatcherID())
						if !ok {
							continue
						}
						tableInfo, ok := stat.(*dispatcherStat).tableInfo.Load().(*common.TableInfo)
						if !ok {
							continue
						}
						events := e.(*event.BatchDMLEvent)
						events.AssembleRows(tableInfo)
						from := &targetMessage.From
						for _, dml := range events.DMLEvents {
							c.ds.Push(dml.DispatcherID, dispatcher.NewDispatcherEvent(from, dml))
						}
						c.metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
					case event.TypeDDLEvent:
						stat, ok := c.dispatcherMap.Load(e.GetDispatcherID())
						if !ok {
							continue
						}
						stat.(*dispatcherStat).setTableInfo(e.(*event.DDLEvent).TableInfo)
						c.metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						c.ds.Push(e.GetDispatcherID(), dispatcher.NewDispatcherEvent(&targetMessage.From, e))
					case event.TypeHandshakeEvent:
						stat, ok := c.dispatcherMap.Load(e.GetDispatcherID())
						if !ok {
							continue
						}
						log.Info("get handshake event",
							zap.Stringer("dispatcherID", e.GetDispatcherID()),
							zap.String("serverID", targetMessage.From.String()))
						stat.(*dispatcherStat).setTableInfo(e.(*event.HandshakeEvent).TableInfo)
						c.metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						c.ds.Push(e.GetDispatcherID(), dispatcher.NewDispatcherEvent(&targetMessage.From, e))
					default:
						c.metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						c.ds.Push(e.GetDispatcherID(), dispatcher.NewDispatcherEvent(&targetMessage.From, e))
					}
				default:
					log.Panic("invalid message type", zap.Any("msg", msg))
				}
			}
		}
	}
}

func (c *EventCollector) updateMetrics(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
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
