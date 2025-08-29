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
	"github.com/pingcap/ticdc/pkg/apperror"
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
	"golang.org/x/sync/errgroup"
)

const (
	receiveChanSize     = 1024 * 8
	commonMsgRetryQuota = 3 // The number of retries for most droppable dispatcher requests.
)

// DispatcherMessage is the message send to EventService.
type DispatcherMessage struct {
	Message *messaging.TargetMessage
	// Droppable indicates whether the message can be dropped after repeated delivery failures.
	//
	// This is based on the assumption that:
	// - Most dispatcher requests target local event services (safe to retry indefinitely)
	// - Remote requests can be dropped (system can progress without them, may cause temporary delays)
	//
	// Why not retry all messages indefinitely?
	// Permanently unavailable remote targets would cause messages
	// to accumulate in the queue permanently.
	//
	// TODO: Implement application-level retry logic for better architectural flexibility.
	Droppable  bool
	RetryQuota int
}

func newDispatcherMessage(msg *messaging.TargetMessage, droppable bool, retryQuota int) DispatcherMessage {
	return DispatcherMessage{
		Message:    msg,
		Droppable:  droppable,
		RetryQuota: retryQuota,
	}
}

func (d *DispatcherMessage) decrAndCheckRetry() bool {
	if !d.Droppable {
		return true
	}
	d.RetryQuota--
	return d.RetryQuota > 0
}

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

	logCoordinatorClient *LogCoordinatorClient

	// dispatcherMessageChan buffers requests to the EventService.
	// It automatically retries failed requests up to a configured maximum retry limit.
	dispatcherMessageChan *chann.DrainableChann[DispatcherMessage]

	receiveChannels     []chan *messaging.TargetMessage
	redoReceiveChannels []chan *messaging.TargetMessage
	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]
	// redoDs is the dynamicStream for redo dispatcher events.
	redoDs dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]

	g      *errgroup.Group
	cancel context.CancelFunc

	metricDispatcherReceivedKVEventCount         prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricReceiveEventLagDuration                prometheus.Observer

	metricRedoDispatcherReceivedKVEventCount         prometheus.Counter
	metricRedoDispatcherReceivedResolvedTsEventCount prometheus.Counter
}

func New(serverId node.ID) *EventCollector {
	receiveChannels := make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency)
	redoReceiveChannels := make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency)
	for i := 0; i < config.DefaultBasicEventHandlerConcurrency; i++ {
		receiveChannels[i] = make(chan *messaging.TargetMessage, receiveChanSize)
		redoReceiveChannels[i] = make(chan *messaging.TargetMessage, receiveChanSize)
	}
	eventCollector := &EventCollector{
		serverId:                             serverId,
		dispatcherMap:                        sync.Map{},
		dispatcherMessageChan:                chann.NewAutoDrainChann[DispatcherMessage](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		receiveChannels:                      receiveChannels,
		redoReceiveChannels:                  redoReceiveChannels,
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent", "eventDispatcher"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs", "eventDispatcher"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),

		metricRedoDispatcherReceivedKVEventCount:         metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent", "redoDispatcher"),
		metricRedoDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs", "redoDispatcher"),
	}
	eventCollector.logCoordinatorClient = newLogCoordinatorClient(eventCollector)
	eventCollector.ds = NewEventDynamicStream(eventCollector)
	eventCollector.redoDs = NewEventDynamicStream(eventCollector)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.MessageCenterHandler)
	eventCollector.mc.RegisterHandler(messaging.RedoEventCollectorTopic, eventCollector.RedoMessageCenterHandler)

	return eventCollector
}

func (c *EventCollector) Run(ctx context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	c.g = g
	c.cancel = cancel

	for _, ch := range c.receiveChannels {
		g.Go(func() error {
			return c.runDispatchMessage(ctx, ch, false)
		})
	}

	for _, ch := range c.redoReceiveChannels {
		g.Go(func() error {
			return c.runDispatchMessage(ctx, ch, true)
		})
	}

	g.Go(func() error {
		return c.logCoordinatorClient.run(ctx)
	})

	g.Go(func() error {
		return c.processDSFeedback(ctx)
	})

	g.Go(func() error {
		return c.controlCongestion(ctx)
	})

	g.Go(func() error {
		return c.sendDispatcherRequests(ctx)
	})

	g.Go(func() error {
		return c.updateMetrics(ctx)
	})

	log.Info("event collector is running")
}

func (c *EventCollector) Close() {
	log.Info("event collector is closing")
	c.cancel()
	_ = c.g.Wait()
	c.redoDs.Close()
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

func (c *EventCollector) AddDispatcher(target dispatcher.DispatcherService, memoryQuota uint64) {
	c.PrepareAddDispatcher(target, memoryQuota, nil)
	c.logCoordinatorClient.requestReusableEventService(target)
}

// PrepareAddDispatcher is used to prepare the dispatcher to be added to the event collector.
// It will send a register request to local event service and call `readyCallback` when local event service is ready.
func (c *EventCollector) PrepareAddDispatcher(
	target dispatcher.DispatcherService,
	memoryQuota uint64,
	readyCallback func(),
) {
	log.Info("add dispatcher", zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("add dispatcher done",
			zap.Stringer("dispatcherID", target.GetId()), zap.Int64("tableID", target.GetTableSpan().GetTableID()),
			zap.Uint64("startTs", target.GetStartTs()), zap.Int("type", target.GetType()))
	}()
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	stat := newDispatcherStat(target, c, readyCallback, memoryQuota)
	c.dispatcherMap.Store(target.GetId(), stat)
	c.changefeedIDMap.Store(target.GetChangefeedID().ID(), target.GetChangefeedID())

	ds := c.getDynamicStream(dispatcher.IsRedoDispatcher(target))
	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota, dynstream.MemoryControlForEventCollector, "eventCollector")
	err := ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Warn("add dispatcher to dynamic stream failed", zap.Error(err))
	}
	stat.run()
}

// CommitAddDispatcher notify local event service that the dispatcher is ready to receive events.
func (c *EventCollector) CommitAddDispatcher(target dispatcher.Dispatcher, startTs uint64) {
	log.Info("commit add dispatcher", zap.Stringer("dispatcherID", target.GetId()),
		zap.Int64("tableID", target.GetTableSpan().GetTableID()), zap.Uint64("startTs", startTs))
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		log.Warn("dispatcher not found when commit add dispatcher",
			zap.Stringer("dispatcherID", target.GetId()), zap.Int64("tableID", target.GetTableSpan().GetTableID()),
			zap.Uint64("startTs", startTs))
		return
	}
	stat := value.(*dispatcherStat)
	stat.commitReady(c.getLocalServerID())
}

func (c *EventCollector) RemoveDispatcher(target dispatcher.Dispatcher) {
	log.Info("remove dispatcher", zap.Stringer("dispatcherID", target.GetId()))
	defer func() {
		log.Info("remove dispatcher done", zap.Stringer("dispatcherID", target.GetId()),
			zap.Int64("tableID", target.GetTableSpan().GetTableID()))
	}()
	isRedo := dispatcher.IsRedoDispatcher(target)
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		return
	}
	stat := value.(*dispatcherStat)
	stat.remove()

	ds := c.getDynamicStream(isRedo)
	err := ds.RemovePath(target.GetId())
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
	c.dispatcherMap.Delete(target.GetId())
}

// isRepeatedMsgType returns true when the message is heartbeat like message.
// this kind of message can be dropped quickly when send failure.
func isRepeatedMsgType(msg *messaging.TargetMessage) bool {
	// only handle len(msg.Message) == 1 for simplicity
	if len(msg.Message) != 1 {
		return false
	}
	switch msg.Message[0].(type) {
	case *event.DispatcherHeartbeat:
		return true
	default:
		return false
	}
}

// Queues a message for sending (best-effort, no delivery guarantee)
// Messages may be dropped if errors occur. For reliable delivery, implement retry/ack logic at caller side
func (c *EventCollector) enqueueMessageForSend(msg *messaging.TargetMessage) {
	if msg != nil {
		if isRepeatedMsgType(msg) {
			c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, true, 1)
		} else {
			if msg.To == c.serverId {
				c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, false, 0)
			} else {
				c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, true, commonMsgRetryQuota)
			}
		}
	}
}

func (c *EventCollector) getLocalServerID() node.ID {
	return c.serverId
}

func (c *EventCollector) getDispatcherStatByID(dispatcherID common.DispatcherID) *dispatcherStat {
	value, ok := c.dispatcherMap.Load(dispatcherID)
	if !ok {
		return nil
	}
	return value.(*dispatcherStat)
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
		if stat.(*dispatcherStat).connState.isReceivingDataEvent() {
			group(stat.(*dispatcherStat).connState.getEventServiceID(), dp)
		}
	}

	return groupedHeartbeats
}

func (c *EventCollector) processDSFeedback(ctx context.Context) error {
	log.Info("Start process feedback from dynamic stream")
	defer log.Info("Stop process feedback from dynamic stream")
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case feedback := <-c.ds.Feedback():
			switch feedback.FeedbackType {
			case dynstream.PauseArea, dynstream.ResumeArea:
				// Ignore it, because it is no need to pause and resume an area in event collector.
			case dynstream.PausePath:
				feedback.Dest.pause()
			case dynstream.ResumePath:
				feedback.Dest.resume()
			}
		case feedback := <-c.redoDs.Feedback():
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

func (c *EventCollector) sendDispatcherRequests(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case req := <-c.dispatcherMessageChan.Out():
			err := c.mc.SendCommand(req.Message)
			if err != nil {
				sleepInterval := 10 * time.Millisecond
				// if the error is Congested, sleep a larger interval
				if appErr, ok := err.(apperror.AppError); ok && appErr.Type == apperror.ErrorTypeMessageCongested {
					sleepInterval = 1 * time.Second
				}
				log.Info("failed to send dispatcher request message, try again later",
					zap.String("message", req.Message.String()),
					zap.Duration("sleepInterval", sleepInterval),
					zap.Error(err))
				if !req.decrAndCheckRetry() {
					log.Warn("dispatcher request retry limit exceeded, dropping request",
						zap.String("message", req.Message.String()))
					continue
				}
				// Put the request back to the channel for later retry.
				c.dispatcherMessageChan.In() <- req
				// Sleep a short time to avoid too many requests in a short time.
				// TODO: requests can to different EventService, so we should improve the logic here.
				time.Sleep(sleepInterval)
			}
		}
	}
}

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
				stat.reset(targetMessage.From)
			}
		}
	}
}

// MessageCenterHandler is the handler for the events message from EventService.
func (c *EventCollector) MessageCenterHandler(_ context.Context, targetMessage *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.UnixMilli(targetMessage.CreateAt)).Seconds()
	c.metricReceiveEventLagDuration.Observe(inflightDuration)

	start := time.Now()
	defer func() {
		metrics.EventCollectorHandleEventDuration.Observe(time.Since(start).Seconds())
	}()

	// If the message is a log service event, we need to forward it to the
	// corresponding channel to handle it in multi-thread.
	if targetMessage.Type.IsLogServiceEvent() {
		c.receiveChannels[targetMessage.GetGroup()%uint64(len(c.receiveChannels))] <- targetMessage
		return nil
	}

	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *event.DispatcherHeartbeatResponse:
			c.handleDispatcherHeartbeatResponse(targetMessage)
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

// RedoMessageCenterHandler is the handler for the redo events message from EventService.
func (c *EventCollector) RedoMessageCenterHandler(_ context.Context, targetMessage *messaging.TargetMessage) error {
	// If the message is a log service event, we need to forward it to the
	// corresponding channel to handle it in multi-thread.
	if targetMessage.Type.IsLogServiceEvent() {
		c.redoReceiveChannels[targetMessage.GetGroup()%uint64(len(c.redoReceiveChannels))] <- targetMessage
		return nil
	}
	log.Panic("invalid message type", zap.Any("msg", targetMessage))
	return nil
}

// runDispatchMessage dispatches messages from the input channel to the dynamic stream.
// Note: Avoid implementing any message handling logic within this function
// as messages may be stale and need be verified before process.
func (c *EventCollector) runDispatchMessage(ctx context.Context, inCh <-chan *messaging.TargetMessage, isRedo bool) error {
	ds := c.getDynamicStream(isRedo)
	metricDispatcherReceivedKVEventCount, metricDispatcherReceivedResolvedTsEventCount := c.getMetric(isRedo)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
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
							ds.Push(resolvedEvent.DispatcherID, dispatcher.NewDispatcherEvent(from, resolvedEvent))
							resolvedTsCount += resolvedEvent.Len()
						}
						metricDispatcherReceivedResolvedTsEventCount.Add(float64(resolvedTsCount))
					default:
						metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						dispatcherEvent := dispatcher.NewDispatcherEvent(&targetMessage.From, e)
						ds.Push(e.GetDispatcherID(), dispatcherEvent)
					}
				default:
					log.Panic("invalid message type", zap.Any("msg", msg))
				}
			}
		}
	}
}

func (c *EventCollector) controlCongestion(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			messages := c.newCongestionControlMessages()
			for serverID, m := range messages {
				if len(m.GetAvailables()) != 0 {
					msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, m)
					if err := c.mc.SendCommand(msg); err != nil {
						log.Warn("send congestion control message failed", zap.Error(err))
					}
				}
			}
		}
	}
}

func (c *EventCollector) newCongestionControlMessages() map[node.ID]*event.CongestionControl {
	// collect all changefeeds' available memory quota
	availables := make(map[common.ChangeFeedID]uint64)
	for _, quota := range c.ds.GetMetrics().MemoryControl.AreaMemoryMetrics {
		changefeedID, ok := c.changefeedIDMap.Load(quota.Area())
		if !ok {
			continue
		}
		availables[changefeedID.(common.ChangeFeedID)] = uint64(quota.AvailableMemory())
	}
	if len(availables) == 0 {
		return nil
	}

	// calculate each changefeed's available memory quota for each node
	// by the proportion of the dispatcher on each node.
	// this is not accurate, we should also consider each node's workload distribution.
	proportions := make(map[common.ChangeFeedID]map[node.ID]uint64)
	c.dispatcherMap.Range(func(k, v interface{}) bool {
		stat := v.(*dispatcherStat)
		eventServiceID := stat.connState.getEventServiceID()
		if eventServiceID == "" {
			return true
		}

		changefeedID := stat.target.GetChangefeedID()
		holder, ok := proportions[changefeedID]
		if !ok {
			holder = make(map[node.ID]uint64)
			proportions[changefeedID] = holder
		}
		holder[eventServiceID]++
		return true
	})

	// group the available memory quota by nodeID
	result := make(map[node.ID]*event.CongestionControl)
	for changefeedID, total := range availables {
		proportion := proportions[changefeedID]
		var sum uint64
		for _, portion := range proportion {
			sum += portion
		}
		if sum == 0 {
			continue
		}

		for nodeID, portion := range proportion {
			ratio := float64(portion) / float64(sum)
			quota := uint64(float64(total) * ratio)

			m, ok := result[nodeID]
			if !ok {
				m = event.NewCongestionControl()
				result[nodeID] = m
			}
			m.AddAvailableMemory(changefeedID.ID(), quota)
		}
	}
	return result
}

func (c *EventCollector) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			// FIXME: record ds?
			dsMetrics := c.ds.GetMetrics()
			metrics.DynamicStreamEventChanSize.WithLabelValues("event-collector").Set(float64(dsMetrics.EventChanSize))
			metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-collector").Set(float64(dsMetrics.PendingQueueLen))
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

func (c *EventCollector) getDynamicStream(isRedo bool) dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler] {
	if isRedo {
		return c.redoDs
	}
	return c.ds
}

func (c *EventCollector) getMetric(isRedo bool) (prometheus.Counter, prometheus.Counter) {
	if isRedo {
		return c.metricRedoDispatcherReceivedKVEventCount, c.metricRedoDispatcherReceivedResolvedTsEventCount
	}
	return c.metricDispatcherReceivedKVEventCount, c.metricDispatcherReceivedResolvedTsEventCount
}
