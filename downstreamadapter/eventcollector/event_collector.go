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
	receiveChanSize = 1024 * 8
	retryLimit      = 3 // The maximum number of retries for sending dispatcher requests and heartbeats.
)

// DispatcherMessage is the message send to EventService.
type DispatcherMessage struct {
	Message    *messaging.TargetMessage
	RetryCount int
}

func (d *DispatcherMessage) incrAndCheckRetry() bool {
	d.RetryCount++
	return d.RetryCount < retryLimit
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

	receiveChannels []chan *messaging.TargetMessage
	// ds is the dynamicStream for dispatcher events.
	// All the events from event service will be sent to ds to handle.
	// ds will dispatch the events to different dispatchers according to the dispatcherID.
	ds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]

	congestionController *congestionController

	g      *errgroup.Group
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
		dispatcherMessageChan:                chann.NewAutoDrainChann[DispatcherMessage](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		receiveChannels:                      receiveChannels,
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent"),
		metricDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs"),
		metricReceiveEventLagDuration:                metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
	}
	eventCollector.logCoordinatorClient = newLogCoordinatorClient(eventCollector)
	eventCollector.ds = NewEventDynamicStream(eventCollector)
	eventCollector.congestionController = newCongestionController(eventCollector)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.MessageCenterHandler)

	return eventCollector
}

func (c *EventCollector) Run(ctx context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	c.g = g
	c.cancel = cancel

	for _, ch := range c.receiveChannels {
		g.Go(func() error {
			return c.runDispatchMessage(ctx, ch)
		})
	}

	g.Go(func() error {
		return c.logCoordinatorClient.run(ctx)
	})

	g.Go(func() error {
		return c.processDSFeedback(ctx)
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
	c.logCoordinatorClient.requestReusableEventService(target)
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

	stat := newDispatcherStat(target, c, readyCallback, memoryQuota)
	c.dispatcherMap.Store(target.GetId(), stat)
	c.changefeedIDMap.Store(target.GetChangefeedID().ID(), target.GetChangefeedID())
	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota, dynstream.MemoryControlForEventCollector, "eventCollector")
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
	stat.commitReady(c.getLocalServerID())
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
	c.congestionController.removeDispatcher(stat)
}

// Queues a message for sending (best-effort, no delivery guarantee)
// Messages may be dropped if errors occur. For reliable delivery, implement retry/ack logic at caller side
func (c *EventCollector) enqueueMessageForSend(msg *messaging.TargetMessage) {
	if msg != nil {
		c.dispatcherMessageChan.In() <- DispatcherMessage{
			Message:    msg,
			RetryCount: 0,
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
				log.Info("failed to send dispatcher request message, try again later",
					zap.String("message", req.Message.String()),
					zap.Error(err))
				if !req.incrAndCheckRetry() {
					log.Warn("dispatcher request retry limit exceeded, dropping request",
						zap.String("message", req.Message.String()))
					continue
				}
				// Put the request back to the channel for later retry.
				c.dispatcherMessageChan.In() <- req
				// Sleep a short time to avoid too many requests in a short time.
				// TODO: requests can to different EventService, so we should improve the logic here.
				time.Sleep(10 * time.Millisecond)
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

// runDispatchMessage dispatches messages from the input channel to the dynamic stream.
// Note: Avoid implementing any message handling logic within this function
// as messages may be stale and need be verified before process.
func (c *EventCollector) runDispatchMessage(ctx context.Context, inCh <-chan *messaging.TargetMessage) error {
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
							c.ds.Push(resolvedEvent.DispatcherID, dispatcher.NewDispatcherEvent(from, resolvedEvent))
							resolvedTsCount += resolvedEvent.Len()
						}
						c.metricDispatcherReceivedResolvedTsEventCount.Add(float64(resolvedTsCount))
					default:
						c.metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						dispatcherEvent := dispatcher.NewDispatcherEvent(&targetMessage.From, e)
						c.ds.Push(e.GetDispatcherID(), dispatcherEvent)
						if e.GetType() == event.TypeBatchDMLEvent {
							c.congestionController.Acknowledge(targetMessage.From, e.(*event.BatchDMLEvent))
						}
					}
				default:
					log.Panic("invalid message type", zap.Any("msg", msg))
				}
			}
		}
	}
}

type congestionController struct {
	collector *EventCollector

	lock           sync.Mutex
	slidingWindows map[common.ChangeFeedID]map[node.ID]slidingWindow
	distributions  map[common.ChangeFeedID]map[node.ID]uint64
}

func newCongestionController(
	collector *EventCollector,
) *congestionController {
	return &congestionController{
		collector:      collector,
		slidingWindows: make(map[common.ChangeFeedID]map[node.ID]slidingWindow),
		distributions:  make(map[common.ChangeFeedID]map[node.ID]uint64),
	}
}

func (c *congestionController) addDispatcher(dispatcher *dispatcherStat) {
	changefeedID := dispatcher.target.GetChangefeedID()
	eventServiceID := dispatcher.connState.getEventServiceID()
	// nodeID is not set yet, just skip it temporarily.
	if eventServiceID == "" {
		log.Panic("event service is not set", zap.Stringer("changefeedID", changefeedID))
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.distributions[changefeedID]; !ok {
		c.distributions[changefeedID] = make(map[node.ID]uint64)
	}
	c.distributions[changefeedID][eventServiceID]++

	if _, ok := c.slidingWindows[changefeedID]; !ok {
		c.slidingWindows[changefeedID] = make(map[node.ID]slidingWindow)
	}

	if _, ok := c.slidingWindows[changefeedID][eventServiceID]; !ok {
		c.slidingWindows[changefeedID][eventServiceID] = newSlidingWindow()
	}
}

func (c *congestionController) removeDispatcher(dispatcher *dispatcherStat) {
	changefeedID := dispatcher.target.GetChangefeedID()
	eventServiceID := dispatcher.connState.getEventServiceID()

	// nodeID is not set yet, just skip it temporarily.
	if eventServiceID == "" {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	proportion, ok := c.distributions[changefeedID]
	if !ok {
		return
	}

	proportion[eventServiceID]--
	if proportion[eventServiceID] == 0 {
		delete(proportion, eventServiceID)
	}
	if len(proportion) == 0 {
		delete(c.slidingWindows, changefeedID)
		delete(c.distributions, changefeedID)
	}
}

func (c *congestionController) queryAvailable(changefeedID common.ChangeFeedID) int64 {
	for _, quota := range c.collector.ds.GetMetrics().MemoryControl.AreaMemoryMetrics {
		if quota.Area() == changefeedID.ID() {
			return quota.AvailableMemory()
		}
	}
	return 0
}

func (c *congestionController) newCongestionControlMessage(
	changefeedID common.ChangeFeedID, nodeID node.ID,
) *messaging.TargetMessage {
	c.lock.Lock()
	proportion, ok := c.distributions[changefeedID]
	if !ok {
		log.Panic("no distribution found for changefeed, this should never happen",
			zap.String("changefeedID", changefeedID.String()))
	}
	defer c.lock.Unlock()

	var sum uint64
	for _, portion := range proportion {
		sum += portion
	}
	if sum == 0 {
		log.Panic("there is no active dispatcher for the changefeed, this should never happen",
			zap.String("changefeedID", changefeedID.String()))
	}

	available := c.queryAvailable(changefeedID)
	portion := proportion[nodeID]
	ratio := float64(portion) / float64(sum)
	available = int64(float64(available) * ratio)
	quota := c.slidingWindows[changefeedID][nodeID].next(available)

	log.Info("send quota", zap.Stringer("changefeedID", changefeedID), zap.Stringer("nodeID", nodeID),
		zap.Int64("quota", quota), zap.Int64("available", available))

	m := event.NewCongestionControl()
	m.AddAvailableMemory(changefeedID.ID(), uint64(quota))
	message := messaging.NewSingleTargetMessage(nodeID, messaging.EventServiceTopic, m)
	return message
}

func (c *congestionController) Acknowledge(from node.ID, item *event.BatchDMLEvent) {
	dispatcherID := item.GetDispatcherID()
	changefeedID := c.collector.getDispatcherStatByID(dispatcherID).target.GetChangefeedID()

	acked := c.slidingWindows[changefeedID][from].ack(item.GetSize())
	if !acked {
		return
	}

	message := c.newCongestionControlMessage(changefeedID, from)
	if err := c.collector.mc.SendCommand(message); err != nil {
		log.Warn("send congestion control message failed", zap.Error(err))
	}
}

const (
	quotaUnit               = 1024 * 1024       // 1MB
	quotaSlowStartThreshold = 1024 * 1024 * 64  // 64MB
	quotaHighThreshold      = 1024 * 1024 * 128 // 128MB
)

type slidingWindow struct {
	requested int64
	confirmed int64
}

func newSlidingWindow() slidingWindow {
	return slidingWindow{
		requested: quotaUnit,
	}
}

func (s slidingWindow) next(available int64) int64 {
	s.confirmed = 0
	if s.requested >= available {
		s.requested = quotaUnit
		return s.requested
	}

	if s.requested < quotaSlowStartThreshold {
		s.requested *= 2
	} else {
		s.requested += quotaUnit
	}

	if s.requested >= quotaHighThreshold {
		s.requested /= 2
	}
	return s.requested
}

func (s slidingWindow) ack(nBytes int64) bool {
	s.confirmed += nBytes
	return s.confirmed >= s.requested
}

func (c *EventCollector) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
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
