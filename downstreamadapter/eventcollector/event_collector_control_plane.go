// Copyright 2026 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

func isRepeatedMsgType(msg *messaging.TargetMessage) bool {
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

func (c *EventCollector) enqueueMessageForSend(msg *messaging.TargetMessage) {
	if msg == nil {
		return
	}
	if isRepeatedMsgType(msg) {
		c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, true, 1)
		return
	}
	if msg.To == c.serverId {
		c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, false, 0)
		return
	}
	c.dispatcherMessageChan.In() <- newDispatcherMessage(msg, true, commonMsgRetryQuota)
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

func (c *EventCollector) addRemoveTombstone(dispatcherID common.DispatcherID, serverID node.ID, message *messaging.TargetMessage) {
	c.removeTombstones.Store(dispatcherRemoveKey{
		dispatcherID: dispatcherID,
		serverID:     serverID,
	}, &dispatcherRemoveTombstone{message: message})
}

func (c *EventCollector) clearRemoveTombstone(dispatcherID common.DispatcherID, serverID node.ID) {
	c.removeTombstones.Delete(dispatcherRemoveKey{
		dispatcherID: dispatcherID,
		serverID:     serverID,
	})
}

func (c *EventCollector) SendDispatcherHeartbeat(heartbeat *event.DispatcherHeartbeat) {
	groupedHeartbeats := c.groupHeartbeat(heartbeat)
	for serverID, heartbeat := range groupedHeartbeats {
		msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, heartbeat)
		c.enqueueMessageForSend(msg)
	}
}

func (c *EventCollector) groupHeartbeat(heartbeat *event.DispatcherHeartbeat) map[node.ID]*event.DispatcherHeartbeat {
	groupedHeartbeats := make(map[node.ID]*event.DispatcherHeartbeat)
	group := func(target node.ID, dp event.DispatcherProgress) {
		heartbeat, ok := groupedHeartbeats[target]
		if !ok {
			heartbeat = &event.DispatcherHeartbeat{
				Version:              event.DispatcherHeartbeatVersion1,
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

func (c *EventCollector) sendDispatcherRequests(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case req := <-c.dispatcherMessageChan.Out():
			err := c.mc.SendCommand(req.Message)
			if err != nil {
				sleepInterval := 10 * time.Millisecond
				if appErr, ok := err.(errors.AppError); ok && appErr.Type == errors.ErrorTypeMessageCongested {
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
				c.dispatcherMessageChan.In() <- req
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
		if ds.State != event.DSStateRemoved {
			continue
		}
		v, ok := c.dispatcherMap.Load(ds.DispatcherID)
		if !ok {
			continue
		}
		stat := v.(*dispatcherStat)
		if stat.connState.isCurrentEventService(targetMessage.From) {
			currentIncarnation := stat.connState.getEventServiceIncarnation()
			if isIncarnationStale(currentIncarnation, response.Incarnation) {
				continue
			}
			log.Info("dispatcher removed in event service",
				zap.Stringer("dispatcherID", ds.DispatcherID),
				zap.Stringer("eventServiceID", targetMessage.From),
				zap.Uint64("incarnation", response.Incarnation))
			stat.prepareRebuild(targetMessage.From, response.Incarnation)
		}
	}
}

func (c *EventCollector) handleDispatcherControlEvent(targetMessage *messaging.TargetMessage) {
	if len(targetMessage.Message) != 1 {
		log.Panic("invalid dispatcher control event message", zap.Any("msg", targetMessage))
	}
	controlEvent := targetMessage.Message[0].(*event.DispatcherControlEvent)
	switch controlEvent.Action {
	case event.DispatcherControlActionRemove:
		switch controlEvent.Status {
		case event.DispatcherControlStatusAccepted,
			event.DispatcherControlStatusNotFound,
			event.DispatcherControlStatusStale:
			c.clearRemoveTombstone(controlEvent.DispatcherID, targetMessage.From)
		}
	default:
		stat := c.getDispatcherStatByID(controlEvent.DispatcherID)
		if stat == nil {
			return
		}
		stat.handleDispatcherControlEvent(targetMessage.From, controlEvent)
	}
}

func (c *EventCollector) MessageCenterHandler(ctx context.Context, targetMessage *messaging.TargetMessage) error {
	inflightDuration := time.Since(time.UnixMilli(targetMessage.CreateAt)).Seconds()
	c.metricReceiveEventLagDuration.Observe(inflightDuration)

	start := time.Now()
	defer func() {
		metrics.EventCollectorHandleEventDuration.Observe(time.Since(start).Seconds())
	}()

	switch targetMessage.Type {
	case messaging.TypeReadyEvent, messaging.TypeNotReusableEvent, messaging.TypeHandshakeEvent:
		return c.handleBootstrapMessage(targetMessage)
	}

	if targetMessage.Type.IsLogServiceEvent() {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case c.receiveChannels[targetMessage.GetGroup()%uint64(len(c.receiveChannels))] <- targetMessage:
		}
		return nil
	}

	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *event.DispatcherHeartbeatResponse:
			c.handleDispatcherHeartbeatResponse(targetMessage)
		case *event.DispatcherControlEvent:
			c.handleDispatcherControlEvent(targetMessage)
		default:
			log.Warn("unknown message type, ignore it",
				zap.String("type", targetMessage.Type.String()),
				zap.Any("msg", msg))
		}
	}
	return nil
}

func (c *EventCollector) reconcileDispatcherControl(ctx context.Context) error {
	ticker := time.NewTicker(controlReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.dispatcherMap.Range(func(_, value interface{}) bool {
				value.(*dispatcherStat).reconcileControlRequests()
				return true
			})
			c.removeTombstones.Range(func(_, value interface{}) bool {
				c.enqueueMessageForSend(value.(*dispatcherRemoveTombstone).message)
				return true
			})
		}
	}
}

func (c *EventCollector) RedoMessageCenterHandler(ctx context.Context, targetMessage *messaging.TargetMessage) error {
	switch targetMessage.Type {
	case messaging.TypeReadyEvent, messaging.TypeNotReusableEvent, messaging.TypeHandshakeEvent:
		return c.handleBootstrapMessage(targetMessage)
	}

	if targetMessage.Type.IsLogServiceEvent() {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case c.redoReceiveChannels[targetMessage.GetGroup()%uint64(len(c.redoReceiveChannels))] <- targetMessage:
		}
		return nil
	}
	log.Warn("unknown message type, ignore it",
		zap.String("type", targetMessage.Type.String()),
		zap.Any("msg", targetMessage))
	return nil
}
