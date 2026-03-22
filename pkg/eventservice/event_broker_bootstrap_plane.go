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

package eventservice

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

func (c *eventBroker) sendNotReusableEvent(server node.ID, d *dispatcherStat) {
	event := event.NewNotReusableEvent(d.info.GetID(), c.incarnation)
	wrapEvent := newWrapNotReusableEvent(server, event)

	c.getMessageCh(d.messageWorkerIndex, common.IsRedoMode(d.info.GetMode())) <- wrapEvent
	updateMetricEventServiceSendCommandCount(d.info.GetMode())
}

func (c *eventBroker) sendDispatcherControlEvent(
	server node.ID,
	dispatcherID common.DispatcherID,
	epoch uint64,
	action event.DispatcherControlAction,
	status event.DispatcherControlStatus,
	reasonCode uint64,
) {
	e := event.NewDispatcherControlEvent(dispatcherID, epoch, c.incarnation, action, status, reasonCode)
	msg := messaging.NewSingleTargetMessage(server, messaging.EventCollectorTopic, &e)
	if err := c.msgSender.SendCommand(msg); err != nil {
		log.Warn("send dispatcher control event failed",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Uint64("epoch", epoch),
			zap.Uint64("incarnation", c.incarnation),
			zap.Uint64("reasonCode", reasonCode),
			zap.Uint8("action", uint8(action)),
			zap.Uint8("status", uint8(status)),
			zap.Error(err))
	}
}

func (c *eventBroker) scanReady(task scanTask) bool {
	if task.isRemoved.Load() {
		return false
	}

	if task.isTaskScanning.Load() {
		return false
	}

	if !c.checkAndSendReady(task) {
		return false
	}

	c.sendHandshakeIfNeed(task)

	ok, _ := c.getScanTaskDataRange(task)
	return ok
}

func (c *eventBroker) checkAndSendReady(task scanTask) bool {
	if task.epoch == 0 {
		now := time.Now().Unix()
		lastSendTime := task.lastReadySendTime.Load()
		currentInterval := task.readyInterval.Load()
		if now-lastSendTime < currentInterval {
			return false
		}
		remoteID := node.ID(task.info.GetServerID())
		event := event.NewReadyEvent(task.info.GetID(), c.incarnation)
		wrapEvent := newWrapReadyEvent(remoteID, event)
		c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- wrapEvent
		log.Debug("send ready event to dispatcher",
			zap.Stringer("changefeedID", task.changefeedStat.changefeedID), zap.Stringer("dispatcherID", task.id))
		task.lastReadySendTime.Store(now)
		newInterval := currentInterval * 2
		if newInterval > maxReadyEventIntervalSeconds {
			newInterval = maxReadyEventIntervalSeconds
		}
		task.readyInterval.Store(newInterval)
		updateMetricEventServiceSendCommandCount(task.info.GetMode())
		return false
	}
	return true
}

func (c *eventBroker) sendHandshakeIfNeed(task scanTask) {
	if task.isHandshaked() {
		return
	}

	task.handshakeLock.Lock()
	defer task.handshakeLock.Unlock()

	if task.isHandshaked() {
		return
	}

	remoteID := node.ID(task.info.GetServerID())
	event := event.NewHandshakeEvent(task.id, task.startTs, task.epoch, c.incarnation, task.startTableInfo)
	log.Info("send handshake event to dispatcher",
		zap.Stringer("changefeedID", task.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", task.id),
		zap.Int64("tableID", task.info.GetTableSpan().GetTableID()),
		zap.Uint64("commitTs", event.GetCommitTs()),
		zap.Uint64("epoch", event.GetEpoch()),
		zap.Uint64("seq", event.GetSeq()))
	wrapEvent := newWrapHandshakeEvent(remoteID, event)
	c.getMessageCh(task.messageWorkerIndex, common.IsRedoMode(task.info.GetMode())) <- wrapEvent
	updateMetricEventServiceSendCommandCount(task.info.GetMode())
	task.setHandshaked()
}
