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

package dispatchermanager

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

/*
HeartBeatCollector is responsible for sending and receiving messages to maintainer by messageCenter
Sending messages include:
 1. HeartBeatRequest: the watermark and table status
 2. BlockStatusRequest: the info about block events

Receiving messages include:
 1. HeartBeatResponse: the ack and actions for block events(Need a better name)
 2. SchedulerDispatcherRequest: ask for create or remove a dispatcher
 3. CheckpointTsMessage: the latest checkpoint ts of the changefeed, it only for the MQ-class Sink

HeartBeatCollector is an server level component.
*/
type HeartBeatCollector struct {
	from node.ID

	heartBeatReqQueue   *HeartbeatRequestQueue
	blockStatusReqQueue *BlockStatusRequestQueue

	dispatcherStatusDynamicStream           dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, *dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]
	heartBeatResponseDynamicStream          dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *EventDispatcherManager, *HeartBeatResponseHandler]
	schedulerDispatcherRequestDynamicStream dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *EventDispatcherManager, *SchedulerDispatcherRequestHandler]
	checkpointTsMessageDynamicStream        dynstream.DynamicStream[int, common.GID, CheckpointTsMessage, *EventDispatcherManager, *CheckpointTsMessageHandler]

	mc messaging.MessageCenter

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

func NewHeartBeatCollector(serverId node.ID) *HeartBeatCollector {
	dStatusDS := dispatcher.GetDispatcherStatusDynamicStream()
	heartBeatCollector := HeartBeatCollector{
		from:                                    serverId,
		heartBeatReqQueue:                       NewHeartbeatRequestQueue(),
		blockStatusReqQueue:                     NewBlockStatusRequestQueue(),
		dispatcherStatusDynamicStream:           dStatusDS,
		heartBeatResponseDynamicStream:          newHeartBeatResponseDynamicStream(dStatusDS),
		schedulerDispatcherRequestDynamicStream: newSchedulerDispatcherRequestDynamicStream(),
		checkpointTsMessageDynamicStream:        newCheckpointTsMessageDynamicStream(),
		mc:                                      appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
	}
	heartBeatCollector.mc.RegisterHandler(messaging.HeartbeatCollectorTopic, heartBeatCollector.RecvMessages)

	return &heartBeatCollector
}

func (c *HeartBeatCollector) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	log.Info("heartbeat collector is running")

	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		err := c.sendHeartBeatMessages(ctx)
		if err != nil {
			log.Error("failed to send heartbeat messages", zap.Error(err))
		}
	}()

	go func() {
		defer c.wg.Done()
		err := c.sendBlockStatusMessages(ctx)
		if err != nil {
			log.Error("failed to send block status messages", zap.Error(err))
		}
	}()
}

func (c *HeartBeatCollector) RegisterEventDispatcherManager(m *EventDispatcherManager) error {
	m.SetHeartbeatRequestQueue(c.heartBeatReqQueue)
	m.SetBlockStatusRequestQueue(c.blockStatusReqQueue)
	err := c.heartBeatResponseDynamicStream.AddPath(m.changefeedID.Id, m)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.schedulerDispatcherRequestDynamicStream.AddPath(m.changefeedID.Id, m)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *HeartBeatCollector) RegisterCheckpointTsMessageDs(m *EventDispatcherManager) error {
	err := c.checkpointTsMessageDynamicStream.AddPath(m.changefeedID.Id, m)
	return errors.Trace(err)
}

func (c *HeartBeatCollector) RemoveEventDispatcherManager(m *EventDispatcherManager) error {
	err := c.heartBeatResponseDynamicStream.RemovePath(m.changefeedID.Id)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.schedulerDispatcherRequestDynamicStream.RemovePath(m.changefeedID.Id)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *HeartBeatCollector) RemoveCheckpointTsMessage(changefeedID common.ChangeFeedID) error {
	err := c.checkpointTsMessageDynamicStream.RemovePath(changefeedID.Id)
	return errors.Trace(err)
}

func (c *HeartBeatCollector) sendHeartBeatMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("heartbeat collector is shutting down, exit sendHeartBeatMessages")
			return ctx.Err()
		default:
			heartBeatRequestWithTargetID := c.heartBeatReqQueue.Dequeue(ctx)
			if heartBeatRequestWithTargetID == nil {
				continue
			}
			err := c.mc.SendCommand(
				messaging.NewSingleTargetMessage(
					heartBeatRequestWithTargetID.TargetID,
					messaging.MaintainerManagerTopic,
					heartBeatRequestWithTargetID.Request,
				))
			if err != nil {
				log.Error("failed to send heartbeat request message", zap.Error(err))
			}
		}
	}
}

func (c *HeartBeatCollector) sendBlockStatusMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			log.Info("heartbeat collector is shutting down, exit sendBlockStatusMessages")
			return ctx.Err()
		default:
			blockStatusRequestWithTargetID := c.blockStatusReqQueue.Dequeue(ctx)
			if blockStatusRequestWithTargetID == nil {
				continue
			}
			err := c.mc.SendCommand(
				messaging.NewSingleTargetMessage(
					blockStatusRequestWithTargetID.TargetID,
					messaging.MaintainerManagerTopic,
					blockStatusRequestWithTargetID.Request,
				))
			if err != nil {
				log.Error("failed to send block status request message", zap.Error(err))
			}
		}
	}
}

func (c *HeartBeatCollector) RecvMessages(_ context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeHeartBeatResponse:
		// TODO: Change a more appropriate name for HeartBeatResponse. It should be BlockStatusResponse or something else.
		heartbeatResponse := msg.Message[0].(*heartbeatpb.HeartBeatResponse)
		c.heartBeatResponseDynamicStream.Push(
			common.NewChangefeedGIDFromPB(heartbeatResponse.ChangefeedID),
			NewHeartBeatResponse(heartbeatResponse))
	case messaging.TypeScheduleDispatcherRequest:
		schedulerDispatcherRequest := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
		c.schedulerDispatcherRequestDynamicStream.Push(
			common.NewChangefeedGIDFromPB(schedulerDispatcherRequest.ChangefeedID),
			NewSchedulerDispatcherRequest(schedulerDispatcherRequest))
		// TODO: check metrics
		metrics.HandleDispatcherRequsetCounter.WithLabelValues("default", schedulerDispatcherRequest.ChangefeedID.Name, "receive").Inc()
	case messaging.TypeCheckpointTsMessage:
		checkpointTsMessage := msg.Message[0].(*heartbeatpb.CheckpointTsMessage)
		c.checkpointTsMessageDynamicStream.Push(
			common.NewChangefeedIDFromPB(checkpointTsMessage.ChangefeedID).Id,
			NewCheckpointTsMessage(checkpointTsMessage))
	default:
		log.Panic("unknown message type", zap.Any("message", msg.Message))
	}
	return nil
}

func (c *HeartBeatCollector) Close() {
	c.mc.DeRegisterHandler(messaging.HeartbeatCollectorTopic)
	c.cancel()
	c.wg.Wait()
	log.Info("heartbeat collector is closed")
}
