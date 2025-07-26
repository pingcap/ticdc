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

package eventcollector

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/chann"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	logCoordinatorTopic       = messaging.LogCoordinatorTopic
	logCoordinatorClientTopic = messaging.LogCoordinatorClientTopic
)

type LogCoordinatorClient struct {
	eventCollector            *EventCollector
	mc                        messaging.MessageCenter
	coordinatorInfo           atomic.Value
	logCoordinatorRequestChan *chann.DrainableChann[*logservicepb.ReusableEventServiceRequest]
}

func newLogCoordinatorClient(eventCollector *EventCollector) *LogCoordinatorClient {
	client := &LogCoordinatorClient{
		eventCollector:            eventCollector,
		mc:                        appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		logCoordinatorRequestChan: chann.NewAutoDrainChann[*logservicepb.ReusableEventServiceRequest](),
	}
	client.mc.RegisterHandler(logCoordinatorClientTopic, client.MessageCenterHandler)
	return client
}

func (l *LogCoordinatorClient) MessageCenterHandler(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg := msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			l.setCoordinatorInfo(targetMessage.From)
		case *logservicepb.ReusableEventServiceResponse:
			dispatcherID := common.NewDispatcherIDFromPB(msg.ID)
			dispatcher := l.eventCollector.getDispatcherStatByID(dispatcherID)
			if dispatcher != nil {
				dispatcher.setRemoteCandidates(msg.Nodes)
			}
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

func (l *LogCoordinatorClient) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case req := <-l.logCoordinatorRequestChan.Out():
			coordinatorID := l.getCoordinatorInfo()
			if coordinatorID == "" {
				log.Info("coordinator info is empty, try send request later")
				l.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
				continue
			}
			msg := messaging.NewSingleTargetMessage(coordinatorID, logCoordinatorTopic, req)
			err := l.mc.SendCommand(msg)
			if err != nil {
				log.Info("fail to send dispatcher request message to log coordinator, try again later", zap.Error(err))
				l.logCoordinatorRequestChan.In() <- req
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

func (l *LogCoordinatorClient) requestReusableEventService(dispatcher dispatcher.DispatcherService) {
	if dispatcher.GetTableSpan().TableID != 0 {
		l.logCoordinatorRequestChan.In() <- &logservicepb.ReusableEventServiceRequest{
			ID:      dispatcher.GetId().ToPB(),
			Span:    dispatcher.GetTableSpan(),
			StartTs: dispatcher.GetStartTs(),
		}
	}
}

func (c *LogCoordinatorClient) setCoordinatorInfo(id node.ID) {
	c.coordinatorInfo.Store(id)
}

func (c *LogCoordinatorClient) getCoordinatorInfo() node.ID {
	if v := c.coordinatorInfo.Load(); v != nil {
		return v.(node.ID)
	}
	return ""
}
