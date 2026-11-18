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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
)

const (
	logCoordinatorTopic        = messaging.LogCoordinatorTopic
	logCoordinatorClientTopic  = messaging.LogCoordinatorClientTopic
	logCoordinatorQueryTimeout = 5 * time.Second
)

type logCoordinatorInfo struct {
	id       node.ID
	revision int64
}

type pendingReusableRequest struct {
	request     *logservicepb.ReusableEventServiceRequest
	dispatcher  *dispatcherStat
	coordinator logCoordinatorInfo
	requestID   uint64
	attempts    int
}

type LogCoordinatorClient struct {
	eventCollector            *EventCollector
	mc                        messaging.MessageCenter
	electionClient            etcd.OwnerCaptureInfoClient
	coordinatorMessages       chan *messaging.TargetMessage
	logCoordinatorRequestChan *chann.DrainableChann[*logservicepb.ReusableEventServiceRequest]
	enableRemoteEventService  bool
	// Coordinator identity and pending queries are owned by run. Election reads
	// must not block the message center's shared event routing goroutine.
	coordinatorInfo logCoordinatorInfo
	pendingRequests map[common.DispatcherID]*pendingReusableRequest
	nextRequestID   uint64
}

func newLogCoordinatorClient(eventCollector *EventCollector, electionClient etcd.OwnerCaptureInfoClient) *LogCoordinatorClient {
	client := &LogCoordinatorClient{
		eventCollector:            eventCollector,
		mc:                        eventCollector.mc,
		electionClient:            electionClient,
		coordinatorMessages:       make(chan *messaging.TargetMessage, receiveChanSize),
		logCoordinatorRequestChan: chann.NewAutoDrainChann[*logservicepb.ReusableEventServiceRequest](),
		enableRemoteEventService:  config.GetGlobalServerConfig().Debug.EventService.EnableRemoteEventService,
		pendingRequests:           make(map[common.DispatcherID]*pendingReusableRequest),
	}
	client.mc.RegisterHandler(logCoordinatorClientTopic, client.MessageCenterHandler)
	return client
}

func (l *LogCoordinatorClient) MessageCenterHandler(ctx context.Context, targetMessage *messaging.TargetMessage) error {
	select {
	case l.coordinatorMessages <- targetMessage:
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
		// Broadcasts repeat and pending queries are retried by run. Dropping
		// control messages here keeps election I/O off the data routing path.
	}
	return nil
}

func (l *LogCoordinatorClient) handleCoordinatorMessage(ctx context.Context, targetMessage *messaging.TargetMessage) {
	if targetMessage == nil || targetMessage.From.IsEmpty() {
		return
	}
	for _, msg := range targetMessage.Message {
		switch msg := msg.(type) {
		case *common.LogCoordinatorBroadcastRequest:
			revision, ok := l.getCoordinatorRevision(ctx, targetMessage.From)
			if !ok {
				if l.coordinatorInfo.id == targetMessage.From {
					l.coordinatorInfo = logCoordinatorInfo{}
				}
				continue
			}
			l.setCoordinatorInfo(logCoordinatorInfo{id: targetMessage.From, revision: revision})
		case *logservicepb.ReusableEventServiceResponse:
			l.handleReusableResponse(ctx, targetMessage.From, msg)
		default:
			log.Warn("unknown message type, ignore it",
				zap.String("type", targetMessage.Type.String()))
		}
	}
}

func (l *LogCoordinatorClient) run(ctx context.Context) error {
	retryTicker := time.NewTicker(time.Second)
	defer retryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case msg := <-l.coordinatorMessages:
			l.handleCoordinatorMessage(ctx, msg)
		case req := <-l.logCoordinatorRequestChan.Out():
			l.addReusableRequest(req)
		case <-retryTicker.C:
			for _, request := range l.pendingRequests {
				l.sendReusableRequest(request)
			}
		}
	}
}

func (l *LogCoordinatorClient) requestReusableEventService(dispatcher dispatcher.DispatcherService) {
	if l.enableRemoteEventService && dispatcher.GetTableSpan().TableID != 0 {
		l.logCoordinatorRequestChan.In() <- &logservicepb.ReusableEventServiceRequest{
			ID:      dispatcher.GetId().ToPB(),
			Span:    dispatcher.GetTableSpan(),
			StartTs: dispatcher.GetStartTs(),
		}
	}
}

func (l *LogCoordinatorClient) getCoordinatorRevision(ctx context.Context, id node.ID) (int64, bool) {
	if l.electionClient == nil {
		return 0, false
	}
	ctx, cancel := context.WithTimeout(ctx, logCoordinatorQueryTimeout)
	defer cancel()
	revision, err := l.electionClient.GetLogCoordinatorRevision(ctx, id.String())
	return revision, err == nil && revision > 0
}

func (l *LogCoordinatorClient) setCoordinatorInfo(info logCoordinatorInfo) {
	if l.coordinatorInfo == info {
		return
	}
	l.coordinatorInfo = info
	for _, request := range l.pendingRequests {
		l.sendReusableRequest(request)
	}
}

func (l *LogCoordinatorClient) addReusableRequest(req *logservicepb.ReusableEventServiceRequest) {
	if !l.enableRemoteEventService || req == nil || req.ID == nil {
		return
	}
	id := common.NewDispatcherIDFromPB(req.ID)
	stat := l.eventCollector.getDispatcherStatByID(id)
	if stat == nil {
		return
	}
	request := &pendingReusableRequest{request: req, dispatcher: stat}
	l.pendingRequests[id] = request
	l.sendReusableRequest(request)
}

func (l *LogCoordinatorClient) isRequestActive(request *pendingReusableRequest) bool {
	return l.eventCollector.getDispatcherStatByID(request.dispatcher.getDispatcherID()) == request.dispatcher &&
		!request.dispatcher.session.connState.isRemoved() && !request.dispatcher.session.isReceivingDataEvent()
}

func (l *LogCoordinatorClient) sendReusableRequest(request *pendingReusableRequest) {
	if !l.isRequestActive(request) {
		delete(l.pendingRequests, request.dispatcher.getDispatcherID())
		return
	}
	if l.coordinatorInfo.id.IsEmpty() {
		return
	}
	if request.coordinator != l.coordinatorInfo {
		l.nextRequestID++
		request.requestID = l.nextRequestID
		request.coordinator = l.coordinatorInfo
		request.attempts = 0
	}
	if request.attempts >= commonMsgRetryQuota {
		delete(l.pendingRequests, request.dispatcher.getDispatcherID())
		return
	}
	request.attempts++
	// The local message center may retain the request pointer, so retries must
	// not mutate an already sent message when the coordinator generation changes.
	message := *request.request
	message.RequestID = request.requestID
	_ = l.mc.SendCommand(messaging.NewSingleTargetMessage(l.coordinatorInfo.id, logCoordinatorTopic, &message))
}

func (l *LogCoordinatorClient) handleReusableResponse(
	ctx context.Context, from node.ID, response *logservicepb.ReusableEventServiceResponse,
) {
	if !l.enableRemoteEventService || response == nil || response.ID == nil || response.RequestID == 0 {
		return
	}
	id := common.NewDispatcherIDFromPB(response.ID)
	request, ok := l.pendingRequests[id]
	if !ok || request.requestID != response.RequestID || request.coordinator != l.coordinatorInfo || from != l.coordinatorInfo.id {
		return
	}
	if !l.isRequestActive(request) {
		delete(l.pendingRequests, id)
		return
	}
	revision, valid := l.getCoordinatorRevision(ctx, from)
	if !valid {
		l.coordinatorInfo = logCoordinatorInfo{}
		return
	}
	if revision != request.coordinator.revision {
		l.setCoordinatorInfo(logCoordinatorInfo{id: from, revision: revision})
		return
	}
	seen := make(map[node.ID]struct{}, len(response.Nodes))
	nodes := make([]string, 0, len(response.Nodes))
	for _, candidate := range response.Nodes {
		id := node.ID(candidate)
		if id.IsEmpty() || id == l.eventCollector.serverId {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		if l.mc.IsReadyToSend(id) {
			nodes = append(nodes, candidate)
		}
	}
	delete(l.pendingRequests, id)
	request.dispatcher.startRemoteProbing(nodes)
}
