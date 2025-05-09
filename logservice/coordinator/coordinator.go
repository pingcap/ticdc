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

package logcoordinator

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/chann"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/spanz"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

type LogCoordinator interface {
	Run(ctx context.Context) error
}

type subscriptionState struct {
	subID        uint64
	span         *heartbeatpb.TableSpan
	checkpointTs uint64
	resolvedTs   uint64
}

type subscriptionStates []*subscriptionState // sorted by subID for easy update

type eventStoreState struct {
	subscriptionStates map[int64]subscriptionStates
}

type requestAndTarget struct {
	req    *logservicepb.ReusableEventServiceRequest
	target node.ID
}

type logCoordinator struct {
	messageCenter messaging.MessageCenter

	nodes struct {
		sync.RWMutex
		m map[node.ID]*node.Info
	}

	eventStoreStates struct {
		sync.RWMutex
		m map[node.ID]*eventStoreState
	}

	requestChan *chann.DrainableChann[requestAndTarget]
}

func New() LogCoordinator {
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &logCoordinator{
		messageCenter: messageCenter,
		requestChan:   chann.NewAutoDrainChann[requestAndTarget](),
	}
	c.nodes.m = make(map[node.ID]*node.Info)
	c.eventStoreStates.m = make(map[node.ID]*eventStoreState)

	// recv and handle messages
	messageCenter.RegisterHandler(messaging.LogCoordinatorTopic, c.handleMessage)
	// watch node changes
	nodeManager := appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	nodes := nodeManager.GetAliveNodes()
	for id, node := range nodes {
		c.nodes.m[id] = node
	}
	nodeManager.RegisterNodeChangeHandler("log-coordinator", c.handleNodeChange)
	return c
}

func (c *logCoordinator) Run(ctx context.Context) error {
	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			// send broadcast message to all nodes
			c.nodes.RLock()
			messages := make([]*messaging.TargetMessage, 0, 2*len(c.nodes.m))
			for id := range c.nodes.m {
				messages = append(messages, messaging.NewSingleTargetMessage(id, messaging.EventStoreTopic, &common.LogCoordinatorBroadcastRequest{}))
				messages = append(messages, messaging.NewSingleTargetMessage(id, messaging.EventCollectorTopic, &common.LogCoordinatorBroadcastRequest{}))
			}
			c.nodes.RUnlock()
			for _, message := range messages {
				// just ignore messagees fail to send
				if err := c.messageCenter.SendEvent(message); err != nil {
					log.Debug("send broadcast message to node failed", zap.Error(err))
				}
			}
		case req := <-c.requestChan.Out():
			nodes := c.getCandidateNodes(req.target, req.req.GetSpan(), req.req.GetStartTs())
			response := &logservicepb.ReusableEventServiceResponse{
				ID:    req.req.GetID(),
				Nodes: nodes,
			}
			c.messageCenter.SendEvent(messaging.NewSingleTargetMessage(req.target, messaging.EventCollectorTopic, response))
		}
	}
}

func (c *logCoordinator) handleMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg.(type) {
		case *logservicepb.EventStoreState:
			c.updateEventStoreState(targetMessage.From, msg.(*logservicepb.EventStoreState))
		case *logservicepb.ReusableEventServiceRequest:
			c.requestChan.In() <- requestAndTarget{
				req:    msg.(*logservicepb.ReusableEventServiceRequest),
				target: targetMessage.From,
			}
		default:
			log.Panic("invalid message type", zap.Any("msg", msg))
		}
	}
	return nil
}

func (c *logCoordinator) handleNodeChange(allNodes map[node.ID]*node.Info) {
	c.nodes.Lock()
	defer c.nodes.Unlock()
	for id := range c.nodes.m {
		if _, ok := allNodes[id]; !ok {
			delete(c.nodes.m, id)
			log.Info("log coordinator detect node removed", zap.String("nodeId", id.String()))
		}
	}
	for id, node := range allNodes {
		if _, ok := c.nodes.m[id]; !ok {
			c.nodes.m[id] = node
			log.Info("log coordinator detect node added", zap.String("nodeId", id.String()))
		}
	}
}

func (c *logCoordinator) updateEventStoreState(nodeId node.ID, state *logservicepb.EventStoreState) {
	c.eventStoreStates.Lock()
	defer c.eventStoreStates.Unlock()

	// TODO: avoid remove all, only update related subscription states
	delete(c.eventStoreStates.m, nodeId)
	eventStoreState := &eventStoreState{
		subscriptionStates: make(map[int64]subscriptionStates),
	}
	count := 0
	for tableId, subscriptions := range state.GetSubscriptions() {
		subs := subscriptions.GetSubscriptions()
		subStates := make(subscriptionStates, 0, len(subs))
		count += len(subs)
		for _, subscription := range subs {
			subscriptionState := &subscriptionState{
				subID:        subscription.GetSubID(),
				span:         subscription.GetSpan(),
				checkpointTs: subscription.GetCheckpointTs(),
				resolvedTs:   subscription.GetResolvedTs(),
			}
			subStates = append(subStates, subscriptionState)
		}
		eventStoreState.subscriptionStates[tableId] = subStates
	}
	c.eventStoreStates.m[nodeId] = eventStoreState
}

// getCandidateNode return all nodes(exclude the request node) which may contain data for `span` from `startTs`,
// and the return slice should be sorted by resolvedTs(largest first).
func (c *logCoordinator) getCandidateNodes(requestNodeID node.ID, span *heartbeatpb.TableSpan, startTs uint64) []string {
	c.eventStoreStates.RLock()
	defer c.eventStoreStates.RUnlock()

	// TODO: support incomplete span
	if !isCompleteSpan(span) {
		return nil
	}

	type candidateNode struct {
		nodeID     node.ID
		resolvedTs uint64
	}
	var candidates []candidateNode
	for nodeID, state := range c.eventStoreStates.m {
		if nodeID == requestNodeID {
			continue
		}
		subscriptionStates, ok := state.subscriptionStates[span.GetTableID()]
		if !ok {
			continue
		}
		// Find the maximum resolvedTs for the current nodeID
		var maxResolvedTs uint64
		found := false
		for _, subscriptionState := range subscriptionStates {
			if subscriptionState.checkpointTs <= startTs {
				if !found || subscriptionState.resolvedTs > maxResolvedTs {
					maxResolvedTs = subscriptionState.resolvedTs
					found = true
				}
			}
		}

		// If a valid subscription with checkpointTs <= startTs was found, add to candidates
		if found {
			candidates = append(candidates, candidateNode{
				nodeID:     nodeID,
				resolvedTs: maxResolvedTs,
			})
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].resolvedTs > candidates[j].resolvedTs
	})

	var candidateNodes []string
	for _, candidate := range candidates {
		candidateNodes = append(candidateNodes, string(candidate.nodeID))
	}

	return candidateNodes
}

func isCompleteSpan(tableSpan *heartbeatpb.TableSpan) bool {
	startKey, endKey := spanz.GetTableRange(tableSpan.TableID)
	if spanz.StartCompare(startKey, tableSpan.StartKey) == 0 && spanz.EndCompare(endKey, tableSpan.EndKey) == 0 {
		return true
	}
	return false
}
