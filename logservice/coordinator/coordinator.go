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
	"bytes"
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
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type LogCoordinator interface {
	Run(ctx context.Context) error
}

type requestAndTarget struct {
	req    *logservicepb.ReusableEventServiceRequest
	target node.ID
}

type logCoordinator struct {
	messageCenter messaging.MessageCenter

	nodes struct {
		sync.Mutex
		m map[node.ID]*node.Info
	}

	eventStoreStates struct {
		sync.Mutex
		m map[node.ID]*logservicepb.EventStoreState
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
	c.eventStoreStates.m = make(map[node.ID]*logservicepb.EventStoreState)

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
			c.nodes.Lock()
			messages := make([]*messaging.TargetMessage, 0, 2*len(c.nodes.m))
			for id := range c.nodes.m {
				messages = append(messages, messaging.NewSingleTargetMessage(id, messaging.EventStoreTopic, &common.LogCoordinatorBroadcastRequest{}))
				messages = append(messages, messaging.NewSingleTargetMessage(id, messaging.EventCollectorTopic, &common.LogCoordinatorBroadcastRequest{}))
			}
			c.nodes.Unlock()
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
			err := c.messageCenter.SendEvent(messaging.NewSingleTargetMessage(req.target, messaging.EventCollectorTopic, response))
			if err != nil {
				log.Warn("send reusable event service response failed", zap.Error(err))
			}
		}
	}
}

func (c *logCoordinator) handleMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg := msg.(type) {
		case *logservicepb.EventStoreState:
			c.updateEventStoreState(targetMessage.From, msg)
		case *logservicepb.ReusableEventServiceRequest:
			c.requestChan.In() <- requestAndTarget{
				req:    msg,
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

func (c *logCoordinator) updateEventStoreState(nodeID node.ID, newState *logservicepb.EventStoreState) {
	c.eventStoreStates.Lock()
	defer c.eventStoreStates.Unlock()

	c.eventStoreStates.m[nodeID] = newState
}

// getCandidateNode return all nodes(exclude the request node) which may contain data for `span` from `startTs`,
// and the return slice should be sorted by resolvedTs(largest first).
func (c *logCoordinator) getCandidateNodes(requestNodeID node.ID, span *heartbeatpb.TableSpan, startTs uint64) []string {
	c.eventStoreStates.Lock()
	defer c.eventStoreStates.Unlock()

	// FIXME: remove this check
	if !isCompleteSpan(span) {
		return nil
	}

	type candidateNode struct {
		nodeID     node.ID
		resolvedTs uint64
	}
	var candidates []candidateNode
	for nodeID, eventStoreState := range c.eventStoreStates.m {
		if nodeID == requestNodeID {
			continue
		}
		subStates, ok := eventStoreState.GetTableStates()[span.GetTableID()]
		if !ok {
			continue
		}
		// Find the maximum resolvedTs for the current nodeID
		var maxResolvedTs uint64
		found := false
		for _, subsState := range subStates.GetSubscriptions() {
			if bytes.Compare(subsState.Span.StartKey, span.StartKey) <= 0 &&
				bytes.Compare(span.EndKey, subsState.Span.EndKey) <= 0 &&
				subsState.CheckpointTs <= startTs {
				if !found || subsState.ResolvedTs > maxResolvedTs {
					maxResolvedTs = subsState.ResolvedTs
					found = true
				}
			}
		}
		// Check maxResolveTs is not significantly smaller than request startTs to filter out invalid nodes
		const maxTimeDiff = 3600000 // 1 hour in milliseconds
		if found &&
			startTs > maxResolvedTs &&
			(oracle.ExtractPhysical(startTs)-oracle.ExtractPhysical(maxResolvedTs)) > maxTimeDiff {
			found = false
		}

		// If a valid subscription with checkpointTs <= startTs was found, add to candidates
		if found {
			candidates = append(candidates, candidateNode{
				nodeID:     nodeID,
				resolvedTs: maxResolvedTs,
			})
		}
	}

	// return candidate nodes sorted by resolvedTs in descending order
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
