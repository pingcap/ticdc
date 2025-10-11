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
	"math"
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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	eventStoreTopic           = messaging.EventStoreTopic
	logCoordinatorTopic       = messaging.LogCoordinatorTopic
	logCoordinatorClientTopic = messaging.LogCoordinatorClientTopic
)

type LogCoordinator interface {
	Run(ctx context.Context) error
}

type requestAndTarget struct {
	req    *logservicepb.ReusableEventServiceRequest
	target node.ID
}

type changefeedState struct {
	cfID       common.ChangeFeedID
	nodeStates map[node.ID]uint64

	minPullerResolvedTs uint64
	resolvedTsGauge     prometheus.Gauge
	resolvedTsLagGauge  prometheus.Gauge
}

type logCoordinator struct {
	messageCenter messaging.MessageCenter
	pdClock       pdutil.Clock

	nodes struct {
		sync.Mutex
		m map[node.ID]*node.Info
	}

	eventStoreStates struct {
		sync.Mutex
		m map[node.ID]*logservicepb.EventStoreState
	}

	changefeedStates struct {
		sync.Mutex
		// GID -> changefeedState
		m map[common.GID]*changefeedState
	}

	requestChan *chann.DrainableChann[requestAndTarget]
}

func New() LogCoordinator {
	messageCenter := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	c := &logCoordinator{
		messageCenter: messageCenter,
		pdClock:       appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		requestChan:   chann.NewAutoDrainChann[requestAndTarget](),
	}
	c.nodes.m = make(map[node.ID]*node.Info)
	c.eventStoreStates.m = make(map[node.ID]*logservicepb.EventStoreState)
	c.changefeedStates.m = make(map[common.GID]*changefeedState)

	// recv and handle messages
	messageCenter.RegisterHandler(logCoordinatorTopic, c.handleMessage)
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
	broadcastTick := time.NewTicker(time.Second)
	defer broadcastTick.Stop()
	metricTick := time.NewTicker(1 * time.Second)
	defer metricTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-broadcastTick.C:
			// send broadcast message to all nodes
			c.nodes.Lock()
			messages := make([]*messaging.TargetMessage, 0, 2*len(c.nodes.m))
			phyResolvedTs := c.getAllPullerPhyResolvedTs()
			entries := make([]*heartbeatpb.ChangefeedPullerResolvedTsEntry, 0, len(phyResolvedTs))
			if len(phyResolvedTs) > 0 {
				for cfID, ts := range phyResolvedTs {
					entries = append(entries, &heartbeatpb.ChangefeedPullerResolvedTsEntry{
						ChangefeedID: cfID.ToPB(),
						ResolvedTs:   ts,
					})
				}
			}
			for id := range c.nodes.m {
				messages = append(messages, messaging.NewSingleTargetMessage(id, eventStoreTopic, &common.LogCoordinatorBroadcastRequest{}))
				messages = append(messages, messaging.NewSingleTargetMessage(id, logCoordinatorClientTopic, &common.LogCoordinatorBroadcastRequest{}))
				// We don't know which id is coordinator, so we broadcast to all nodes.
				// If we try to find the coordinator node, we need to maintain a etcd client in log coordinator,
				// and get the coordinator info from etcd every broadcastTick.
				if len(entries) != 0 {
					messages = append(messages, messaging.NewSingleTargetMessage(
						id,
						messaging.CoordinatorTopic,
						&heartbeatpb.AllChangefeedPullerResolvedTs{Entries: entries}),
					)
				}
			}
			c.nodes.Unlock()
			for _, message := range messages {
				// just ignore messages fail to send
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
			err := c.messageCenter.SendEvent(messaging.NewSingleTargetMessage(req.target, logCoordinatorClientTopic, response))
			if err != nil {
				log.Warn("send reusable event service response failed", zap.Error(err))
			}
		case <-metricTick.C:
			c.updateChangefeedMetrics()
		}
	}
}

func (c *logCoordinator) handleMessage(_ context.Context, targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		switch msg := msg.(type) {
		case *logservicepb.EventStoreState:
			c.updateEventStoreState(targetMessage.From, msg)
		case *logservicepb.ChangefeedStates:
			c.updateChangefeedStates(targetMessage.From, msg)
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

			// Clean up states for the removed node.
			c.eventStoreStates.Lock()
			delete(c.eventStoreStates.m, id)
			c.eventStoreStates.Unlock()

			c.changefeedStates.Lock()
			for _, state := range c.changefeedStates.m {
				if _, exists := state.nodeStates[id]; exists {
					delete(state.nodeStates, id)
				}
			}
			c.changefeedStates.Unlock()
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

func (c *logCoordinator) updateChangefeedStates(from node.ID, states *logservicepb.ChangefeedStates) {
	c.changefeedStates.Lock()
	defer c.changefeedStates.Unlock()

	// Create a set of incoming changefeed GIDs for efficient lookup.
	incomingGIDs := make(map[common.GID]struct{})
	for _, state := range states.States {
		cfID := common.NewChangefeedIDFromPB(state.GetChangefeedID())
		incomingGIDs[cfID.ID()] = struct{}{}
	}

	// First, handle changefeeds that might have been removed from the reporting node.
	for gid, state := range c.changefeedStates.m {
		// If the node was previously reporting for this changefeed...
		if _, exists := state.nodeStates[from]; exists {
			// ...but is no longer in the incoming message, it means the changefeed was removed from this node.
			if _, incoming := incomingGIDs[gid]; !incoming {
				delete(state.nodeStates, from)
				log.Info("changefeed removed from node",
					zap.Stringer("changefeedID", state.cfID),
					zap.String("nodeID", string(from)),
					zap.Uint64("changefeedGIDLow", gid.Low),
					zap.Uint64("changefeedGIDHigh", gid.High))
				// If the changefeed has no more nodes, remove the changefeed state and its associated metrics.
				if len(state.nodeStates) == 0 {
					metrics.ChangefeedResolvedTsGauge.DeleteLabelValues(state.cfID.Keyspace(), state.cfID.Name())
					metrics.ChangefeedResolvedTsLagGauge.DeleteLabelValues(state.cfID.Keyspace(), state.cfID.Name())
					delete(c.changefeedStates.m, gid)
					log.Info("changefeed state removed as it has no active nodes",
						zap.Stringer("changefeedID", state.cfID),
						zap.Uint64("changefeedGIDLow", gid.Low),
						zap.Uint64("changefeedGIDHigh", gid.High))
				}
			}
		}
	}

	// Then, update with the new states from the message.
	for _, state := range states.States {
		cfID := common.NewChangefeedIDFromPB(state.GetChangefeedID())
		gid := cfID.ID()
		if _, ok := c.changefeedStates.m[gid]; !ok {
			log.Info("new changefeed states added",
				zap.Stringer("changefeedID", cfID),
				zap.Uint64("changefeedGIDLow", gid.Low),
				zap.Uint64("changefeedGIDHigh", gid.High))
			// Initialize metrics for the new changefeed.
			c.changefeedStates.m[gid] = &changefeedState{
				cfID:               cfID,
				nodeStates:         make(map[node.ID]uint64),
				resolvedTsGauge:    metrics.ChangefeedResolvedTsGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
				resolvedTsLagGauge: metrics.ChangefeedResolvedTsLagGauge.WithLabelValues(cfID.Keyspace(), cfID.Name()),
			}
		}
		c.changefeedStates.m[gid].nodeStates[from] = state.GetResolvedTs()
	}
}

func (c *logCoordinator) updateChangefeedMetrics() {
	pdTime := c.pdClock.CurrentTime()
	pdPhyTs := oracle.GetPhysical(pdTime)

	c.changefeedStates.Lock()
	defer c.changefeedStates.Unlock()

	for _, state := range c.changefeedStates.m {
		if len(state.nodeStates) == 0 {
			continue
		}

		minResolvedTs := uint64(math.MaxUint64)
		for _, resolvedTs := range state.nodeStates {
			if resolvedTs < minResolvedTs {
				minResolvedTs = resolvedTs
			}
		}

		if minResolvedTs == math.MaxUint64 {
			log.Warn("minResolvedTs is MaxUint64, this should not happen",
				zap.Stringer("changefeedID", state.cfID))
			continue
		}

		phyResolvedTs := oracle.ExtractPhysical(minResolvedTs)
		state.minPullerResolvedTs = minResolvedTs
		state.resolvedTsGauge.Set(float64(phyResolvedTs))
		lag := float64(pdPhyTs-phyResolvedTs) / 1e3
		state.resolvedTsLagGauge.Set(lag)
	}
}

func (c *logCoordinator) getAllPullerPhyResolvedTs() map[common.ChangeFeedID]uint64 {
	result := make(map[common.ChangeFeedID]uint64)
	c.changefeedStates.Lock()
	defer c.changefeedStates.Unlock()

	for _, state := range c.changefeedStates.m {
		if len(state.nodeStates) == 0 {
			continue
		}
		result[state.cfID] = state.minPullerResolvedTs
	}
	return result
}

// getCandidateNode return all nodes(exclude the request node) which may contain data for `span` from `startTs`,
// and the return slice should be sorted by resolvedTs(largest first).
func (c *logCoordinator) getCandidateNodes(requestNodeID node.ID, span *heartbeatpb.TableSpan, startTs uint64) []string {
	type candidateSubscription struct {
		nodeID         node.ID
		subscriptionID uint64
		resolvedTs     uint64
	}
	var candidateSubs []candidateSubscription
	c.eventStoreStates.Lock()
	for nodeID, eventStoreState := range c.eventStoreStates.m {
		if nodeID == requestNodeID {
			continue
		}
		subStates, ok := eventStoreState.GetTableStates()[span.GetTableID()]
		if !ok {
			continue
		}
		// Find the maximum resolvedTs for the current nodeID
		var maxResolvedTs, subID uint64
		found := false
		for _, subsState := range subStates.GetSubscriptions() {
			if bytes.Compare(subsState.Span.StartKey, span.StartKey) <= 0 &&
				bytes.Compare(span.EndKey, subsState.Span.EndKey) <= 0 &&
				subsState.CheckpointTs <= startTs {
				if !found || subsState.ResolvedTs > maxResolvedTs {
					maxResolvedTs = subsState.ResolvedTs
					subID = subsState.SubID
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
			candidateSubs = append(candidateSubs, candidateSubscription{
				nodeID:         nodeID,
				subscriptionID: subID,
				resolvedTs:     maxResolvedTs,
			})
		}
	}
	c.eventStoreStates.Unlock()

	// return candidate nodes sorted by resolvedTs in descending order
	sort.Slice(candidateSubs, func(i, j int) bool {
		return candidateSubs[i].resolvedTs > candidateSubs[j].resolvedTs
	})
	var subIDs []uint64
	var candidateNodes []string
	if len(candidateSubs) > 0 {
		c.nodes.Lock()
		for _, candidate := range candidateSubs {
			if c.nodes.m[candidate.nodeID] != nil {
				subIDs = append(subIDs, candidate.subscriptionID)
				candidateNodes = append(candidateNodes, string(candidate.nodeID))
			}
		}
		c.nodes.Unlock()
	}
	log.Info("log coordinator get candidate nodes",
		zap.String("requestNodeID", requestNodeID.String()),
		zap.String("span", common.FormatTableSpan(span)),
		zap.Uint64("startTs", startTs),
		zap.Strings("candidateNodes", candidateNodes),
		zap.Uint64s("subscriptionIDs", subIDs))

	return candidateNodes
}
