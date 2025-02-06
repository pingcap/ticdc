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

package bootstrap

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const (
	defaultResendInterval = time.Millisecond * 500
)

// Bootstrapper handles the logic of a distributed instance(eg. changefeed maintainer, coordinator) startup.
// When a distributed instance starts, it must wait for all nodes to report their current status.
// Only when all nodes have reported their status can the instance bootstrap and schedule tables.
// Note: Bootstrapper is not thread-safe! All methods except `NewBootstrapper` must be called in the same thread.
type Bootstrapper[T any] struct {
	// id is a log identifier
	id string
	// bootstrapped is true when the bootstrapper is bootstrapped
	bootstrapped bool

	// nodes is a map of node id to node status
	nodes map[node.ID]*NodeStatus[T]
	// newBootstrapMsg is a factory function that returns a new bootstrap message
	newBootstrapMsg NewBootstrapMessageFn
	resendInterval  time.Duration

	// For test only
	currentTime func() time.Time
}

// NewBootstrapper create a new bootstrapper for a distributed instance.
func NewBootstrapper[T any](id string, newBootstrapMsg NewBootstrapMessageFn) *Bootstrapper[T] {
	return &Bootstrapper[T]{
		id:              id,
		nodes:           make(map[node.ID]*NodeStatus[T]),
		bootstrapped:    false,
		newBootstrapMsg: newBootstrapMsg,
		currentTime:     time.Now,
		resendInterval:  defaultResendInterval,
	}
}

// HandleNewNodes add node to bootstrapper and return messages that need to be sent to remote node
func (b *Bootstrapper[T]) HandleNewNodes(nodes []*node.Info) []*messaging.TargetMessage {
	msgs := make([]*messaging.TargetMessage, 0, len(nodes))
	for _, info := range nodes {
		if _, ok := b.nodes[info.ID]; !ok {
			// A new node is found, send a bootstrap message to it.
			b.nodes[info.ID] = NewNodeStatus[T](info)
			log.Info("find a new node",
				zap.String("changefeed", b.id),
				zap.String("nodeAddr", info.AdvertiseAddr),
				zap.Any("nodeID", info.ID))
			msgs = append(msgs, b.newBootstrapMsg(info.ID))
			b.nodes[info.ID].lastBootstrapTime = b.currentTime()
		}
	}
	return msgs
}

// HandleRemoveNodes remove node from bootstrapper,
// finished bootstrap if all node are initialized after these node removed
// return cached bootstrap
func (b *Bootstrapper[T]) HandleRemoveNodes(nodeIDs []node.ID) map[node.ID]*T {
	for _, id := range nodeIDs {
		status, ok := b.nodes[id]
		if ok {
			delete(b.nodes, id)
			log.Info("remove node from bootstrapper",
				zap.String("changefeed", b.id),
				zap.Int("status", int(status.state)),
				zap.Any("nodeID", id))
		} else {
			log.Info("node is not tracked by bootstrapper, ignore it",
				zap.String("changefeed", b.id),
				zap.Any("nodeID", id))
		}
	}
	return b.collectInitialBootstrapResponses()
}

// HandleBootstrapResponse do the following:
// 1. cache the bootstrap response reported from remote nodes
// 2. check if all node are initialized
// 3. return cached bootstrap response if all nodes are initialized
func (b *Bootstrapper[T]) HandleBootstrapResponse(
	from node.ID,
	msg *T,
) map[node.ID]*T {
	nodeStatus, ok := b.nodes[from]
	if !ok {
		log.Warn("received bootstrap response from untracked node, ignore it",
			zap.String("changefeed", b.id),
			zap.Any("nodeID", from))
		return nil
	}
	nodeStatus.cachedBootstrapResp = msg
	nodeStatus.state = NodeStateInitialized
	return b.collectInitialBootstrapResponses()
}

// ResendBootstrapMessage return message that need to be resent
func (b *Bootstrapper[T]) ResendBootstrapMessage() []*messaging.TargetMessage {
	var msgs []*messaging.TargetMessage
	if !b.CheckAllNodeInitialized() {
		now := b.currentTime()
		for id, status := range b.nodes {
			if status.state == NodeStateUninitialized &&
				now.Sub(status.lastBootstrapTime) >= b.resendInterval {
				msgs = append(msgs, b.newBootstrapMsg(id))
				status.lastBootstrapTime = now
			}
		}
	}
	return msgs
}

// GetAllNodes return all nodes the tracked by bootstrapper, the returned value must not be modified
func (b *Bootstrapper[T]) GetAllNodes() map[node.ID]*NodeStatus[T] {
	return b.nodes
}

// CheckAllNodeInitialized check if all nodes are initialized.
// returns true when all nodes report the bootstrap response and bootstrapped
func (b *Bootstrapper[T]) CheckAllNodeInitialized() bool {
	return b.bootstrapped && b.checkAllNodeInitialized()
}

// return true if all nodes have reported bootstrap response
func (b *Bootstrapper[T]) checkAllNodeInitialized() bool {
	if len(b.nodes) == 0 {
		return false
	}
	for _, nodeStatus := range b.nodes {
		if nodeStatus.state == NodeStateUninitialized {
			return false
		}
	}
	return true
}

// collectInitialBootstrapResponses checks if all nodes have been initialized and
// collects their bootstrap responses.
// Returns:
//   - nil if either:
//     a) not all nodes are initialized yet, or
//     b) bootstrap was already completed previously
//   - map[node.ID]*T containing all nodes' bootstrap responses on first successful bootstrap,
//     after which the cached responses are cleared
//
// Note: This method will only return a non-nil result exactly once during the bootstrapper's lifecycle.
func (b *Bootstrapper[T]) collectInitialBootstrapResponses() map[node.ID]*T {
	if !b.bootstrapped && b.checkAllNodeInitialized() {
		b.bootstrapped = true
		nodeBootstrapResponses := make(map[node.ID]*T, len(b.nodes))
		for _, status := range b.nodes {
			nodeBootstrapResponses[status.node.ID] = status.cachedBootstrapResp
			status.cachedBootstrapResp = nil
		}
		return nodeBootstrapResponses
	}
	return nil
}

type NodeState int

const (
	// NodeStateUninitialized means the node status is unknown,
	// no bootstrap response of this node received yet.
	NodeStateUninitialized NodeState = iota
	// NodeStateInitialized means bootstrapper has received the bootstrap response of this node.
	NodeStateInitialized
)

// NodeStatus represents the bootstrap state and metadata of a node in the system.
// It tracks initialization status, node information, cached bootstrap response,
// and timing data for bootstrap message retries.
type NodeStatus[T any] struct {
	state NodeState
	node  *node.Info
	// cachedBootstrapResp is the bootstrap response of this node.
	// It is cached when the bootstrap response is received.
	cachedBootstrapResp *T

	// lastBootstrapTime is the time when the bootstrap message is created for this node.
	// It approximates the time when we send the bootstrap message to the node.
	// It is used to limit the frequency of sending bootstrap message.
	lastBootstrapTime time.Time
}

func NewNodeStatus[T any](node *node.Info) *NodeStatus[T] {
	return &NodeStatus[T]{
		state: NodeStateUninitialized,
		node:  node,
	}
}

type NewBootstrapMessageFn func(id node.ID) *messaging.TargetMessage
