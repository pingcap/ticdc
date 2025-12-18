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
	"sync"
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
	// bootstrapped is true after make sure all nodes are initialized.
	bootstrapped bool

	mutex sync.RWMutex
	// nodes is a map of node id to node status
	nodes map[node.ID]*nodeStatus[T]

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
		nodes:           make(map[node.ID]*nodeStatus[T]),
		bootstrapped:    false,
		newBootstrapMsg: newBootstrapMsg,
		currentTime:     time.Now,
		resendInterval:  defaultResendInterval,
	}
}

// HandleNewNodes updates the bootstrapper with the current set of active nodes.
// It returns the IDs of newly added nodes, removed nodes, messages to be sent, and any cached bootstrap responses.
func (b *Bootstrapper[T]) HandleNewNodes(activeNodes map[node.ID]*node.Info) (
	addedNodes []node.ID,
	removedNodes []node.ID,
	messages []*messaging.TargetMessage,
	cachedResponses map[node.ID]*T,
) {
	b.mutex.Lock()
	for id, info := range activeNodes {
		if _, ok := b.nodes[id]; ok {
			continue
		}
		// A new node is found, send a bootstrap message to it.
		b.nodes[id] = newNodeStatus[T](info)
		log.Info("found a new node",
			zap.String("id", b.id),
			zap.String("nodeAddr", info.AdvertiseAddr),
			zap.Any("nodeID", id))
		messages = append(messages, b.newBootstrapMsg(id))
		b.nodes[id].lastBootstrapTime = b.currentTime()
		addedNodes = append(addedNodes, id)
	}

	for id := range b.nodes {
		if _, ok := activeNodes[id]; !ok {
			log.Info("remove node from bootstrapper",
				zap.String("id", b.id),
				zap.Any("nodeID", id))
			delete(b.nodes, id)
			removedNodes = append(removedNodes, id)
		}
	}

	b.mutex.Unlock()
	cachedResponses = b.collectInitialBootstrapResponses()
	return
}

// HandleBootstrapResponse do the following:
// 1. cache the bootstrap response reported from remote nodes
// 2. check if all node are initialized
// 3. return cached bootstrap response if all nodes are initialized
func (b *Bootstrapper[T]) HandleBootstrapResponse(
	from node.ID,
	msg *T,
) map[node.ID]*T {
	b.mutex.RLock()
	status, ok := b.nodes[from]
	b.mutex.RUnlock()
	if !ok {
		log.Warn("received bootstrap response from untracked node, ignore it",
			zap.String("id", b.id),
			zap.Any("nodeID", from))
		return nil
	}
	status.cachedBootstrapResp = msg
	status.state = nodeStateInitialized
	return b.collectInitialBootstrapResponses()
}

// ResendBootstrapMessage return message that need to be resent
func (b *Bootstrapper[T]) ResendBootstrapMessage() []*messaging.TargetMessage {
	if b.Bootstrapped() {
		return nil
	}

	var messages []*messaging.TargetMessage
	now := b.currentTime()
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	for id, status := range b.nodes {
		if status.state == nodeStateUninitialized &&
			now.Sub(status.lastBootstrapTime) >= b.resendInterval {
			messages = append(messages, b.newBootstrapMsg(id))
			status.lastBootstrapTime = now
		}
	}
	return messages
}

// GetAllNodeIDs return all node IDs tracked by bootstrapper
func (b *Bootstrapper[T]) GetAllNodeIDs() []node.ID {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	result := make([]node.ID, 0, len(b.nodes))
	for id := range b.nodes {
		result = append(result, id)
	}
	return result
}

func (b *Bootstrapper[T]) PrintBootstrapStatus() {
	bootstrappedNodes := make([]node.ID, 0)
	unbootstrappedNodes := make([]node.ID, 0)
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for id, status := range b.nodes {
		if status.state == nodeStateInitialized {
			bootstrappedNodes = append(bootstrappedNodes, id)
		} else {
			unbootstrappedNodes = append(unbootstrappedNodes, id)
		}
	}
	log.Info("bootstrap status",
		zap.String("changefeed", b.id),
		zap.Int("bootstrappedNodeCount", len(bootstrappedNodes)),
		zap.Int("unbootstrappedNodeCount", len(unbootstrappedNodes)),
		zap.Any("bootstrappedNodes", bootstrappedNodes),
		zap.Any("unbootstrappedNodes", unbootstrappedNodes),
	)
}

// Bootstrapped check if all nodes are initialized.
// returns true when all nodes report the bootstrap response and bootstrapped
func (b *Bootstrapper[T]) Bootstrapped() bool {
	return b.bootstrapped && b.checkAllNodeInitialized()
}

// return true if all nodes have reported bootstrap response
func (b *Bootstrapper[T]) checkAllNodeInitialized() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	if len(b.nodes) == 0 {
		return false
	}
	for _, status := range b.nodes {
		if status.state == nodeStateUninitialized {
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
		b.mutex.RLock()
		defer b.mutex.RUnlock()
		nodeBootstrapResponses := make(map[node.ID]*T, len(b.nodes))
		for _, status := range b.nodes {
			nodeBootstrapResponses[status.node.ID] = status.cachedBootstrapResp
			status.cachedBootstrapResp = nil
		}
		return nodeBootstrapResponses
	}
	return nil
}

type nodeState int

const (
	// nodeStateUninitialized means the node status is unknown,
	// no bootstrap response of this node received yet.
	nodeStateUninitialized nodeState = iota
	// nodeStateInitialized means bootstrapper has received the bootstrap response of this node.
	nodeStateInitialized
)

// nodeStatus represents the bootstrap state and metadata of a node in the system.
// It tracks initialization status, node information, cached bootstrap response,
// and timing data for bootstrap message retries.
type nodeStatus[T any] struct {
	state nodeState
	node  *node.Info
	// cachedBootstrapResp is the bootstrap response of this node.
	// It is cached when the bootstrap response is received.
	cachedBootstrapResp *T

	// lastBootstrapTime is the time when the bootstrap message is created for this node.
	// It approximates the time when we send the bootstrap message to the node.
	// It is used to limit the frequency of sending bootstrap message.
	lastBootstrapTime time.Time
}

func newNodeStatus[T any](node *node.Info) *nodeStatus[T] {
	return &nodeStatus[T]{
		state: nodeStateUninitialized,
		node:  node,
	}
}

type NewBootstrapMessageFn func(id node.ID) *messaging.TargetMessage
