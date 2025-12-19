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
type Bootstrapper[T any] struct {
	// id is a log identifier
	id string

	mutex sync.Mutex
	// bootstrapped is true after get in touch with all nodes by receive their responses.
	bootstrapped bool
	nodes        map[node.ID]*node.Status[T]

	// newBootstrapRequest returns a new bootstrap message
	newBootstrapRequest NewBootstrapRequestFn
	resendInterval      time.Duration

	// For test only
	currentTime func() time.Time
}

// NewBootstrapper create a new bootstrapper for a distributed instance.
func NewBootstrapper[T any](id string, newBootstrapMsg NewBootstrapRequestFn) *Bootstrapper[T] {
	return &Bootstrapper[T]{
		id:                  id,
		nodes:               make(map[node.ID]*node.Status[T]),
		bootstrapped:        false,
		newBootstrapRequest: newBootstrapMsg,
		currentTime:         time.Now,
		resendInterval:      defaultResendInterval,
	}
}

// HandleNewNodes updates the bootstrapper with the current set of active nodes.
// It returns the IDs of newly added nodes, removed nodes, messages to be sent, and any cached bootstrap responses.
func (b *Bootstrapper[T]) HandleNewNodes(activeNodes map[node.ID]*node.Info) (
	addedNodes []node.ID,
	removedNodes []node.ID,
	messages []*messaging.TargetMessage,
	responses map[node.ID]*T,
) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for id, info := range activeNodes {
		if _, ok := b.nodes[id]; ok {
			continue
		}
		// A new node is found, send a bootstrap message to it.
		b.nodes[id] = node.NewStatus[T](info)
		messages = append(messages, b.newBootstrapRequest(id, info.AdvertiseAddr))
		b.nodes[id].SetLastBootstrapTime(b.currentTime())
		addedNodes = append(addedNodes, id)
	}

	if len(addedNodes) > 0 {
		b.bootstrapped = false
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

	responses = b.collectInitialBootstrapResponses()
	if len(responses) > 0 {
		b.bootstrapped = true
	}
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
	b.mutex.Lock()
	defer b.mutex.Unlock()
	status, ok := b.nodes[from]
	if !ok {
		log.Warn("received bootstrap response from untracked node, ignore it",
			zap.String("id", b.id),
			zap.Any("nodeID", from))
		return nil
	}
	status.SetResponse(msg)
	status.SetInitialized()

	responses := b.collectInitialBootstrapResponses()
	if len(responses) > 0 {
		b.bootstrapped = true
	}
	return responses
}

// ResendBootstrapMessage return message that need to be resent
func (b *Bootstrapper[T]) ResendBootstrapMessage() []*messaging.TargetMessage {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if b.bootstrapped {
		return nil
	}

	var messages []*messaging.TargetMessage
	now := b.currentTime()
	for id, status := range b.nodes {
		if !status.Initialized() &&
			now.Sub(status.GetLastBootstrapTime()) >= b.resendInterval {
			messages = append(messages, b.newBootstrapRequest(id, status.GetNodeInfo().AdvertiseAddr))
			status.SetLastBootstrapTime(now)
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
		if status.Initialized() {
			bootstrappedNodes = append(bootstrappedNodes, id)
		} else {
			unbootstrappedNodes = append(unbootstrappedNodes, id)
		}
	}
	log.Info("bootstrap status",
		zap.String("id", b.id),
		zap.Int("bootstrappedNodeCount", len(bootstrappedNodes)),
		zap.Int("unbootstrappedNodeCount", len(unbootstrappedNodes)),
		zap.Any("bootstrappedNodes", bootstrappedNodes),
		zap.Any("unbootstrappedNodes", unbootstrappedNodes),
	)
}

// Bootstrapped check if all nodes are initialized.
// returns true when all nodes report the bootstrap response and bootstrapped
func (b *Bootstrapper[T]) Bootstrapped() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.bootstrapped
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
// Note: this method must be called after lock.
func (b *Bootstrapper[T]) collectInitialBootstrapResponses() map[node.ID]*T {
	if b.bootstrapped {
		return nil
	}

	for _, status := range b.nodes {
		if !status.Initialized() {
			return nil
		}
	}

	responses := make(map[node.ID]*T, len(b.nodes))
	for _, status := range b.nodes {
		resp := status.GetResponse()
		if resp != nil {
			responses[status.GetNodeInfo().ID] = resp
		}
	}
	return responses
}

type NewBootstrapRequestFn func(id node.ID, addr string) *messaging.TargetMessage
