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

package dispatcherorchestrator

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/messaging"
)

// orchestratorShard is a FIFO worker shard for changefeed control messages.
// Each shard preserves the existing queue semantics inside the shard, while
// allowing different shards to execute concurrently.
type orchestratorShard struct {
	queue  *pendingMessageQueue
	handle func(msg *messaging.TargetMessage)

	wg        sync.WaitGroup
	runOnce   sync.Once
	closeOnce sync.Once
}

func newOrchestratorShard(handle func(msg *messaging.TargetMessage)) *orchestratorShard {
	return &orchestratorShard{
		queue:  newPendingMessageQueue(),
		handle: handle,
	}
}

// Run starts the shard worker loop.
func (s *orchestratorShard) Run() {
	s.runOnce.Do(func() {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				key, ok := s.queue.Pop()
				if !ok {
					return
				}
				msg := s.queue.Get(key)
				s.handle(msg)
				s.queue.Done(key)
			}
		}()
	})
}

// TryEnqueue keeps the existing queue semantics inside the shard, including
// de-duplication by (changefeedID, messageType).
func (s *orchestratorShard) TryEnqueue(key pendingMessageKey, msg *messaging.TargetMessage) bool {
	return s.queue.TryEnqueue(key, msg)
}

// CloseAsync signals the worker to stop after it drains the currently queued work.
// The call is idempotent so orchestrator shutdown can broadcast to every shard first
// and then wait for all workers in parallel.
func (s *orchestratorShard) CloseAsync() {
	s.closeOnce.Do(func() {
		s.queue.Close()
	})
}

// Wait blocks until the shard worker exits.
func (s *orchestratorShard) Wait() {
	s.wg.Wait()
}
