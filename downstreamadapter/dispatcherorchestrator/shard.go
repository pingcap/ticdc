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

	wg sync.WaitGroup
}

func newOrchestratorShard(handle func(msg *messaging.TargetMessage)) *orchestratorShard {
	return &orchestratorShard{
		queue:  newPendingMessageQueue(),
		handle: handle,
	}
}

// Run starts the shard worker loop.
func (s *orchestratorShard) Run() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			msg, ok := s.queue.Pop()
			if !ok {
				return
			}
			s.handle(msg)
		}
	}()
}

// TryEnqueue keeps the existing queue semantics inside the shard, including
// de-duplication by (changefeedID, messageType).
func (s *orchestratorShard) TryEnqueue(key pendingMessageKey, msg *messaging.TargetMessage) bool {
	return s.queue.TryEnqueue(key, msg)
}

// Close stops the shard worker after the currently running handler returns.
func (s *orchestratorShard) Close() {
	s.queue.Close()
	s.wg.Wait()
}
