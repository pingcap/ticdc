// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package dispatcher

import (
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
)

const blockEventWorkerCount = 8

type blockEventTask struct {
	dispatcher *BasicDispatcher
	event      commonEvent.BlockEvent
}
type blockEventExecutor struct {
	queues []*chann.UnlimitedChannel[blockEventTask, any]
	wg     sync.WaitGroup
}

func newBlockEventExecutor() *blockEventExecutor {
	executor := &blockEventExecutor{
		queues: make([]*chann.UnlimitedChannel[blockEventTask, any], blockEventWorkerCount),
	}
	for i := 0; i < blockEventWorkerCount; i++ {
		queue := chann.NewUnlimitedChannelDefault[blockEventTask]()
		executor.queues[i] = queue
		executor.wg.Add(1)
		go func(ch *chann.UnlimitedChannel[blockEventTask, any]) {
			defer executor.wg.Done()
			for {
				task, ok := ch.Get()
				if !ok {
					return
				}
				task.dispatcher.DealWithBlockEvent(task.event)
			}
		}(queue)
	}
	return executor
}

func (e *blockEventExecutor) Submit(dispatcher *BasicDispatcher, event commonEvent.BlockEvent) {
	idx := common.GID(dispatcher.id).Hash(uint64(len(e.queues)))
	e.queues[idx].Push(blockEventTask{dispatcher: dispatcher, event: event})
}

func (e *blockEventExecutor) Close() {
	if e == nil {
		return
	}
	for _, queue := range e.queues {
		queue.Close()
	}
	e.wg.Wait()
}
