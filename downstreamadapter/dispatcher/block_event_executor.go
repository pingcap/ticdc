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
	"github.com/pingcap/ticdc/utils/chann"
)

const blockEventWorkerCount = 8

type blockEventTask struct {
	f func()
}

type blockEventExecutor struct {
	// ready contains dispatcher IDs that have pending tasks.
	//
	// We intentionally avoid hashing a dispatcher to a fixed worker queue. A slow downstream
	// operation (e.g. "ADD INDEX") would block the worker forever and cause head-of-line
	// blocking for other dispatchers mapped to the same queue. With this design, each dispatcher
	// can be processed by any idle worker.
	ready *chann.UnlimitedChannel[common.DispatcherID, any]

	// inUseDispatcher keeps track of dispatchers that are currently being processed by workers.
	inUseDispatcher sync.Map // map[common.DispatcherID]struct{}

	mu    sync.Mutex
	tasks map[common.DispatcherID][]blockEventTask

	wg sync.WaitGroup
}

func newBlockEventExecutor() *blockEventExecutor {
	executor := &blockEventExecutor{
		ready: chann.NewUnlimitedChannelDefault[common.DispatcherID](),
		tasks: make(map[common.DispatcherID][]blockEventTask),
	}
	for i := 0; i < blockEventWorkerCount; i++ {
		executor.wg.Add(1)
		go func() {
			defer executor.wg.Done()
			for {
				dispatcherID, ok := executor.ready.Get()
				if !ok {
					return
				}

				if _, loaded := executor.inUseDispatcher.Load(dispatcherID); loaded {
					// Another worker is already processing this dispatcher.
					// Re-enqueue the dispatcher ID and try later.
					//
					// dispatcher event ds ensures if a ddl task is not processed, there can't be new tasks from this dispatcher submitted.
					// So there is only one case there will be two task from same dispatchers in blockEventExecutor at the same time:
					// 1. worker 1 pop dispatcher A, start processing
					// 2. during processing, new task from dispatcher A is submitted, and push A to ready queue
					// Thus, we only need to check here, if this dispatcher is in process, we just re-push the dispatcher ID to the ready queue.
					executor.ready.Push(dispatcherID)
					continue
				}

				task, ok := executor.pop(dispatcherID)
				if !ok || task.f == nil {
					continue
				}

				executor.inUseDispatcher.Store(dispatcherID, struct{}{})
				task.f()
				executor.inUseDispatcher.Delete(dispatcherID)
			}
		}()
	}
	return executor
}

func (e *blockEventExecutor) Submit(dispatcher *BasicDispatcher, f func()) {
	dispatcherID := dispatcher.id

	e.mu.Lock()
	defer e.mu.Unlock()

	e.tasks[dispatcherID] = append(e.tasks[dispatcherID], blockEventTask{f: f})
	e.ready.Push(dispatcherID)
}

func (e *blockEventExecutor) pop(dispatcherID common.DispatcherID) (blockEventTask, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	q, ok := e.tasks[dispatcherID]
	if !ok || len(q) == 0 {
		delete(e.tasks, dispatcherID)
		return blockEventTask{}, false
	}

	task := q[0]
	if len(q) == 1 {
		delete(e.tasks, dispatcherID)
		return task, true
	}
	e.tasks[dispatcherID] = q[1:]
	return task, true
}

func (e *blockEventExecutor) Close() {
	if e == nil {
		return
	}

	e.ready.Close()
	e.wg.Wait()
}
