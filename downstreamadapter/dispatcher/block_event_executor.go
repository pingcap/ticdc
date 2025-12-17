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
				task, ok := executor.pop(dispatcherID)
				if !ok || task.f == nil {
					continue
				}
				task.f()
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
