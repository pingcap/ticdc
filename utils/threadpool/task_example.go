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

package threadpool

import "time"

type OnceTask struct{}

func (t *OnceTask) Execute() time.Time {
	return time.Time{}
}

type RepeatedTask struct {
	taskHandle *TaskHandle
}

func NewRepeatedTask(ts ThreadPool) *RepeatedTask {
	t := &RepeatedTask{}
	t.taskHandle = ts.Submit(t, time.Now().Add(1*time.Second))
	return t
}

func (t *RepeatedTask) Execute() time.Time {
	return time.Now().Add(1 * time.Second)
}

func (t *RepeatedTask) Cancel() {
	t.taskHandle.Cancel()
}

func RunSomethingRepeatedly() {
	tp := NewThreadPoolDefault()

	times := 0
	tp.SubmitFunc(func() time.Time {
		// Do something 1000 times

		times++
		if times == 1000 {
			return time.Time{}
		} else {
			return time.Now().Add(1 * time.Nanosecond)
		}
	}, time.Now())

	tp.Stop()
}
