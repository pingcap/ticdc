// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestEnqueueInitialRangeTaskDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &subscriptionClient{ctx: ctx, rangeTaskCh: make(chan rangeTask, 1)}
	span := &subscribedSpan{subID: 1, startTs: 100}
	task := rangeTask{span: heartbeatpb.TableSpan{TableID: 1}, subscribedSpan: span}

	client.enqueueInitialRangeTask(task, 100*time.Millisecond)
	select {
	case <-client.rangeTaskCh:
		t.Fatal("initial range task was enqueued before the delay")
	default:
	}
	select {
	case got := <-client.rangeTaskCh:
		require.Equal(t, task, got)
	case <-time.After(time.Second):
		t.Fatal("initial range task was not enqueued after the delay")
	}
}

func TestEnqueueInitialRangeTaskSkipsStoppedSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &subscriptionClient{ctx: ctx, rangeTaskCh: make(chan rangeTask, 1)}
	span := &subscribedSpan{subID: 1}
	client.enqueueInitialRangeTask(rangeTask{subscribedSpan: span}, 20*time.Millisecond)
	span.stopped.Store(true)

	select {
	case <-client.rangeTaskCh:
		t.Fatal("stopped subscription's range task was enqueued")
	case <-time.After(100 * time.Millisecond):
	}
}
