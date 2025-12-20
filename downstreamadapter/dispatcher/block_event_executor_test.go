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

package dispatcher

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestBlockEventExecutorDoesNotBlockOtherDispatchers(t *testing.T) {
	executor := newBlockEventExecutor()
	t.Cleanup(executor.Close)

	// These IDs would hash to the same shard when mod=8, which used to introduce head-of-line blocking.
	d1 := &BasicDispatcher{id: common.DispatcherID{Low: 1, High: 0}}
	d2 := &BasicDispatcher{id: common.DispatcherID{Low: 9, High: 0}}

	firstStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	secondDone := make(chan struct{})

	executor.Submit(d1, func() {
		close(firstStarted)
		<-unblockFirst
	})
	require.Eventually(t, func() bool {
		select {
		case <-firstStarted:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	executor.Submit(d2, func() {
		close(secondDone)
	})
	require.Eventually(t, func() bool {
		select {
		case <-secondDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	close(unblockFirst)
}
