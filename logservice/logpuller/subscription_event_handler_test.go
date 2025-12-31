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

package logpuller

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionEventHandlerHandleAwaitWakeContract(t *testing.T) {
	t.Parallel()

	var (
		mu         sync.Mutex
		calls      []string
		wakeSubID  SubscriptionID
		wakeCalled bool

		advanceCalled bool
		advancedTs    uint64

		finishCallback func()
		consumed       []common.RawKVEntry
	)

	h := &subscriptionEventHandler{
		wake: func(id SubscriptionID) {
			mu.Lock()
			defer mu.Unlock()
			wakeCalled = true
			wakeSubID = id
			calls = append(calls, "wake")
		},
	}

	span := &subscribedSpan{
		subID: SubscriptionID(100),
		consumeKVEvents: func(events []common.RawKVEntry, wakeCallback func()) bool {
			mu.Lock()
			defer mu.Unlock()
			consumed = append(consumed, events...)
			finishCallback = wakeCallback
			return true // async
		},
		advanceResolvedTs: func(ts uint64) {
			mu.Lock()
			defer mu.Unlock()
			advanceCalled = true
			advancedTs = ts
			calls = append(calls, "advance")
		},
	}

	events := []subscriptionEvent{
		{subID: span.subID, kvEntries: []common.RawKVEntry{{CRTs: 11}, {CRTs: 12}}},
		{subID: span.subID, resolvedTs: 20},
		{subID: span.subID, kvEntries: []common.RawKVEntry{{CRTs: 13}}},
		{subID: span.subID, resolvedTs: 19}, // lower than max, should be ignored
	}

	await := h.Handle(span, events...)
	require.True(t, await)

	mu.Lock()
	require.NotNil(t, finishCallback)
	require.False(t, wakeCalled)
	require.False(t, advanceCalled)
	mu.Unlock()

	// pendingKVEntries should remain until async completion.
	require.Len(t, span.pendingKVEntries, 3)

	mu.Lock()
	require.Len(t, consumed, 3)
	mu.Unlock()

	finishCallback()

	require.Len(t, span.pendingKVEntries, 0)

	mu.Lock()
	require.True(t, advanceCalled)
	require.Equal(t, uint64(20), advancedTs)
	require.True(t, wakeCalled)
	require.Equal(t, span.subID, wakeSubID)
	require.Equal(t, []string{"advance", "wake"}, calls)
	mu.Unlock()
}
