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
// See the License for the specific language governing permissions and
// limitations under the License.

package notifyqueue

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueuePushPopAndReady(t *testing.T) {
	q := New[int]()

	select {
	case <-q.Ready():
		t.Fatal("empty queue should not be ready")
	default:
	}

	q.Push(1)
	q.Push(2)
	require.Equal(t, 2, q.Len())

	select {
	case <-q.Ready():
	default:
		t.Fatal("push should signal ready")
	}

	v, ok := q.TryPop()
	require.True(t, ok)
	require.Equal(t, 1, v)
	v, ok = q.TryPop()
	require.True(t, ok)
	require.Equal(t, 2, v)
	require.Equal(t, 0, q.Len())

	_, ok = q.TryPop()
	require.False(t, ok)
}

func TestQueueReadyIsCoalesced(t *testing.T) {
	q := New[int]()

	q.Push(1)
	q.Push(2)

	select {
	case <-q.Ready():
	default:
		t.Fatal("push should signal ready")
	}

	select {
	case <-q.Ready():
		t.Fatal("ready signal should be coalesced")
	default:
	}
}

func TestQueueDrain(t *testing.T) {
	q := New[int]()
	q.Push(1)
	q.Push(2)
	q.Push(3)

	require.Equal(t, []int{1, 2, 3}, q.Drain())
	require.Equal(t, 0, q.Len())

	_, ok := q.TryPop()
	require.False(t, ok)
}
