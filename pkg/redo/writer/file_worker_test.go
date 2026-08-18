// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFlushAllReleasesCallbacksPerCompletedFile(t *testing.T) {
	firstFlushed := make(chan struct{})
	secondFlushed := make(chan struct{})
	var firstCallback atomic.Int64
	var secondCallback atomic.Int64

	first := &fileCache{
		flushed:   firstFlushed,
		postFlush: []func(){func() { firstCallback.Add(1) }},
	}
	second := &fileCache{
		flushed:   secondFlushed,
		postFlush: []func(){func() { secondCallback.Add(1) }},
	}
	worker := &fileWorkerGroup{
		files:   []*fileCache{first, second},
		flushCh: make(chan *fileCache, 1),
	}

	done := make(chan error, 1)
	go func() {
		done <- worker.flushAll(context.Background())
	}()

	require.Same(t, second, <-worker.flushCh)
	close(firstFlushed)
	require.Eventually(t, func() bool {
		return firstCallback.Load() == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), secondCallback.Load())
	select {
	case err := <-done:
		require.FailNow(t, "flushAll returned before every file completed", "error: %v", err)
	default:
	}

	close(secondFlushed)
	require.NoError(t, <-done)
	require.Equal(t, int64(1), secondCallback.Load())
	require.Empty(t, worker.files)
}
