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

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newTestFileWorkerGroup(
	t *testing.T, inputCh chan *polymorphicRedoEvent, flushBatchSize int,
) *fileWorkerGroup {
	consistentCfg := testutil.NewConsistentConfig("blackhole://")
	consistentCfg.MaxLogSize = util.AddressOf(int64(1))
	consistentCfg.FlushIntervalInMs = util.AddressOf(int64(time.Hour / time.Millisecond))
	consistentCfg.FlushBatchSize = util.AddressOf(flushBatchSize)
	consistentCfg.FlushWorkerNum = util.AddressOf(1)
	cfg, err := writer.NewConfig(
		common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName),
		consistentCfg,
	)
	require.NoError(t, err)
	return newFileWorkerGroup(cfg, inputCh, nil)
}

// TestFileWorkerFlushesAtConfiguredBatchSize configures a three-row threshold,
// feeds exactly three encoded events, acknowledges the sealed file, and verifies
// that all callbacks run only after the count-based flush completes.
func TestFileWorkerFlushesAtConfiguredBatchSize(t *testing.T) {
	inputCh := make(chan *polymorphicRedoEvent)
	fileWorkers := newTestFileWorkerGroup(t, inputCh, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- fileWorkers.bgWriteLogs(ctx, inputCh)
	}()

	fileFlushedCh := make(chan struct{})
	go func() {
		file := <-fileWorkers.flushCh
		file.markFlushed()
		close(fileFlushedCh)
	}()

	postFlushCount := atomic.NewInt64(0)
	for i := 1; i <= 3; i++ {
		inputCh <- &polymorphicRedoEvent{
			commitTs: uint64(i),
			data:     []byte{byte(i)},
			callback: func() {
				postFlushCount.Inc()
			},
		}
	}

	require.Eventually(t, func() bool {
		return postFlushCount.Load() == 3
	}, time.Second, 10*time.Millisecond)
	<-fileFlushedCh

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
}

// TestFileWorkerDisablesCountBasedFlushWithZero feeds more rows than the old
// hard-coded limit while the ticker and file-size limit cannot fire. Processing
// must continue without waiting for a count-triggered file flush or callbacks.
func TestFileWorkerDisablesCountBasedFlushWithZero(t *testing.T) {
	const legacyFlushBatchSize = 1024

	inputCh := make(chan *polymorphicRedoEvent)
	fileWorkers := newTestFileWorkerGroup(t, inputCh, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- fileWorkers.bgWriteLogs(ctx, inputCh)
	}()

	postFlushCount := atomic.NewInt64(0)
	sendDoneCh := make(chan struct{})
	go func() {
		defer close(sendDoneCh)
		for i := 1; i <= legacyFlushBatchSize+2; i++ {
			select {
			case <-ctx.Done():
				return
			case inputCh <- &polymorphicRedoEvent{
				commitTs: uint64(i),
				data:     []byte{byte(i)},
				callback: func() {
					postFlushCount.Inc()
				},
			}:
			}
		}
	}()

	select {
	case <-sendDoneCh:
	case <-time.After(5 * time.Second):
		cancel()
		require.FailNow(t, "file worker blocked on a disabled count-based flush")
	}
	require.Zero(t, postFlushCount.Load())

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
}

// TestFileWorkerReleasesSizeRotatedCallbacksInOrder creates three events that
// each force the previous file to rotate, completes the second upload before
// the first, and verifies callbacks run in input order as the durable prefix
// advances. It also checks that invoked callback slots no longer retain their
// function values while the current unflushed file remains unacknowledged.
func TestFileWorkerReleasesSizeRotatedCallbacksInOrder(t *testing.T) {
	const eventSize = 600 * 1024

	inputCh := make(chan *polymorphicRedoEvent)
	fileWorkers := newTestFileWorkerGroup(t, inputCh, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- fileWorkers.bgWriteLogs(ctx, inputCh)
	}()

	callbackOrder := make(chan int, 3)
	for i := 1; i <= 3; i++ {
		index := i
		inputCh <- &polymorphicRedoEvent{
			commitTs: uint64(i),
			data:     make([]byte, eventSize),
			callback: func() {
				callbackOrder <- index
			},
		}
	}

	firstFile := <-fileWorkers.flushCh
	secondFile := <-fileWorkers.flushCh
	firstCallbackSlots := firstFile.postFlushCallbacks
	secondCallbackSlots := secondFile.postFlushCallbacks
	require.Len(t, firstCallbackSlots, 1)
	require.Len(t, secondCallbackSlots, 1)

	secondFile.markFlushed()
	require.Never(t, func() bool {
		return len(callbackOrder) != 0
	}, 100*time.Millisecond, 10*time.Millisecond)

	firstFile.markFlushed()
	select {
	case index := <-callbackOrder:
		require.Equal(t, 1, index)
	case <-time.After(time.Second):
		require.FailNow(t, "first rotated file callback was not released")
	}
	select {
	case index := <-callbackOrder:
		require.Equal(t, 2, index)
	case <-time.After(time.Second):
		require.FailNow(t, "second rotated file callback was not released")
	}

	require.Nil(t, firstCallbackSlots[0])
	require.Nil(t, secondCallbackSlots[0])
	require.Empty(t, callbackOrder)

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
}
