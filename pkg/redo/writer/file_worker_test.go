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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
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

func TestFileWorkerFlushBarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	defer extStorage.Close()

	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	consistentCfg := testutil.NewConsistentConfig(uri.String())
	flushInterval := int64(time.Hour / time.Millisecond)
	consistentCfg.FlushIntervalInMs = util.AddressOf(flushInterval)
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	inputCh := make(chan *polymorphicRedoEvent, 2)
	worker := newFileWorkerGroup(cfg, inputCh, extStorage)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx)
	}()

	var flushed atomic.Int64
	inputCh <- &polymorphicRedoEvent{
		commitTs:  1,
		data:      []byte("redo"),
		postFlush: func() { flushed.Add(1) },
	}
	barrier := make(chan error, 1)
	inputCh <- &polymorphicRedoEvent{flushBarrier: barrier}

	select {
	case err := <-barrier:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for redo flush barrier")
	}
	require.Equal(t, int64(1), flushed.Load())

	var fileCount int
	err = extStorage.WalkDir(ctx, nil, func(_ string, _ int64) error {
		fileCount++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, fileCount)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestFileWorkerBusyRatioRecordsActualWork(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	consistentCfg := testutil.NewConsistentConfig("nfs:///tmp/redo")
	flushInterval := int64(time.Hour / time.Millisecond)
	consistentCfg.FlushIntervalInMs = util.AddressOf(flushInterval)
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	inputCh := make(chan *polymorphicRedoEvent, 1)
	worker := newFileWorkerGroup(cfg, inputCh, nil)
	busyTime := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_redo_worker_busy_seconds"})
	worker.metricBusyRatio = busyTime
	defer worker.close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- worker.bgWriteLogs(ctx, inputCh)
	}()

	inputCh <- &polymorphicRedoEvent{commitTs: 1, data: []byte("redo")}
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(busyTime) > 0
	}, time.Second, time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
