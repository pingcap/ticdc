// Copyright 2026 PingCAP, Inc.
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

package spool

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newTestMessage(value string, rows int) *common.Message {
	msg := common.NewMsg(nil, []byte(value))
	msg.SetRowsCount(rows)
	return msg
}

func TestSuppressAndResumeWake(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	options := &options{
		quotaBytes:         1000,
		rootDir:            t.TempDir(),
		segmentBytes:       1 << 20,
		memoryRatio:        0.99,
		highWatermarkRatio: 0.6,
		lowWatermarkRatio:  0.3,
	}
	manager, err := New(
		changefeedID,
		WithQuotaBytes(options.quotaBytes),
		WithRootDir(options.rootDir),
		WithSegmentBytes(options.segmentBytes),
		WithMemoryRatio(options.memoryRatio),
		WithHighWatermarkRatio(options.highWatermarkRatio),
		WithLowWatermarkRatio(options.lowWatermarkRatio),
	)
	require.NoError(t, err)
	defer manager.Close()

	var wakeCount int64
	incWake := func() {
		atomic.AddInt64(&wakeCount, 1)
	}

	entry1, err := manager.Enqueue([]*common.Message{
		newTestMessage(string(make([]byte, 350)), 1),
	}, incWake)
	require.NoError(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	entry2, err := manager.Enqueue([]*common.Message{
		newTestMessage(string(make([]byte, 450)), 1),
	}, incWake)
	require.NoError(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStoragePendingPostEnqueueGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStoragePostEnqueuePausedGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))

	manager.Release(entry1)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	manager.Release(entry2)
	require.Equal(t, int64(2), atomic.LoadInt64(&wakeCount))
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.CloudStoragePendingPostEnqueueGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.CloudStoragePostEnqueuePausedGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
}

func TestSpillAndReadBack(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	options := &options{
		quotaBytes:         64,
		rootDir:            t.TempDir(),
		segmentBytes:       1 << 20,
		memoryRatio:        0.01,
		highWatermarkRatio: 0.95,
		lowWatermarkRatio:  0.7,
	}
	manager, err := New(
		changefeedID,
		WithQuotaBytes(options.quotaBytes),
		WithRootDir(options.rootDir),
		WithSegmentBytes(options.segmentBytes),
		WithMemoryRatio(options.memoryRatio),
		WithHighWatermarkRatio(options.highWatermarkRatio),
		WithLowWatermarkRatio(options.lowWatermarkRatio),
	)
	require.NoError(t, err)
	defer manager.Close()

	msg := newTestMessage("hello-spool", 3)
	entry, err := manager.Enqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())
	require.False(t, entry.InMemory())
	require.Equal(t, uint64(len(msg.Value)), entry.FileBytes())

	msgs, callbacks, err := manager.Load(entry)
	require.NoError(t, err)
	require.Len(t, callbacks, 0)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("hello-spool"), msgs[0].Value)
	require.Equal(t, 3, msgs[0].GetRowsCount())

	manager.Release(entry)
}

func TestNewUsesDefaultOptionsWhenValuesAreMissing(t *testing.T) {
	t.Parallel()

	expectedQuotaBytes := int64(10 * 1024 * 1024 * 1024)
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, WithRootDir(t.TempDir()))
	require.NoError(t, err)
	require.NotNil(t, manager)
	require.Equal(t, defaultSegmentBytes, manager.segmentBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultMemoryRatio), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultHighWatermarkRatio), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultLowWatermarkRatio), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestInvalidWithOptionsKeepDefaults(t *testing.T) {
	t.Parallel()

	cfg := defaultOptions()
	WithQuotaBytes(-1)(cfg)
	WithSegmentBytes(-1)(cfg)
	WithMemoryRatio(-1)(cfg)
	WithHighWatermarkRatio(2)(cfg)
	WithLowWatermarkRatio(-1)(cfg)

	require.Equal(t, defaultQuotaBytes, cfg.quotaBytes)
	require.Equal(t, defaultSegmentBytes, cfg.segmentBytes)
	require.Equal(t, defaultMemoryRatio, cfg.memoryRatio)
	require.Equal(t, defaultHighWatermarkRatio, cfg.highWatermarkRatio)
	require.Equal(t, defaultLowWatermarkRatio, cfg.lowWatermarkRatio)
}

func TestEnqueueOnClosedSpool(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(1024),
		WithRootDir(t.TempDir()),
	)
	require.NoError(t, err)

	manager.Close()

	entry, err := manager.Enqueue([]*common.Message{newTestMessage("v", 1)}, nil)
	require.Nil(t, entry)
	require.Error(t, err)
	require.True(t, errors.ErrInternalCheckFailed.Equal(err))
}

func TestEnqueueErrorDoesNotMutateInputMessage(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(1024),
		WithRootDir(t.TempDir()),
	)
	require.NoError(t, err)

	manager.Close()

	callbackCalled := atomic.Bool{}
	msg := newTestMessage("v", 1)
	msg.Callback = func() {
		callbackCalled.Store(true)
	}

	entry, err := manager.Enqueue([]*common.Message{msg}, nil)
	require.Nil(t, entry)
	require.Error(t, err)
	require.True(t, errors.ErrInternalCheckFailed.Equal(err))
	require.NotNil(t, msg.Callback)

	msg.Callback()
	require.True(t, callbackCalled.Load())
}

func TestNewSanitizesInvalidOptions(t *testing.T) {
	t.Parallel()

	expectedQuotaBytes := int64(10 * 1024 * 1024 * 1024)
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(-1),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(-1),
		WithMemoryRatio(-1),
		WithHighWatermarkRatio(0.6),
		WithLowWatermarkRatio(0.9),
	)
	require.NoError(t, err)
	require.NotNil(t, manager)
	require.Equal(t, defaultSegmentBytes, manager.segmentBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultMemoryRatio), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultHighWatermarkRatio), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultLowWatermarkRatio), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestNewAppliesFunctionalOptions(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithRootDir(rootDir),
		WithQuotaBytes(2048),
		WithSegmentBytes(4096),
		WithMemoryRatio(0.25),
		WithHighWatermarkRatio(0.75),
		WithLowWatermarkRatio(0.5),
	)
	require.NoError(t, err)
	require.Equal(t, rootDir, manager.rootDir)
	require.Equal(t, int64(4096), manager.segmentBytes)
	require.Equal(t, int64(512), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(1536), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(1024), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestPrepareRootDirOnlyRemovesSegmentLogs(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	segmentPath := filepath.Join(rootDir, "segment-000001.log")
	segmentBackupPath := filepath.Join(rootDir, "segment-backup.log")
	keepPath := filepath.Join(rootDir, "keep.txt")

	require.NoError(t, os.WriteFile(segmentPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(segmentBackupPath, []byte("keep"), 0o644))
	require.NoError(t, os.WriteFile(keepPath, []byte("keep"), 0o644))

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(1024),
		WithRootDir(rootDir),
	)
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(segmentBackupPath)
	require.NoError(t, err)
	_, err = os.Stat(keepPath)
	require.NoError(t, err)
}

func TestCloseOnlyRemovesSegmentLogs(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	keepPath := filepath.Join(rootDir, "keep.txt")
	require.NoError(t, os.WriteFile(keepPath, []byte("keep"), 0o644))

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(32),
		WithRootDir(rootDir),
	)
	require.NoError(t, err)

	entry, err := manager.Enqueue([]*common.Message{
		newTestMessage("spill-me", 1),
	}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())

	segmentPath := filepath.Join(rootDir, "segment-000001.log")
	_, err = os.Stat(segmentPath)
	require.NoError(t, err)

	manager.Close()

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(keepPath)
	require.NoError(t, err)
}

func TestRemoveSpoolFilesIgnoresMissingDir(t *testing.T) {
	t.Parallel()

	rootDir := filepath.Join(t.TempDir(), "missing")
	err := removeSpoolFiles(rootDir)
	require.NoError(t, err)
}

func TestEnqueueSpillsWhenSerializedBatchExceedsMemoryQuota(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(64),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(1<<20),
		WithMemoryRatio(0.2),
		WithHighWatermarkRatio(0.9),
		WithLowWatermarkRatio(0.6),
	)
	require.NoError(t, err)
	defer manager.Close()

	msg := newTestMessage("a", 1)
	entry, err := manager.Enqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())
}

func TestExternalRootDirDeletionReturnsErrorWhenNewSegmentIsNeeded(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(64),
		WithRootDir(rootDir),
		WithSegmentBytes(40),
		WithMemoryRatio(0.01),
		WithHighWatermarkRatio(0.95),
		WithLowWatermarkRatio(0.7),
	)
	require.NoError(t, err)
	defer manager.Close()

	firstEntry, err := manager.Enqueue([]*common.Message{
		newTestMessage("first-spilled-entry", 1),
	}, nil)
	require.NoError(t, err)
	require.True(t, firstEntry.IsSpilled())

	require.NoError(t, os.RemoveAll(rootDir))

	// Removing the directory is not immediately fatal if the existing spilled
	// entry is still readable through the open segment file handle.
	firstMsgs, _, err := manager.Load(firstEntry)
	require.NoError(t, err)
	require.Len(t, firstMsgs, 1)
	require.Equal(t, []byte("first-spilled-entry"), firstMsgs[0].Value)

	// The error surfaces when spool next needs the damaged path to persist a new
	// pending entry in another segment.
	secondEntry, err := manager.Enqueue([]*common.Message{
		newTestMessage("second-spilled-entry", 1),
	}, nil)
	require.Nil(t, secondEntry)
	require.Error(t, err)
	rfcCode, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrUnexpected.RFCCode(), rfcCode)
}

func TestExternalSegmentDamageReturnsErrorOnLoad(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(64),
		WithRootDir(rootDir),
		WithSegmentBytes(1<<20),
		WithMemoryRatio(0.01),
		WithHighWatermarkRatio(0.95),
		WithLowWatermarkRatio(0.7),
	)
	require.NoError(t, err)
	defer manager.Close()

	entry, err := manager.Enqueue([]*common.Message{
		newTestMessage("spilled-entry", 1),
	}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())

	segmentPath := filepath.Join(rootDir, "segment-000001.log")
	require.NoError(t, os.Truncate(segmentPath, 0))

	msgs, callbacks, err := manager.Load(entry)
	require.Nil(t, msgs)
	require.Nil(t, callbacks)
	require.Error(t, err)
	rfcCode, ok := errors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, errors.ErrUnexpected.RFCCode(), rfcCode)
}

func TestDiscardRunsCallbacksOnce(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(1024),
		WithRootDir(t.TempDir()),
	)
	require.NoError(t, err)
	defer manager.Close()

	var callbackCount atomic.Int64
	msg := newTestMessage("v", 1)
	msg.Callback = func() {
		callbackCount.Add(1)
	}

	entry, err := manager.Enqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)

	manager.Discard(entry)
	manager.Discard(entry)

	require.Equal(t, int64(1), callbackCount.Load())
	require.False(t, entry.InMemory())
	require.False(t, entry.IsSpilled())
	require.Zero(t, entry.FileBytes())
}

func TestWaitForDiskQuotaReturnsAfterRelease(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(40),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(1<<20),
		WithMemoryRatio(0.01),
		WithHighWatermarkRatio(0.95),
		WithLowWatermarkRatio(0.7),
	)
	require.NoError(t, err)
	defer manager.Close()

	entry, err := manager.Enqueue([]*common.Message{newTestMessage("first-entry", 1)}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- manager.WaitForDiskQuota(context.Background(), []*common.Message{newTestMessage("second-entry", 1)})
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("wait returned too early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	manager.Release(entry)

	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("wait for disk quota did not return after release")
	}
}

func TestSpoolMetricsAndCleanup(t *testing.T) {
	changefeedID := commonType.NewChangefeedID4Test("test", "spool-metrics")
	manager, err := New(
		changefeedID,
		WithQuotaBytes(64),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(40),
		WithMemoryRatio(0.01),
	)
	require.NoError(t, err)

	entry, err := manager.Enqueue([]*common.Message{newTestMessage("first-entry", 1)}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStorageSpillCountCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStorageSpoolSegmentGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
	require.Greater(t, promtestutil.ToFloat64(metrics.CloudStorageSpillBytesCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())), float64(0))

	msgs, _, err := manager.Load(entry)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStorageLoadCountCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())))
	require.Greater(t, promtestutil.ToFloat64(metrics.CloudStorageLoadBytesCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String())), float64(0))

	manager.Close()

	require.False(t, metrics.CloudStorageSpillCountCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStorageSpillBytesCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStorageLoadCountCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStorageLoadBytesCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStorageSpoolSegmentGauge.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStoragePendingPostEnqueueGauge.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
	require.False(t, metrics.CloudStoragePostEnqueuePausedGauge.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()))
}
