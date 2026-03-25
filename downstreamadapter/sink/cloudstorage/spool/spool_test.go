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

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
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
		diskQuotaBytes:     1000,
		rootDir:            t.TempDir(),
		segmentCapacity:    1 << 20,
		memoryRatio:        0.99,
		highWatermarkRatio: 0.6,
		lowWatermarkRatio:  0.3,
	}
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(options.diskQuotaBytes),
		WithRootDir(options.rootDir),
		WithSegmentBytes(options.segmentCapacity),
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

	manager.Release(entry1)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	manager.Release(entry2)
	require.Equal(t, int64(2), atomic.LoadInt64(&wakeCount))
}

func TestSpillAndReadBack(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	options := &options{
		diskQuotaBytes:     64,
		rootDir:            t.TempDir(),
		segmentCapacity:    1 << 20,
		memoryRatio:        0.01,
		highWatermarkRatio: 0.95,
		lowWatermarkRatio:  0.7,
	}
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(options.diskQuotaBytes),
		WithRootDir(options.rootDir),
		WithSegmentBytes(options.segmentCapacity),
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

	msgs, postFlushCallbacks, err := manager.Load(entry)
	require.NoError(t, err)
	require.Len(t, postFlushCallbacks, 0)
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
	require.Equal(t, defaultSegmentCapacity, manager.segmentCapacity)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultMemoryRatio), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultHighWatermarkRatio), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultLowWatermarkRatio), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestEnqueueOnClosedSpool(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(1024),
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
		WithDiskQuotaBytes(1024),
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
		WithDiskQuotaBytes(-1),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(-1),
		WithMemoryRatio(-1),
		WithHighWatermarkRatio(0.6),
		WithLowWatermarkRatio(0.9),
	)
	require.NoError(t, err)
	require.NotNil(t, manager)
	require.Equal(t, defaultSegmentCapacity, manager.segmentCapacity)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultMemoryRatio), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultHighWatermarkRatio), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(float64(expectedQuotaBytes)*defaultLowWatermarkRatio), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestNewAppliesFunctionalOptions(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithRootDir(baseDir),
		WithDiskQuotaBytes(2048),
		WithSegmentBytes(4096),
		WithMemoryRatio(0.25),
		WithHighWatermarkRatio(0.75),
		WithLowWatermarkRatio(0.5),
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(baseDir, changefeedID.Keyspace(), changefeedID.Name()), manager.workDir)
	require.Equal(t, int64(4096), manager.segmentCapacity)
	require.Equal(t, int64(512), manager.quota.budget.memoryQuotaBytes)
	require.Equal(t, int64(1536), manager.quota.budget.highWatermarkBytes)
	require.Equal(t, int64(1024), manager.quota.budget.lowWatermarkBytes)
	manager.Close()
}

func TestPrepareRootDirRecreatesOwnedChangefeedDir(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	rootDir := filepath.Join(baseDir, changefeedID.Keyspace(), changefeedID.Name())
	segmentPath := filepath.Join(rootDir, "segment-000001.log")
	keepInSpoolDir := filepath.Join(rootDir, "keep.txt")
	keepInBaseDir := filepath.Join(baseDir, "keep.txt")

	require.NoError(t, os.MkdirAll(rootDir, 0o755))
	require.NoError(t, os.WriteFile(segmentPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(keepInSpoolDir, []byte("remove"), 0o644))
	require.NoError(t, os.WriteFile(keepInBaseDir, []byte("keep"), 0o644))

	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(1024),
		WithRootDir(baseDir),
	)
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(keepInSpoolDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(keepInBaseDir)
	require.NoError(t, err)
}

func TestCloseRemovesOwnedChangefeedDir(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	keepPath := filepath.Join(baseDir, "keep.txt")
	require.NoError(t, os.WriteFile(keepPath, []byte("keep"), 0o644))
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(32),
		WithRootDir(baseDir),
	)
	require.NoError(t, err)

	entry, err := manager.Enqueue([]*common.Message{
		newTestMessage("spill-me", 1),
	}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())

	segmentPath := filepath.Join(manager.workDir, "segment-000001.log")
	_, err = os.Stat(segmentPath)
	require.NoError(t, err)

	manager.Close()

	_, err = os.Stat(segmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(manager.workDir)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(keepPath)
	require.NoError(t, err)
}

func TestEnqueueSpillsWhenSerializedBatchExceedsMemoryQuota(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(64),
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

	baseDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(64),
		WithRootDir(baseDir),
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

	require.NoError(t, os.RemoveAll(manager.workDir))

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

	baseDir := t.TempDir()
	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(64),
		WithRootDir(baseDir),
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

	segmentPath := filepath.Join(manager.workDir, "segment-000001.log")
	require.NoError(t, os.Truncate(segmentPath, 0))

	msgs, postFlushCallbacks, err := manager.Load(entry)
	require.Nil(t, msgs)
	require.Nil(t, postFlushCallbacks)
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
		WithDiskQuotaBytes(1024),
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
		WithDiskQuotaBytes(40),
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

func TestWaitForDiskQuotaRemovesCanceledWaiter(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool-wait-cancel")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(40),
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

	waitCtx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- manager.WaitForDiskQuota(waitCtx, []*common.Message{newTestMessage("second-entry", 1)})
	}()

	require.Eventually(t, func() bool {
		manager.quota.waitersMu.Lock()
		defer manager.quota.waitersMu.Unlock()
		return len(manager.quota.waiters) == 1
	}, time.Second, 10*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-waitDone, context.Canceled)

	require.Eventually(t, func() bool {
		manager.quota.waitersMu.Lock()
		defer manager.quota.waitersMu.Unlock()
		return len(manager.quota.waiters) == 0
	}, time.Second, 10*time.Millisecond)
}

func TestTryEnqueueReturnsWaitActionWithoutMutatingMessage(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool-try-wait")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(40),
		WithRootDir(t.TempDir()),
		WithSegmentBytes(1<<20),
		WithMemoryRatio(0.01),
		WithHighWatermarkRatio(0.95),
		WithLowWatermarkRatio(0.7),
	)
	require.NoError(t, err)
	defer manager.Close()

	firstEntry, err := manager.Enqueue([]*common.Message{newTestMessage("first-entry", 1)}, nil)
	require.NoError(t, err)
	require.True(t, firstEntry.IsSpilled())

	msg := newTestMessage("second-entry", 1)
	callbackCalled := atomic.Bool{}
	msg.Callback = func() {
		callbackCalled.Store(true)
	}

	action, entry, err := manager.TryEnqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)
	require.Equal(t, EnqueueActionWaitDiskQuota, action)
	require.Nil(t, entry)
	require.NotNil(t, msg.Callback)

	msg.Callback()
	require.True(t, callbackCalled.Load())
}

func TestTryEnqueueReturnsOversizedActionWithInMemoryEntry(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool-try-oversized")
	manager, err := New(
		changefeedID,
		WithDiskQuotaBytes(1),
		WithRootDir(t.TempDir()),
		WithMemoryRatio(0.01),
	)
	require.NoError(t, err)
	defer manager.Close()

	action, entry, err := manager.TryEnqueue([]*common.Message{newTestMessage("oversized-entry", 1)}, nil)
	require.NoError(t, err)
	require.Equal(t, EnqueueActionAcceptedOversized, action)
	require.NotNil(t, entry)
	require.True(t, entry.InMemory())
	require.False(t, entry.IsSpilled())
}
