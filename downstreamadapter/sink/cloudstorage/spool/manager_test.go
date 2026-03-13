// Copyright 2025 PingCAP, Inc.
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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
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
	options := &Options{
		QuotaBytes:         1000,
		RootDir:            t.TempDir(),
		SegmentBytes:       1 << 20,
		MemoryRatio:        0.99,
		HighWatermarkRatio: 0.6,
		LowWatermarkRatio:  0.3,
	}
	manager, err := New(changefeedID, options)
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
	options := &Options{
		QuotaBytes:         64,
		RootDir:            t.TempDir(),
		SegmentBytes:       1 << 20,
		MemoryRatio:        0.01,
		HighWatermarkRatio: 0.95,
		LowWatermarkRatio:  0.7,
	}
	manager, err := New(changefeedID, options)
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

func TestNewInvalidConfigError(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, &Options{RootDir: t.TempDir()})
	require.Nil(t, manager)
	require.Error(t, err)
	require.True(t, cerror.ErrStorageSinkInvalidConfig.Equal(err))
}

func TestEnqueueOnClosedManager(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, &Options{
		QuotaBytes: 1024,
		RootDir:    t.TempDir(),
	})
	require.NoError(t, err)

	manager.Close()

	entry, err := manager.Enqueue([]*common.Message{newTestMessage("v", 1)}, nil)
	require.Nil(t, entry)
	require.Error(t, err)
	require.True(t, cerror.ErrInternalCheckFailed.Equal(err))
}

func TestEnqueueErrorDoesNotMutateInputMessage(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, &Options{
		QuotaBytes: 1024,
		RootDir:    t.TempDir(),
	})
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
	require.True(t, cerror.ErrInternalCheckFailed.Equal(err))
	require.NotNil(t, msg.Callback)

	msg.Callback()
	require.True(t, callbackCalled.Load())
}

func TestNewRejectsNegativeMemoryRatio(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, &Options{
		QuotaBytes:  1024,
		RootDir:     t.TempDir(),
		MemoryRatio: -1,
	})
	require.Nil(t, manager)
	require.Error(t, err)
	require.True(t, cerror.ErrStorageSinkInvalidConfig.Equal(err))
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
	manager, err := New(changefeedID, &Options{
		QuotaBytes: 1024,
		RootDir:    rootDir,
	})
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
	manager, err := New(changefeedID, &Options{
		QuotaBytes: 32,
		RootDir:    rootDir,
	})
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
	manager, err := New(changefeedID, &Options{
		QuotaBytes:         64,
		RootDir:            t.TempDir(),
		SegmentBytes:       1 << 20,
		MemoryRatio:        0.2,
		HighWatermarkRatio: 0.9,
		LowWatermarkRatio:  0.6,
	})
	require.NoError(t, err)
	defer manager.Close()

	msg := newTestMessage("a", 1)
	entry, err := manager.Enqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())
}

func TestDiscardRunsCallbacksOnce(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	manager, err := New(changefeedID, &Options{
		QuotaBytes: 1024,
		RootDir:    t.TempDir(),
	})
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
