//  Copyright 2023 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/fsutil"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/uuid"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
	"go.uber.org/mock/gomock"
)

func TestWriterWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	extStorage := newTestLocalExternalStorage(t, dir)
	cfs := []common.ChangeFeedID{
		common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName),
		common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Keyspace: "abcd",
			Name:     "test-cf",
		}),
	}

	cf11s := []common.ChangeFeedID{
		common.NewChangeFeedIDWithName("test-cf11", common.DefaultKeyspaceName),
		common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
			Keyspace: "abcd",
			Name:     "test-cf11",
		}),
	}

	for idx, cf := range cfs {
		largePayload := make([]byte, redo.Megabyte)
		uuidGen := uuid.NewConstGenerator("const-uuid")
		writerCfg := newTestWriterConfig(
			t,
			cf,
			&config.ConsistentConfig{
				MaxLogSize: util.AddressOf(int64(1)),
				Storage:    util.AddressOf("file://" + dir),
			},
		)
		w := &Writer{
			logType:   redo.RedoRowLogFileType,
			cfg:       writerCfg,
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: metrics.RedoWriteBytesGauge.
				WithLabelValues("default", "test-cf", redo.RedoRowLogFileType),
			metricFsyncDuration: metrics.RedoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf", redo.RedoRowLogFileType),
			metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf", redo.RedoRowLogFileType),
			storage:       extStorage,
			uuidGenerator: uuidGen,
		}

		w.eventCommitTS.Store(1)
		_, err := w.Write(largePayload)
		require.Nil(t, err)
		var fileName string
		// create a .tmp file
		if w.cfg.ChangeFeedID().Keyspace() == common.DefaultKeyspaceName {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Name(),
				w.logType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Keyspace(), w.cfg.ChangeFeedID().Name(),
				w.logType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path := filepath.Join(w.cfg.Dir(), fileName)
		info, err := os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())

		w.eventCommitTS.Store(12)
		_, err = w.Write([]byte("tt"))
		require.Nil(t, err)
		w.eventCommitTS.Store(22)
		_, err = w.Write([]byte("t"))
		require.Nil(t, err)

		// after rotate, rename to .log
		if w.cfg.ChangeFeedID().Keyspace() == common.DefaultKeyspaceName {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Name(),
				w.logType, 1, uuidGen.NewString(), redo.LogEXT)
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Keyspace(), w.cfg.ChangeFeedID().Name(),
				w.logType, 1, uuidGen.NewString(), redo.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir(), fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		// create a .tmp file with first eventCommitTS as name
		if w.cfg.ChangeFeedID().Keyspace() == common.DefaultKeyspaceName {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Name(),
				w.logType, 12, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Keyspace(), w.cfg.ChangeFeedID().Name(),
				w.logType, 12, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path = filepath.Join(w.cfg.Dir(), fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		err = w.Close()
		require.Nil(t, err)
		require.False(t, w.IsRunning())
		// safe close, rename to .log with max eventCommitTS as name
		if w.cfg.ChangeFeedID().Keyspace() == common.DefaultKeyspaceName {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Name(),
				w.logType, 22, uuidGen.NewString(), redo.LogEXT)
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID(),
				w.cfg.ChangeFeedID().Keyspace(), w.cfg.ChangeFeedID().Name(),
				w.logType, 22, uuidGen.NewString(), redo.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir(), fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())

		writerCfg11 := newTestWriterConfig(
			t,
			cf11s[idx],
			&config.ConsistentConfig{
				MaxLogSize: util.AddressOf(int64(1)),
				Storage:    util.AddressOf("file://" + dir),
			},
		)
		w1 := &Writer{
			logType:   redo.RedoRowLogFileType,
			cfg:       writerCfg11,
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: metrics.RedoWriteBytesGauge.
				WithLabelValues("default", "test-cf11", redo.RedoRowLogFileType),
			metricFsyncDuration: metrics.RedoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf11", redo.RedoRowLogFileType),
			metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf11", redo.RedoRowLogFileType),
			storage:       extStorage,
			uuidGenerator: uuidGen,
		}

		w1.eventCommitTS.Store(1)
		_, err = w1.Write(largePayload)
		require.Nil(t, err)
		// create a .tmp file
		if w1.cfg.ChangeFeedID().Keyspace() == common.DefaultKeyspaceName {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w1.cfg.CaptureID(),
				w1.cfg.ChangeFeedID().Name(),
				w1.logType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w1.cfg.CaptureID(),
				w1.cfg.ChangeFeedID().Keyspace(), w1.cfg.ChangeFeedID().Name(),
				w1.logType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path = filepath.Join(w1.cfg.Dir(), fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		// change the file name, should cause CLose err
		err = os.Rename(path, path+"new")
		require.Nil(t, err)
		err = w1.Close()
		require.NotNil(t, err)
		// closed anyway
		require.False(t, w1.IsRunning())
	}
}

func TestNewWriter(t *testing.T) {
	t.Parallel()

	storageDir := t.TempDir()
	dir := t.TempDir()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	writerCfg := newTestWriterConfig(
		t,
		common.NewChangeFeedIDWithName("test-row-writer", common.DefaultKeyspaceName),
		&config.ConsistentConfig{
			Storage: util.AddressOf("file://" + storageDir),
		},
	)
	w, err := NewFileWriter(context.Background(), writerCfg, redo.RedoRowLogFileType,
		writer.WithUUIDGenerator(func() uuid.Generator { return uuidGen }),
	)
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
	require.False(t, w.IsRunning())

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().WriteFile(gomock.Any(), fmt.Sprintf("%s_abcd_test_ddl_0_const-uuid.log", config.GetGlobalServerConfig().AdvertiseAddr),
		gomock.Any()).Return(nil).Times(1)

	changefeed := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
		Keyspace: "abcd",
		Name:     "test",
	})
	ddlWriterCfg := newTestWriterConfig(
		t,
		changefeed,
		&config.ConsistentConfig{
			Storage: util.AddressOf("file://" + dir),
		},
	)
	w = &Writer{
		logType:   redo.RedoDDLLogFileType,
		cfg:       ddlWriterCfg,
		uint64buf: make([]byte, 8),
		storage:   mockStorage,
		metricWriteBytes: metrics.RedoWriteBytesGauge.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		metricFsyncDuration: metrics.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)

	err = w.Close()
	require.Nil(t, err)
	require.Equal(t, w.running.Load(), false)
}

func TestRotateFileWithFileAllocator(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	mockStorage.EXPECT().WriteFile(gomock.Any(), fmt.Sprintf("%s_abcd_test_row_0_uuid-1.log", config.GetGlobalServerConfig().AdvertiseAddr),
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), fmt.Sprintf("%s_abcd_test_row_100_uuid-2.log", config.GetGlobalServerConfig().AdvertiseAddr),
		gomock.Any()).Return(nil).Times(1)

	dir := t.TempDir()
	uuidGen := uuid.NewMock()
	uuidGen.Push("uuid-1")
	uuidGen.Push("uuid-2")
	uuidGen.Push("uuid-3")
	uuidGen.Push("uuid-4")
	uuidGen.Push("uuid-5")
	changefeed := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
		Keyspace: "abcd",
		Name:     "test",
	})
	rowWriterCfg := newTestWriterConfig(
		t,
		changefeed,
		&config.ConsistentConfig{
			Storage: util.AddressOf("file://" + dir),
		},
	)
	w := &Writer{
		logType:   redo.RedoRowLogFileType,
		cfg:       rowWriterCfg,
		uint64buf: make([]byte, 8),
		metricWriteBytes: metrics.RedoWriteBytesGauge.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		metricFsyncDuration: metrics.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test", redo.RedoRowLogFileType),
		storage:       mockStorage,
		uuidGenerator: uuidGen,
	}
	w.allocator = fsutil.NewFileAllocator(
		w.cfg.Dir(), redo.RedoRowLogFileType, redo.DefaultMaxLogSize*redo.Megabyte)

	w.running.Store(true)
	_, err := w.Write([]byte("test"))
	require.Nil(t, err)

	err = w.rotate()
	require.Nil(t, err)

	w.AdvanceTs(100)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.rotate()
	require.Nil(t, err)

	w.Close()
}

func TestRotateFileWithoutFileAllocator(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	mockStorage.EXPECT().WriteFile(gomock.Any(), fmt.Sprintf("%s_abcd_test_ddl_0_uuid-2.log", config.GetGlobalServerConfig().AdvertiseAddr),
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), fmt.Sprintf("%s_abcd_test_ddl_100_uuid-4.log", config.GetGlobalServerConfig().AdvertiseAddr),
		gomock.Any()).Return(nil).Times(1)

	dir := t.TempDir()
	uuidGen := uuid.NewMock()
	uuidGen.Push("uuid-1")
	uuidGen.Push("uuid-2")
	uuidGen.Push("uuid-3")
	uuidGen.Push("uuid-4")
	uuidGen.Push("uuid-5")
	uuidGen.Push("uuid-6")
	changefeed := common.NewChangeFeedIDWithDisplayName(common.ChangeFeedDisplayName{
		Keyspace: "abcd",
		Name:     "test",
	})
	ddlWriterCfg := newTestWriterConfig(
		t,
		changefeed,
		&config.ConsistentConfig{
			Storage: util.AddressOf("file://" + dir),
		},
	)
	w := &Writer{
		logType:   redo.RedoDDLLogFileType,
		cfg:       ddlWriterCfg,
		uint64buf: make([]byte, 8),
		metricWriteBytes: metrics.RedoWriteBytesGauge.
			WithLabelValues("default", "test", redo.RedoDDLLogFileType),
		metricFsyncDuration: metrics.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test", redo.RedoDDLLogFileType),
		metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test", redo.RedoDDLLogFileType),
		storage:       mockStorage,
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	_, err := w.Write([]byte("test"))
	require.Nil(t, err)

	err = w.rotate()
	require.Nil(t, err)

	w.AdvanceTs(100)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.rotate()
	require.Nil(t, err)

	w.Close()
}

func TestRunFlushesOnBatchBoundaryAndExecutesPostFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	flushIntervalInMs := int64(60 * 1000)
	flushWorkerNum := 9
	batchWriterCfg := newTestWriterConfig(
		t,
		common.NewChangeFeedIDWithName("test-run-batch", common.DefaultKeyspaceName),
		&config.ConsistentConfig{
			FlushIntervalInMs: &flushIntervalInMs,
			FlushWorkerNum:    &flushWorkerNum,
			Storage:           util.AddressOf("file://" + dir),
		},
	)
	w, err := NewFileWriter(context.Background(), batchWriterCfg, redo.RedoRowLogFileType)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- w.Run(ctx)
	}()

	postFlushCnt := atomic.NewInt64(0)
	for i := 0; i < redo.DefaultFlushBatchSize-1; i++ {
		ts := uint64(i + 1)
		w.GetInputCh() <- &pevent.RedoRowEvent{
			StartTs:  ts,
			CommitTs: ts,
			Callback: func() {
				postFlushCnt.Inc()
			},
		}
	}

	// The callback should not be executed before the batch reaches the boundary.
	require.Equal(t, int64(0), postFlushCnt.Load())
	select {
	case err := <-runErrCh:
		require.Failf(t, "run exited unexpectedly", "run returned before cancel: %v", err)
	default:
	}

	ts := uint64(redo.DefaultFlushBatchSize)
	w.GetInputCh() <- &pevent.RedoRowEvent{
		StartTs:  ts,
		CommitTs: ts,
		Callback: func() {
			postFlushCnt.Inc()
		},
	}

	require.Eventually(t, func() bool {
		return postFlushCnt.Load() == int64(redo.DefaultFlushBatchSize)
	}, 10*time.Second, 20*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-runErrCh, context.Canceled)
	require.NoError(t, w.Close())
}
