// Copyright 2022 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/pkg/clock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func testWriter(ctx context.Context, t *testing.T, dir string) *writer {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, uri)
	require.NoError(t, err)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	err = cfg.Apply(context.TODO(), sinkURI, replicaConfig.Sink, true)
	cfg.FileIndexWidth = 6
	require.NoError(t, err)

	changefeedID := commonType.NewChangefeedID4Test("test", t.Name())
	statistics := metrics.NewStatistics(changefeedID, t.Name())
	pdlock := pdutil.NewMonotonicClock(clock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	spoolBuffer := newTestSpool(t, changefeedID, cfg)
	d := newWriter(1, changefeedID, storage,
		cfg, ".json", statistics, spoolBuffer)
	return d
}

func newTestSpool(
	t *testing.T,
	changefeedID commonType.ChangeFeedID,
	cfg *cloudstorage.Config,
) *spool.Spool {
	spoolBuffer, err := spool.New(changefeedID, spool.WithDiskQuotaBytes(cfg.SpoolDiskQuota))
	require.NoError(t, err)
	t.Cleanup(spoolBuffer.Close)
	return spoolBuffer
}

func hasSpoolLogFile(spoolDir string) bool {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		return false
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if path.Ext(entry.Name()) == ".log" {
			return true
		}
	}
	return false
}

func TestWriterRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
	table1Dir := path.Join(parentDir, "test/table1/99")

	tidbTableInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: ast.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)

	dispatcherID := commonType.NewDispatcherID()
	for i := 0; i < 5; i++ {
		tableName := cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherID,
		}
		dmlEvent := &commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
			Rows:            chunk.MutRowFromValues(100, "hello world").ToRow().Chunk(),
		}
		tableTask := newDMLTask(tableName, dmlEvent)
		tableTask.encodedMsgs = []*common.Message{
			{
				Value: []byte(fmt.Sprintf(`{"id":%d,"database":"test","table":"table1","pkNames":[],"isDdl":false,`+
					`"type":"INSERT","es":0,"ts":1663572946034,"sql":"","sqlType":{"c1":12,"c2":12},`+
					`"data":[{"c1":"100","c2":"hello world"}],"old":null}`, i)),
			},
		}
		require.NoError(t, d.enqueueTask(ctx, tableTask))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.run(ctx)
	}()

	require.Eventually(t, func() bool {
		files, err := os.ReadDir(table1Dir)
		return err == nil && len(files) == 2
	}, 10*time.Second, 100*time.Millisecond)

	// check whether files for table1 has been generated
	fileNames := getTableFiles(t, table1Dir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{fmt.Sprintf("CDC_%s_000001.json", dispatcherID.String()), fmt.Sprintf("CDC_%s.index", dispatcherID.String())}, fileNames)
	cancel()
	wg.Wait()
}

func TestWriterFlushMarker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)

	tidbTableInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)
	dispatcherID := commonType.NewDispatcherID()

	var callbackCnt atomic.Int64
	msg := common.NewMsg(nil, []byte(`{"id":1}`))
	msg.SetRowsCount(1)
	msg.Callback = func() {
		callbackCnt.Add(1)
	}

	tableTask := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherID,
		},
		&commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
		},
	)
	tableTask.encodedMsgs = []*common.Message{msg}
	require.NoError(t, d.enqueueTask(ctx, tableTask))

	flushTask := newFlushTask(dispatcherID, 100)
	require.NoError(t, d.enqueueTask(ctx, flushTask))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.run(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	require.NoError(t, flushTask.wait(waitCtx))
	require.Eventually(t, func() bool {
		return callbackCnt.Load() == 1
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	wg.Wait()
}

func TestWriterFlushMarkerOnlyFlushesTargetDispatcher(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
	d.config.FlushInterval = time.Hour

	tidbTableInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)

	dispatcherA := commonType.NewDispatcherID()
	dispatcherB := commonType.NewDispatcherID()

	var callbackA atomic.Int64
	var callbackB atomic.Int64

	taskA := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherA,
		},
		&commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
		},
	)
	msgA := common.NewMsg(nil, []byte(`{"id":"a"}`))
	msgA.SetRowsCount(1)
	msgA.Callback = func() {
		callbackA.Add(1)
	}
	taskA.encodedMsgs = []*common.Message{msgA}

	taskB := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table2",
				TableID: 101,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherB,
		},
		&commonEvent.DMLEvent{
			PhysicalTableID: 101,
			TableInfo: commonType.WrapTableInfo("test", &model.TableInfo{
				ID:   101,
				Name: ast.NewCIStr("table2"),
				Columns: []*model.ColumnInfo{
					{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			}),
		},
	)
	msgB := common.NewMsg(nil, []byte(`{"id":"b"}`))
	msgB.SetRowsCount(1)
	msgB.Callback = func() {
		callbackB.Add(1)
	}
	taskB.encodedMsgs = []*common.Message{msgB}

	require.NoError(t, d.enqueueTask(ctx, taskA))
	require.NoError(t, d.enqueueTask(ctx, taskB))

	flushTask := newFlushTask(dispatcherA, 100)
	require.NoError(t, d.enqueueTask(ctx, flushTask))

	done := make(chan error, 1)
	go func() {
		done <- d.run(ctx)
	}()

	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()
	require.NoError(t, flushTask.wait(waitCtx))
	require.Eventually(t, func() bool {
		return callbackA.Load() == 1
	}, time.Second, 50*time.Millisecond)
	require.Equal(t, int64(0), callbackB.Load())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWriterPostEnqueueAfterConsume(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)

	tidbTableInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)
	dispatcherID := commonType.NewDispatcherID()

	dmlEvent := &commonEvent.DMLEvent{
		PhysicalTableID: 100,
		TableInfo:       tableInfo,
	}
	var enqueueCnt atomic.Int64
	dmlEvent.AddPostEnqueueFunc(func() {
		enqueueCnt.Add(1)
	})

	tableTask := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherID,
		},
		dmlEvent,
	)
	tableTask.encodedMsgs = []*common.Message{
		{
			Value: []byte(`{"id":1}`),
		},
	}

	require.NoError(t, d.enqueueTask(ctx, tableTask))
	require.Equal(t, int64(0), enqueueCnt.Load())

	done := make(chan error, 1)
	go func() {
		done <- d.run(ctx)
	}()

	require.Eventually(t, func() bool {
		return enqueueCnt.Load() == 1
	}, 5*time.Second, 100*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWriterStoresPendingMessagesInSpoolBeforeFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parentDir := t.TempDir()
	dataDir := t.TempDir()

	oldServerCfg := *config.GetGlobalServerConfig()
	serverCfg := oldServerCfg
	serverCfg.DataDir = dataDir
	config.StoreGlobalServerConfig(&serverCfg)
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(&oldServerCfg)
	})

	uri := fmt.Sprintf("file:///%s?flush-interval=1h", parentDir)
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, uri)
	require.NoError(t, err)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	replicaConfig.Sink.CloudStorageConfig = &config.CloudStorageConfig{
		// Keep the quota larger than this encoded batch so the controller still
		// spills it to local spool files instead of taking the oversized in-memory fast path.
		SpoolDiskQuota: util.AddressOf(int64(32)),
	}
	err = cfg.Apply(context.Background(), sinkURI, replicaConfig.Sink, true)
	require.NoError(t, err)
	cfg.FileIndexWidth = 6
	cfg.FlushInterval = time.Hour

	changefeedID := commonType.NewChangefeedID4Test("test", "spool-pending")
	statistics := metrics.NewStatistics(changefeedID, t.Name())
	pdlock := pdutil.NewMonotonicClock(clock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)

	spoolBuffer := newTestSpool(t, changefeedID, cfg)
	d := newWriter(1, changefeedID, storage, cfg, ".json", statistics, spoolBuffer)

	tidbTableInfo := &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)
	dispatcherID := commonType.NewDispatcherID()

	tableTask := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherID,
		},
		&commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
		},
	)
	msg := common.NewMsg(nil, []byte(`{"id":1}`))
	msg.SetRowsCount(1)
	tableTask.encodedMsgs = []*common.Message{msg}
	require.NoError(t, d.enqueueTask(ctx, tableTask))

	done := make(chan error, 1)
	go func() {
		done <- d.run(ctx)
	}()

	spoolDir := path.Join(
		dataDir,
		"cloudstorage-sink-spool",
		changefeedID.Keyspace(),
		changefeedID.Name(),
	)
	require.Eventually(t, func() bool {
		return hasSpoolLogFile(spoolDir)
	}, 5*time.Second, 50*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestDiscardPayloadDoesNotLoadSpilledPayload(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()

	oldServerCfg := *config.GetGlobalServerConfig()
	serverCfg := oldServerCfg
	serverCfg.DataDir = dataDir
	config.StoreGlobalServerConfig(&serverCfg)
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(&oldServerCfg)
	})

	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
	d.spool = newTestSpool(t, d.changeFeedID, &cloudstorage.Config{
		SpoolDiskQuota: 1,
	})

	callbackCount := atomic.Int64{}
	msg := common.NewMsg(nil, []byte(`{"id":1}`))
	msg.SetRowsCount(1)
	msg.Callback = func() {
		callbackCount.Add(1)
	}

	entry, err := d.spool.Enqueue([]*common.Message{msg}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())

	d.spool.Close()

	d.discardPayload(&payload{
		entries: []*spool.Entry{entry},
	})
	require.Equal(t, int64(1), callbackCount.Load())
}

func TestWriterRunExitAfterContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)

	done := make(chan error, 1)
	go func() {
		done <- d.run(ctx)
	}()

	cause := errors.New("writer canceled")
	cancel(cause)

	select {
	case err := <-done:
		require.ErrorIs(t, err, cause)
	case <-time.After(5 * time.Second):
		t.Fatal("writer.run did not exit after context cancel")
	}
}

type failOnIndexStorage struct {
	storage.ExternalStorage
}

func (s *failOnIndexStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if strings.HasSuffix(name, ".index") {
		return errors.New("index write failed")
	}
	return s.ExternalStorage.WriteFile(ctx, name, data)
}

func TestWriterIndexWriteError(t *testing.T) {
	ctx := context.Background()
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", parentDir)
	baseStorage, err := util.GetExternalStorageWithDefaultTimeout(ctx, uri)
	require.NoError(t, err)
	storage := &failOnIndexStorage{ExternalStorage: baseStorage}

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	err = cfg.Apply(context.TODO(), sinkURI, replicaConfig.Sink, true)
	require.NoError(t, err)
	cfg.FileIndexWidth = 6
	cfg.FlushInterval = time.Hour

	changefeedID := commonType.NewChangefeedID4Test("test", "writer-error-metric")
	statistics := metrics.NewStatistics(changefeedID, t.Name())
	pdlock := pdutil.NewMonotonicClock(clock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	spoolBuffer := newTestSpool(t, changefeedID, cfg)
	d := newWriter(1, changefeedID, storage, cfg, ".json", statistics, spoolBuffer)

	tableInfo := commonType.WrapTableInfo("test", &model.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	})
	dispatcherID := commonType.NewDispatcherID()
	task := newDMLTask(
		cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 99,
			DispatcherID:     dispatcherID,
		},
		&commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
		},
	)
	msg := common.NewMsg(nil, []byte(`{"id":1}`))
	msg.SetRowsCount(1)
	task.encodedMsgs = []*common.Message{msg}
	require.NoError(t, d.enqueueTask(ctx, task))
	require.NoError(t, d.enqueueTask(ctx, newFlushTask(dispatcherID, 100)))

	done := make(chan error, 1)
	go func() {
		done <- d.run(ctx)
	}()

	err = <-done
	require.ErrorContains(t, err, "index write failed")
}
