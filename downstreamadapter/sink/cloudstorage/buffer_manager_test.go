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

package cloudstorage

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestBufferManagerFlushesPendingBatchBeforeWaitingForDiskQuota(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "buffer-quota")
	flushCh := make(chan writerTask, 16)
	spoolBuffer, err := spool.New(
		changefeedID,
		spool.WithRootDir(t.TempDir()),
		spool.WithQuotaBytes(40),
		spool.WithMemoryRatio(0.01),
	)
	require.NoError(t, err)
	defer spoolBuffer.Close()

	controller := newBufferManager(1, changefeedID, &cloudstorage.Config{
		FlushInterval:    time.Hour,
		FileSize:         1 << 20,
		SpoolDiskQuota:   40,
		FileIndexWidth:   6,
		UseTableIDAsPath: false,
	}, spoolBuffer, flushCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- controller.run(ctx)
	}()

	firstTask := newBufferedTask("table1", commonType.NewDispatcherID(), `{"id":1}`)
	secondTask := newBufferedTask("table1", firstTask.dispatcherID, `{"id":2}`)

	require.NoError(t, controller.enqueueTask(ctx, firstTask))
	require.NoError(t, controller.enqueueTask(ctx, secondTask))

	select {
	case flushed := <-flushCh:
		require.Nil(t, flushed.marker)
		require.Len(t, flushed.batch.batch, 1)
		require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStorageFlushCountCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1", flushReasonQuota)))
		require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.CloudStoragePendingEntriesGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1")))
	case <-time.After(3 * time.Second):
		t.Fatal("buffer controller did not flush pending batch before waiting for disk quota")
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, metrics.CloudStoragePendingEntriesGauge.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1"))
	require.False(t, metrics.CloudStorageFlushCountCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1", flushReasonQuota))
}

func TestBufferManagerOversizedBatchFlushesImmediatelyFromMemory(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "buffer-oversized")
	flushCh := make(chan writerTask, 16)
	spoolBuffer, err := spool.New(
		changefeedID,
		spool.WithRootDir(t.TempDir()),
		spool.WithQuotaBytes(1),
		spool.WithMemoryRatio(0.01),
	)
	require.NoError(t, err)
	defer spoolBuffer.Close()

	controller := newBufferManager(1, changefeedID, &cloudstorage.Config{
		FlushInterval:    time.Hour,
		FileSize:         1 << 20,
		SpoolDiskQuota:   1,
		FileIndexWidth:   6,
		UseTableIDAsPath: false,
	}, spoolBuffer, flushCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- controller.run(ctx)
	}()

	task := newBufferedTask("table1", commonType.NewDispatcherID(), `{"id":1}`)
	require.NoError(t, controller.enqueueTask(ctx, task))

	select {
	case flushed := <-flushCh:
		require.Len(t, flushed.batch.batch, 1)
		for _, tableTask := range flushed.batch.batch {
			require.Len(t, tableTask.entries, 1)
			require.True(t, tableTask.entries[0].InMemory())
			require.False(t, tableTask.entries[0].IsSpilled())
		}
		require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.CloudStorageFlushCountCounter.WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1", flushReasonOversize)))
	case <-time.After(3 * time.Second):
		t.Fatal("buffer controller did not flush oversized batch immediately")
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.False(t, metrics.CloudStoragePendingEntriesGauge.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1"))
	require.False(t, metrics.CloudStorageFlushCountCounter.DeleteLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), "1", flushReasonOversize))
}

func newBufferedTask(table string, dispatcherID commonType.DispatcherID, payload string) *task {
	tableInfo := &commonType.TableInfo{
		TableName: commonType.TableName{
			Schema:  "test",
			Table:   table,
			TableID: 100,
		},
	}
	event := &commonEvent.DMLEvent{
		PhysicalTableID: 100,
		TableInfo:       tableInfo,
		DispatcherID:    dispatcherID,
	}
	event.TableInfoVersion = 1
	event.Length = 1
	event.ApproximateSize = 1

	t := newDMLTask(cloudstorage.VersionedTableName{
		TableNameWithPhysicTableID: commonType.TableName{
			Schema:  "test",
			Table:   table,
			TableID: 100,
		},
		TableInfoVersion: 1,
		DispatcherID:     dispatcherID,
	}, event)
	msg := common.NewMsg(nil, []byte(payload))
	msg.SetRowsCount(1)
	t.encodedMsgs = []*common.Message{msg}
	return t
}
