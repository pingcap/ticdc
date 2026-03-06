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

package cloudstorage

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	pkgcloudstorage "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestTaskIndexerRouteOutputShardCached(t *testing.T) {
	t.Parallel()

	indexer := newTaskIndexer(2, 4)
	dispatcherID := commonType.NewDispatcherID()
	first := indexer.routeOutputIndex(dispatcherID)
	for i := 0; i < 32; i++ {
		require.Equal(t, first, indexer.routeOutputIndex(dispatcherID))
	}
	require.Equal(t, 1, indexer.cachedOutputCount())
}

func TestTaskIndexerRouteOutputShardStable(t *testing.T) {
	t.Parallel()

	dispatcherA := commonType.NewDispatcherID()
	dispatcherB := commonType.NewDispatcherID()

	indexer := newTaskIndexer(2, 4)
	shardA := indexer.routeOutputIndex(dispatcherA)
	shardB := indexer.routeOutputIndex(dispatcherB)
	for shardA == shardB {
		dispatcherB = commonType.NewDispatcherID()
		shardB = indexer.routeOutputIndex(dispatcherB)
	}
	require.True(t, shardA >= 0 && shardA < 4)
	require.True(t, shardB >= 0 && shardB < 4)
}

func TestEncodingGroupRouteByDispatcher(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	encoderConfig := newTestTxnEncoderConfig(t)
	group := newEncodingGroup(commonType.NewChangefeedID4Test("test", "encoder-group"), encoderConfig, 2, 4)
	eg.Go(func() error {
		return group.Run(egCtx)
	})

	dispatcherA := commonType.NewDispatcherID()
	dispatcherB := commonType.NewDispatcherID()
	shardA := group.indexer.routeOutputIndex(dispatcherA)
	shardB := group.indexer.routeOutputIndex(dispatcherB)
	for shardA == shardB {
		dispatcherB = commonType.NewDispatcherID()
		shardB = group.indexer.routeOutputIndex(dispatcherB)
	}

	receivedA := make(chan []uint64, 1)
	receivedB := make(chan []uint64, 1)
	eg.Go(func() error {
		values := make([]uint64, 0, 5)
		err := group.ConsumeOutputShard(ctx, shardA, func(future *taskFuture) error {
			if err := future.Ready(ctx); err != nil {
				return err
			}
			if future.task.dispatcherID != dispatcherA {
				return nil
			}
			values = append(values, future.task.marker.commitTs)
			if len(values) == 5 {
				receivedA <- values
			}
			return nil
		})
		return err
	})
	eg.Go(func() error {
		values := make([]uint64, 0, 5)
		err := group.ConsumeOutputShard(ctx, shardB, func(future *taskFuture) error {
			if err := future.Ready(ctx); err != nil {
				return err
			}
			if future.task.dispatcherID != dispatcherB {
				return nil
			}
			values = append(values, future.task.marker.commitTs)
			if len(values) == 5 {
				receivedB <- values
			}
			return nil
		})
		return err
	})

	for i := uint64(1); i <= 5; i++ {
		require.NoError(t, group.Add(ctx, newDrainTask(dispatcherA, i, nil)))
		require.NoError(t, group.Add(ctx, newDrainTask(dispatcherB, i, nil)))
	}

	var resultA []uint64
	var resultB []uint64
	select {
	case resultA = <-receivedA:
	case <-time.After(5 * time.Second):
		t.Fatal("wait dispatcher a timeout")
	}
	select {
	case resultB = <-receivedB:
	case <-time.After(5 * time.Second):
		t.Fatal("wait dispatcher b timeout")
	}

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, resultA)
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, resultB)

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestEncodingGroupEncodeDMLTask(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	encoderConfig := newTestTxnEncoderConfig(t)
	changefeedID := commonType.NewChangefeedID4Test("test", "encoder-group")
	group := newEncodingGroup(changefeedID, encoderConfig, 2, 1)
	eg.Go(func() error {
		return group.Run(egCtx)
	})

	dispatcherID := commonType.NewDispatcherID()
	taskValue := newDMLTask(
		pkgcloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: commonType.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 1,
			DispatcherID:     dispatcherID,
		},
		newTestDMLEvent(dispatcherID, 100),
	)
	require.NoError(t, group.Add(ctx, taskValue))

	done := make(chan struct{}, 1)
	eg.Go(func() error {
		return group.ConsumeOutputShard(ctx, 0, func(future *taskFuture) error {
			if err := future.Ready(ctx); err != nil {
				return err
			}
			require.Equal(t, taskValue, future.task)
			done <- struct{}{}
			return nil
		})
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wait dml encode timeout")
	}

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func newTestTxnEncoderConfig(t *testing.T) *common.Config {
	uri := "file:///tmp/test"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	changefeedID := commonType.NewChangefeedID4Test("test", "encoder-config")
	encoderConfig, err := helper.GetEncoderConfig(
		changefeedID,
		sinkURI,
		config.ProtocolCsv,
		replicaConfig.Sink,
		config.DefaultMaxMessageBytes,
	)
	require.NoError(t, err)
	return encoderConfig
}

func newTestDMLEvent(dispatcherID commonType.DispatcherID, tableID int64) *commonEvent.DMLEvent {
	tidbTableInfo := &timodel.TableInfo{
		ID:   tableID,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: ast.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)
	return &commonEvent.DMLEvent{
		DispatcherID:     dispatcherID,
		PhysicalTableID:  tableID,
		TableInfo:        tableInfo,
		TableInfoVersion: 1,
		Rows:             chunk.MutRowFromValues(1, "hello world").ToRow().Chunk(),
	}
}
