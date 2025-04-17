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
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func testWorker(
	t *testing.T,
) (*worker, chan eventFragment, chan eventFragment) {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	changefeedID := common.NewChangefeedID4Test("test", "table1")
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		replicaConfig.Sink, config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoder, err := codec.NewTxnEventEncoder(encoderConfig)
	require.Nil(t, err)

	encodedCh := make(chan eventFragment)
	msgCh := make(chan eventFragment, 1024)
	return newWorker(1, changefeedID, encoder, msgCh, encodedCh), msgCh, encodedCh
}

func TestEncodeEvents(t *testing.T) {
	t.Parallel()

	encodingWorker, _, encodedCh := testWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewAutoDrainChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.Run(egCtx)
	})

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: pmodel.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: pmodel.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := common.WrapTableInfo("test", tidbTableInfo)
	err := encodingWorker.encodeEvents(eventFragment{
		versionedTable: cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: common.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
			TableInfoVersion: 33,
		},
		seqNumber: 1,
		event: &commonEvent.DMLEvent{
			PhysicalTableID: 100,
			TableInfo:       tableInfo,
			Rows:            chunk.MutRowFromValues(100, "hello world", 200, "你好，世界").ToRow().Chunk(),
		},
	})
	require.Nil(t, err)
	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestEncodingWorkerRun(t *testing.T) {
	t.Parallel()

	encodingWorker, msgCh, encodedCh := testWorker(t)

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: pmodel.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: pmodel.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := common.WrapTableInfo("test", tidbTableInfo)

	for i := 0; i < 3; i++ {
		frag := eventFragment{
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: common.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
			},
			seqNumber: uint64(i + 1),
			event: &commonEvent.DMLEvent{
				PhysicalTableID: 100,
				TableInfo:       tableInfo,
				Rows:            chunk.MutRowFromValues(100, "hello world").ToRow().Chunk(),
			},
		}
		msgCh <- frag
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	outputChs := []*chann.DrainableChann[eventFragment]{chann.NewAutoDrainChann[eventFragment]()}
	defragmenter := newDefragmenter(encodedCh, outputChs)
	g.Go(func() error {
		return defragmenter.Run(ctx)
	})

	g.Go(func() error {
		return encodingWorker.Run(ctx)
	})
	cancel()
	encodingWorker.Close()
	err := g.Wait()
	require.ErrorIs(t, err, context.Canceled)
}
