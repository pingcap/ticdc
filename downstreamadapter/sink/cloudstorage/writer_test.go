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
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	pclock "github.com/pingcap/ticdc/pkg/clock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/failpointrecord"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func testWriter(ctx context.Context, t *testing.T, dir string) *writer {
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", dir)
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, uri)
	require.Nil(t, err)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := cloudstorage.NewConfig()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = util.AddressOf(config.DateSeparatorNone.String())
	err = cfg.Apply(context.TODO(), sinkURI, replicaConfig.Sink, true)
	cfg.FileIndexWidth = 6
	require.Nil(t, err)

	changefeedID := commonType.NewChangefeedID4Test("test", "dml-worker-test")
	statistics := metrics.NewStatistics(changefeedID, "dml-worker-test")
	pdlock := pdutil.NewMonotonicClock(pclock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	d := newWriter(1, changefeedID, storage,
		cfg, ".json", chann.NewAutoDrainChann[eventFragment](), statistics)
	return d
}

func TestWriterRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
	fragCh := d.inputCh
	table1Dir := path.Join(parentDir, "test/table1/99")

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: ast.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := commonType.WrapTableInfo("test", tidbTableInfo)

	dispatcherID := commonType.NewDispatcherID()
	for i := 0; i < 5; i++ {
		frag := eventFragment{
			seqNumber: uint64(i),
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: commonType.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				TableInfoVersion: 99,
				DispatcherID:     dispatcherID,
			},
			event: &commonEvent.DMLEvent{
				PhysicalTableID: 100,
				TableInfo:       tableInfo,
				Rows:            chunk.MutRowFromValues(100, "hello world").ToRow().Chunk(),
			},
			encodedMsgs: []*common.Message{
				{
					Value: []byte(fmt.Sprintf(`{"id":%d,"database":"test","table":"table1","pkNames":[],"isDdl":false,`+
						`"type":"INSERT","es":0,"ts":1663572946034,"sql":"","sqlType":{"c1":12,"c2":12},`+
						`"data":[{"c1":"100","c2":"hello world"}],"old":null}`, i)),
				},
			},
		}
		fragCh.In() <- frag
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.Run(ctx)
	}()

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	fileNames := getTableFiles(t, table1Dir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{fmt.Sprintf("CDC_%s_000001.json", dispatcherID.String()), fmt.Sprintf("CDC_%s.index", dispatcherID.String())}, fileNames)
	fragCh.CloseAndDrain()
	cancel()
	d.close()
	wg.Wait()
}

func TestMutateMessageValueForFailpointRecordClassification(t *testing.T) {
	t.Parallel()

	msg := &common.Message{
		Value: []byte(
			`{"pkNames":["id"],"data":[{"id":"1","c2":"v1"}]}` +
				"\r\n" +
				`{"pkNames":["id"],"data":[{"id":"2","_tidb_origin_ts":"100"}]}`,
		),
	}
	rowRecords := []failpointrecord.RowRecord{
		{
			CommitTs:    101,
			PrimaryKeys: map[string]any{"id": "1"},
		},
		{
			CommitTs:    102,
			PrimaryKeys: map[string]any{"id": "2"},
		},
	}

	mutatedRows, originTsMutatedRows := mutateMessageValueForFailpoint(msg, rowRecords)

	require.Len(t, mutatedRows, 1)
	require.Equal(t, uint64(101), mutatedRows[0].CommitTs)
	require.Equal(t, "1", mutatedRows[0].PrimaryKeys["id"])

	require.Len(t, originTsMutatedRows, 1)
	require.Equal(t, uint64(102), originTsMutatedRows[0].CommitTs)
	require.Equal(t, "2", originTsMutatedRows[0].PrimaryKeys["id"])

	require.True(t, bytes.Contains(msg.Value, []byte(`"_tidb_origin_ts":"101"`)))
	require.True(t, bytes.Contains(msg.Value, []byte(`"c2":null`)))
}

func TestSelectColumnToMutateSkipNilOriginTsWhenPossible(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: nil,
		"c2":                       "v1",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, "c2", col)
	}
}

func TestSelectColumnToMutatePreferNonNilOriginTs(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: "100",
		"c2":                       "v1",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, commonEvent.OriginTsColumn, col)
	}
}

func TestSelectColumnToMutateSkipNilNonPKColumns(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id": "1",
		"c1": nil,
		"c2": "v2",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, "c2", col)
	}
}

func TestSelectColumnToMutateNoCandidateWhenAllNonPKColumnsNil(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: nil,
		"c1":                       nil,
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	col, ok := selectColumnToMutate(row, pkSet)
	require.False(t, ok)
	require.Empty(t, col)
}
