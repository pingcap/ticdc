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
	"path"
	"sync"
	"sync/atomic"
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
	"github.com/pingcap/ticdc/pkg/util"
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

	changefeedID := commonType.NewChangefeedID4Test("test", t.Name())
	statistics := metrics.NewStatistics(changefeedID, t.Name())
	pdlock := pdutil.NewMonotonicClock(pclock.New())
	appcontext.SetService(appcontext.DefaultPDClock, pdlock)
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	d := newWriter(1, changefeedID, storage,
		cfg, ".json", statistics, nil)
	return d
}

func TestWriterRun(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)
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
		_ = d.Run(ctx)
	}()

	time.Sleep(4 * time.Second)
	// check whether files for table1 has been generated
	fileNames := getTableFiles(t, table1Dir)
	require.Len(t, fileNames, 2)
	require.ElementsMatch(t, []string{fmt.Sprintf("CDC_%s_000001.json", dispatcherID.String()), fmt.Sprintf("CDC_%s.index", dispatcherID.String())}, fileNames)
	d.closeInput()
	cancel()
	d.close()
	wg.Wait()
}

func TestWriterDrainMarker(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	d := testWriter(ctx, t, parentDir)

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
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

	doneCh := make(chan error, 1)
	require.NoError(t, d.enqueueTask(ctx, newDrainTask(dispatcherID, 100, doneCh)))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = d.Run(ctx)
	}()

	select {
	case err := <-doneCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("wait drain marker timeout")
	}
	require.Eventually(t, func() bool {
		return callbackCnt.Load() == 1
	}, 5*time.Second, 100*time.Millisecond)

	d.closeInput()
	d.close()
	cancel()
	wg.Wait()
}
