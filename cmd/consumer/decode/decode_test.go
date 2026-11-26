// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decode

import (
	"testing"

	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/stretchr/testify/require"
)

func TestDecodeStorageCanalJSONSharesAlignedTableInfo(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCanalJSON)
	cfg.EnableTiDBExtension = true
	cfg.Terminator = "\n"

	group := &Group{codecConfig: cfg}
	row1 := `{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4,"v":4},"mysqlType":{"id":"int","v":"int"},"data":[{"id":"1","v":"10"}],"old":null,"_tidb":{"commitTs":200}}`
	row2 := `{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4,"v":4},"mysqlType":{"id":"int","v":"int"},"data":[{"id":"2","v":"20"}],"old":null,"_tidb":{"commitTs":201}}`

	const tableVersion uint64 = 150
	const tableID int64 = 42
	f := &Future{
		Payload: &source.Payload{
			Value: []byte(row1 + "\n" + row2 + "\n"),
			File: &source.File{
				TableID: tableID,
				Schema: &cloudstorage.SchemaFile{
					Schema:       "test",
					Table:        "t1",
					TableVersion: tableVersion,
					Columns: []cloudstorage.TableCol{
						{Name: "id", Tp: "int", IsPK: "true"},
						{Name: "v", Tp: "int", Nullable: "true"},
					},
				},
			},
		},
	}

	require.NoError(t, group.decodeStorage(t.Context(), f))
	require.Len(t, f.Events, 2)

	dml1 := f.Events[0].(*event.DMLEvent)
	dml2 := f.Events[1].(*event.DMLEvent)
	t.Cleanup(func() {
		dml1.PostFlush()
		dml2.PostFlush()
	})

	require.Same(t, dml1.TableInfo, dml2.TableInfo)
	require.Equal(t, tableVersion, dml1.TableInfo.UpdateTS)
	require.Equal(t, tableID, dml1.PhysicalTableID)
	require.Equal(t, tableID, dml2.PhysicalTableID)
}

func TestDecodeGroupBroadcastDDLDedupBootstrapAndStaleSkip(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent := helper.DDL2Event("create table test.t1(id int primary key, v int)")
	dmlEvent := helper.DML2Event("test", "t1", "insert into test.t1 values (1, 10)")
	bootDDL := helper.DDL2Event("create table test.t2(id int primary key)")
	bootDDL.TableInfo.UpdateTS = 150

	cfg := common.NewConfig(config.ProtocolSimple)
	enc, err := simple.NewEncoder(cfg, nil)
	require.NoError(t, err)

	ddlMsg, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)
	bootMsg, err := enc.EncodeDDLEvent(codec.NewBootstrapDDLEvent(bootDDL.TableInfo))
	require.NoError(t, err)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	staleTable := ddlEvent.TableInfo.CloneWithRouting(ddlEvent.TableInfo.GetSchemaName(), ddlEvent.TableInfo.GetTableName())
	staleTable.UpdateTS = 50
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      staleTable,
		CommitTs:       200,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	dmlMsgs := enc.Build()
	require.Len(t, dmlMsgs, 1)

	schemas := common.NewTableInfoProvider()
	newSimple := func() decoder {
		raw, err := simple.NewDecoder(t.Context(), cfg, nil)
		require.NoError(t, err)
		s := raw.(*simple.Decoder)
		s.ShareTableInfo(schemas)
		return decoder{Decoder: s, schema: s}
	}
	group := &Group{codecConfig: cfg, schemas: schemas}
	dec0 := newSimple()
	dec1 := newSimple()

	// Partition 1 arrives before partition 0 and forwards the DDL.
	f1 := &Future{Payload: &source.Payload{Partition: 1, Value: ddlMsg.Value}}
	cached, err := group.decodeInput(t.Context(), dec1, f1)
	require.NoError(t, err)
	require.False(t, cached)
	require.Len(t, f1.Events, 1)
	require.Zero(t, f1.Marker)

	// Partition 0 drops the duplicate DDL after updating state, keeping commit-ts as Marker.
	f0 := &Future{Payload: &source.Payload{Partition: 0, Value: ddlMsg.Value}}
	cached, err = group.decodeInput(t.Context(), dec0, f0)
	require.NoError(t, err)
	require.False(t, cached)
	require.Empty(t, f0.Events)
	require.Equal(t, ddlEvent.GetCommitTs(), f0.Marker)

	// Bootstrap updates shared schemas without emitting an event or setting Marker.
	fBoot := &Future{Payload: &source.Payload{Partition: 1, Value: bootMsg.Value}}
	cached, err = group.decodeInput(t.Context(), dec1, fBoot)
	require.NoError(t, err)
	require.False(t, cached)
	require.Empty(t, fBoot.Events)
	require.Zero(t, fBoot.Marker)
	require.NotNil(t, schemas.Read("test", "t2", 150))

	// Replayed DML with CommitTs <= ReleasedWatermark is skipped before caching.
	schemas.ReleaseBefore(200)
	fStale := &Future{Payload: &source.Payload{Partition: 0, Value: dmlMsgs[0].Value}}
	cached, err = group.decodeInput(t.Context(), dec0, fStale)
	require.NoError(t, err)
	require.False(t, cached)
	require.Empty(t, fStale.Events)
	require.Zero(t, dec0.schema.CachedCount())
}
