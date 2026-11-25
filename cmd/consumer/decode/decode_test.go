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
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
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
