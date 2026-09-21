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

package schemastore

import (
	"testing"

	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestPhysicalTableDiscoveryRoundTrip(t *testing.T) {
	meta := common.KeyspaceMeta{ID: 7, Name: "test"}
	cfg := &config.FilterConfig{
		Rules: []string{"test.*"}, IgnoreTxnStartTs: []uint64{100},
		EventFilters: []*config.EventFilterRule{{
			Matcher: []string{"test.t"}, IgnoreEvent: []bf.EventType{bf.InsertEvent}, IgnoreSQL: []string{"^ALTER TABLE"},
			IgnoreInsertValueExpr: util.AddressOf("id = 1"), IgnoreDeleteValueExpr: util.AddressOf("id = 2"),
			IgnoreUpdateNewValueExpr: util.AddressOf("id = 3"), IgnoreUpdateOldValueExpr: util.AddressOf("id = 4"),
			IgnoreUpdateOnlyColumns: []string{"updated_at"},
		}},
	}
	req := &GetAllPhysicalTablesRequest{
		RequestID: 1, Keyspace: NewKeyspaceMeta(meta), Ts: 200,
		Filter: NewFilterConfig(cfg), CaseSensitive: true, ForceReplicate: true,
	}
	data, err := req.Marshal()
	require.NoError(t, err)
	var decoded GetAllPhysicalTablesRequest
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, meta, decoded.Keyspace.ToCommon())
	require.Equal(t, cfg, FilterConfigFromProto(decoded.Filter))
	require.True(t, decoded.CaseSensitive)
	require.True(t, decoded.ForceReplicate)

	tables := []commonEvent.Table{
		{SchemaID: 1, TableID: 2, Splitable: true, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"}},
		{SchemaID: 3, TableID: 4},
	}
	resp := &GetAllPhysicalTablesResponse{RequestID: 1, Tables: NewPhysicalTables(tables)}
	data, err = resp.Marshal()
	require.NoError(t, err)
	var decodedResp GetAllPhysicalTablesResponse
	require.NoError(t, decodedResp.Unmarshal(data))
	require.Equal(t, tables, PhysicalTablesFromProto(decodedResp.Tables))
}
