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
// See the License for the specific language governing permissions and
// limitations under the License.

package messaging

import (
	"testing"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastore"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreMessageRoundTrip(t *testing.T) {
	cases := []struct {
		typ     IOType
		message IOTypeT
	}{
		{TypeGetTableInfosRequest, &schemastore.GetTableInfosRequest{RequestID: 1, Keyspace: schemastore.KeyspaceMeta{ID: 2, Name: "ks"}, TableIDs: []int64{3}, Ts: 4}},
		{TypeGetTableInfosResponse, &schemastore.GetTableInfosResponse{RequestID: 1, TableInfos: []schemastore.TableInfoResult{{TableID: 3, TableInfo: []byte("table")}, {TableID: 4, Error: "table deleted"}}, More: true}},
		{TypeGetAllPhysicalTablesRequest, &schemastore.GetAllPhysicalTablesRequest{RequestID: 6, Keyspace: schemastore.KeyspaceMeta{ID: 2, Name: "ks"}, Ts: 10, Filter: &eventpb.InnerFilterConfig{Rules: []string{"test.*"}}, CaseSensitive: true, ForceReplicate: true}},
		{TypeGetAllPhysicalTablesResponse, &schemastore.GetAllPhysicalTablesResponse{RequestID: 6, Tables: []schemastore.PhysicalTable{{SchemaID: 10, TableID: 20, Splitable: true, SchemaTableName: &schemastore.SchemaTableName{SchemaName: "test", TableName: "t"}}}}},
		{TypeGetTableInfosResponse, &schemastore.GetTableInfosResponse{RequestID: 7, Error: &schemastore.Error{Message: "missing keyspace", Code: "CDC:ErrKeyspaceNotFound"}}},
		{TypeGetAllPhysicalTablesResponse, &schemastore.GetAllPhysicalTablesResponse{RequestID: 8, Error: &schemastore.Error{Message: "missing keyspace", Code: "CDC:ErrKeyspaceNotFound"}}},
	}
	for _, tc := range cases {
		t.Run(tc.typ.String(), func(t *testing.T) {
			msg := NewSingleTargetMessage(node.ID("test"), SchemaStoreTopic, tc.message)
			require.Equal(t, tc.typ, msg.Type)
			data, err := tc.message.Marshal()
			require.NoError(t, err)
			decoded, err := decodeIOType(tc.typ, data)
			require.NoError(t, err)
			require.Equal(t, tc.message, decoded)
		})
	}
}
