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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreMessageRoundTrip(t *testing.T) {
	cases := []struct {
		typ     IOType
		message IOTypeT
	}{
		{TypeSchemaStoreRequest, &SchemaStoreRequest{RequestID: 1, Operation: SchemaStoreGetTableInfos, Deadline: 123456, Keyspace: common.KeyspaceMeta{ID: 2, Name: "ks"}, TableIDs: []int64{3}, Ts: 4}},
		{TypeSchemaStoreRequest, &SchemaStoreRequest{RequestID: 1, Operation: SchemaStoreCancelRequest}},
		{TypeSchemaStoreResponse, &SchemaStoreResponse{RequestID: 1, TableInfos: []SchemaStoreTableInfo{{TableID: 3, TableInfo: []byte("table")}, {TableID: 4, Error: "table deleted"}}, More: true}},
		{TypeSchemaStoreRequest, &SchemaStoreRequest{RequestID: 5, Operation: SchemaStoreRegisterKeyspace, Keyspace: common.KeyspaceMeta{ID: 2, Name: "ks"}}},
		{TypeSchemaStoreRequest, &SchemaStoreRequest{RequestID: 6, Operation: SchemaStoreGetAllPhysicalTables, Keyspace: common.KeyspaceMeta{ID: 2, Name: "ks"}, Ts: 10, Filter: config.NewDefaultFilterConfig(), CaseSensitive: true, ForceReplicate: true}},
		{TypeSchemaStoreResponse, &SchemaStoreResponse{RequestID: 6, Tables: []commonEvent.Table{{SchemaID: 10, TableID: 20, Splitable: true, SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t"}}}}},
		{TypeSchemaStoreResponse, &SchemaStoreResponse{RequestID: 7, Error: "missing keyspace", ErrorCode: "CDC:ErrKeyspaceNotFound"}},
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
