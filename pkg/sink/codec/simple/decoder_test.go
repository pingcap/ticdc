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

package simple

import (
	"container/list"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCachedDMLReturnsMessage(t *testing.T) {
	const (
		schema        = "test"
		table         = "t"
		tableID       = int64(1)
		schemaVersion = uint64(100)
		commitTs      = uint64(90)
	)

	decoder := &Decoder{
		config:         common.NewConfig(config.ProtocolSimple),
		memo:           newMemoryTableInfoProvider(),
		cachedMessages: list.New(),
	}
	decoder.msg = &message{
		Version:       defaultVersion,
		Schema:        schema,
		Table:         table,
		TableID:       tableID,
		Type:          DMLTypeInsert,
		CommitTs:      commitTs,
		SchemaVersion: schemaVersion,
		Data:          map[string]any{"id": int64(1)},
	}

	require.Nil(t, decoder.NextDMLMessage())
	require.Equal(t, 1, decoder.cachedMessages.Len())

	decoder.msg = &message{
		Version:  defaultVersion,
		Type:     DDLTypeCreate,
		CommitTs: schemaVersion,
		TableSchema: &TableSchema{
			Schema:  schema,
			Table:   table,
			TableID: tableID,
			Version: schemaVersion,
			Columns: []*columnSchema{
				{
					Name: "id",
					DataType: dataType{
						MySQLType: "bigint",
						Charset:   "binary",
						Collate:   "binary",
						Length:    20,
					},
				},
			},
		},
	}
	ddl := decoder.NextDDLEvent()
	require.NotNil(t, ddl)

	cachedMessages := decoder.GetCachedMessages()
	require.Len(t, cachedMessages, 1)
	require.Zero(t, decoder.cachedMessages.Len())

	dmlMessage := cachedMessages[0]
	require.Equal(t, tableID, dmlMessage.TableID)
	require.Equal(t, schema, dmlMessage.Schema)
	require.Equal(t, table, dmlMessage.Table)
	require.Equal(t, commitTs, dmlMessage.GetCommitTs())
	require.Equal(t, commonType.RowTypeInsert, dmlMessage.RowType)

	decoder.msg = &message{
		Version:  defaultVersion,
		Type:     MessageTypeWatermark,
		CommitTs: commitTs + 1,
	}
	dmlEvent := dmlMessage.ToDMLEvent()
	require.NotNil(t, dmlEvent)
	require.Equal(t, tableID, dmlEvent.GetTableID())
	require.Equal(t, commitTs, dmlEvent.GetCommitTs())
	require.Equal(t, schema, dmlEvent.TableInfo.GetSchemaName())
	require.Equal(t, table, dmlEvent.TableInfo.GetTableName())
	require.NotNil(t, decoder.msg)
	require.Equal(t, MessageTypeWatermark, decoder.msg.Type)
	require.Equal(t, commitTs+1, decoder.msg.CommitTs)
}
