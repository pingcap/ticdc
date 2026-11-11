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
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestChecksumTimestampLocations(t *testing.T) {
	helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
	defer helper.Close()
	ddl := helper.DDL2Event(`create table test.checksum_locations (
		id int primary key, ts timestamp(6), ts2 timestamp(6), nullable_ts timestamp null)`)
	const (
		utc      = "2020-02-19 18:20:20.123456"
		shanghai = "2020-02-20 02:20:20.123456"
	)
	current := map[string]any{
		"id":          int64(1),
		"ts":          map[string]any{"value": shanghai, "location": "Asia/Shanghai"},
		"ts2":         map[string]any{"value": utc, "location": "UTC"},
		"nullable_ts": nil,
	}
	previous := map[string]any{
		"id":          int64(1),
		"ts":          map[string]any{"value": utc, "location": "UTC"},
		"ts2":         map[string]any{"value": shanghai, "location": "Asia/Shanghai"},
		"nullable_ts": nil,
	}
	// Both images represent the same instants, with different locations per
	// column and per image. NULL contributes no bytes to the checksum.
	data := binary.LittleEndian.AppendUint64(nil, 1)
	for range 2 {
		data = binary.LittleEndian.AppendUint32(data, uint32(len(utc)))
		data = append(data, utc...)
	}
	expected := crc32.ChecksumIEEE(data)
	for _, eventType := range []MessageType{DMLTypeInsert, DMLTypeUpdate, DMLTypeDelete} {
		t.Run(string(eventType), func(t *testing.T) {
			msg := &message{Type: eventType, Checksum: &checksum{}}
			if eventType != DMLTypeDelete {
				msg.Data = current
				msg.Checksum.Current = expected
			}
			if eventType != DMLTypeInsert {
				msg.Old = previous
				msg.Checksum.Previous = expected
			}
			event := buildDMLEvent(msg, ddl.TableInfo, true, nil)
			require.NotNil(t, event)
			t.Cleanup(event.PostFlush)
			row, ok := event.GetNextRow()
			require.True(t, ok)
			if msg.Data != nil {
				require.Equal(t, expected, row.Checksum.Current)
				require.Equal(t, shanghai, row.Row.GetTime(1).String())
				require.Equal(t, utc, row.Row.GetTime(2).String())
				require.True(t, row.Row.IsNull(3))
			}
			if msg.Old != nil {
				require.Equal(t, expected, row.Checksum.Previous)
				require.Equal(t, utc, row.PreRow.GetTime(1).String())
				require.Equal(t, shanghai, row.PreRow.GetTime(2).String())
				require.True(t, row.PreRow.IsNull(3))
			}
		})
	}
}

func TestChecksumFailureStopsDecode(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddl := helper.DDL2Event("create table test.checksum_failure (id int primary key)")
	expected := crc32.ChecksumIEEE(binary.LittleEndian.AppendUint64(nil, 1))
	for name, value := range map[string]checksum{
		"current mismatch":  {Current: expected + 1, Previous: expected},
		"previous mismatch": {Current: expected, Previous: expected + 1},
		"corrupted flag":    {Current: expected, Previous: expected, Corrupted: true},
	} {
		t.Run(name, func(t *testing.T) {
			msg := &message{
				Schema: "test", Table: "checksum_failure", CommitTs: 100,
				Type: DMLTypeUpdate, Data: map[string]any{"id": int64(1)},
				Old: map[string]any{"id": int64(1)}, Checksum: &value,
			}
			decoder := &Decoder{config: common.NewConfig(config.ProtocolSimple)}
			decoder.config.EnableRowChecksum = true
			require.PanicsWithValue(t, "consumer detect checksum corrupted", func() {
				decoder.newDMLMessage(msg, ddl.TableInfo).ToDMLEvent()
			})
		})
	}
}

func TestCachedDMLReturnsMessage(t *testing.T) {
	const (
		schema          = "test"
		table           = "t"
		logicalTableID  = int64(1)
		physicalTableID = int64(2)
		schemaVersion   = uint64(100)
		commitTs        = uint64(90)
	)
	tableIDAllocator.Clean()
	t.Cleanup(tableIDAllocator.Clean)

	decoder := &Decoder{
		config:         common.NewConfig(config.ProtocolSimple),
		memo:           newMemoryTableInfoProvider(),
		cachedMessages: list.New(),
	}
	decoder.msg = &message{
		Version:       defaultVersion,
		Schema:        schema,
		Table:         table,
		TableID:       physicalTableID,
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
			TableID: logicalTableID,
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
	require.Equal(t, physicalTableID, dmlMessage.TableID)
	require.Equal(t, schema, dmlMessage.Schema)
	require.Equal(t, table, dmlMessage.Table)
	require.Equal(t, commitTs, dmlMessage.GetCommitTs())
	require.Equal(t, commonType.RowTypeInsert, dmlMessage.RowType)
	require.ElementsMatch(t, []int64{logicalTableID, physicalTableID}, decoder.GetTableIDs(schema, table))

	decoder.msg = &message{
		Version:  defaultVersion,
		Type:     MessageTypeWatermark,
		CommitTs: commitTs + 1,
	}
	dmlEvent := dmlMessage.ToDMLEvent()
	require.NotNil(t, dmlEvent)
	require.Equal(t, physicalTableID, dmlEvent.GetTableID())
	require.Equal(t, commitTs, dmlEvent.GetCommitTs())
	require.Equal(t, schema, dmlEvent.TableInfo.GetSchemaName())
	require.Equal(t, table, dmlEvent.TableInfo.GetTableName())
	require.NotNil(t, decoder.msg)
	require.Equal(t, MessageTypeWatermark, decoder.msg.Type)
	require.Equal(t, commitTs+1, decoder.msg.CommitTs)
}

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// rebuilds from a table schema: a composite primary key must stay the handle key,
// with the real column offsets, because that is what the MySQL sink locates rows
// by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	schema := &TableSchema{
		Schema: "test",
		Table:  "t",
		Columns: []*columnSchema{
			{Name: "a", DataType: dataType{MySQLType: "INT"}},
			{Name: "b", DataType: dataType{MySQLType: "INT"}},
			{Name: "c", DataType: dataType{MySQLType: "VARCHAR"}},
		},
		Indexes: []*IndexSchema{
			{Name: "PRIMARY", Unique: true, Primary: true, Columns: []string{"a", "b"}},
			{Name: "c_unique", Unique: true, Columns: []string{"c"}},
			{Name: "b_idx", Columns: []string{"b"}},
		},
	}

	tableInfo := newTableInfo(schema)
	common.RequireRowLocatorByPrimaryKey(t, tableInfo, "a", "b")
	require.Len(t, tableInfo.GetIndices(), 3)
	indexIDs := make(map[int64]struct{})
	for _, index := range tableInfo.GetIndices() {
		require.NotContains(t, indexIDs, index.ID)
		indexIDs[index.ID] = struct{}{}
	}
}
