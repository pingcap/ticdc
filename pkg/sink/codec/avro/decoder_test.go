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

package avro

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestDecoderCodecCacheIsBounded(t *testing.T) {
	const schema = "{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"
	codec, err := GenCodec(schema)
	require.NoError(t, err)
	decoder := NewDecoder(nil, 0, nil, "topic", nil).(*decoder)

	var firstID, secondID schemamanager.SchemaID
	for i := 1; i <= decoderCodecCacheSize; i++ {
		schemaID := schemamanager.NewConfluentSchemaID(i)
		decoder.codecs.Add(schemaID, codec)
		switch i {
		case 1:
			firstID = schemaID
		case 2:
			secondID = schemaID
		}
	}

	// Keep the first schema hot, so adding one more schema evicts the second one.
	_, ok := decoder.codecs.Get(firstID)
	require.True(t, ok)
	extraID := schemamanager.NewConfluentSchemaID(decoderCodecCacheSize + 1)
	decoder.codecs.Add(extraID, codec)

	require.Equal(t, decoderCodecCacheSize, decoder.codecs.Len())
	require.True(t, decoder.codecs.Contains(firstID))
	require.False(t, decoder.codecs.Contains(secondID))
}

// TestDecodedTableInfoLocatesRowByPrimaryKey checks the table info this decoder
// builds from an avro message schema: the key columns must stay the handle key,
// with the real column offsets, because that is what the MySQL sink locates rows
// by.
func TestDecodedTableInfoLocatesRowByPrimaryKey(t *testing.T) {
	fields := []any{
		map[string]any{"name": "a", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "b", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "c", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
	}
	for _, primaryKeys := range [][]string{{"a", "b"}, {"b"}} {
		t.Run(strings.Join(primaryKeys, ","), func(t *testing.T) {
			columns, _, _, err := avroData2Columns(map[string]any{
				"a": int64(1), "b": int64(2), "c": int64(3),
			}, fields)
			require.NoError(t, err)
			keyMap := make(map[string]any, len(primaryKeys))
			for _, key := range primaryKeys {
				keyMap[key] = nil
			}

			tableInfo := newTableInfo(common.NewTableInfoProvider(), "test", "t", columns, keyMap)
			require.Equal(t, primaryKeys, tableInfo.GetPrimaryKeyColumnNames())
			require.True(t, tableInfo.HasPKOrNotNullUK)
			common.RequireRowLocatorByPrimaryKey(t, tableInfo, primaryKeys...)
		})
	}
}

func TestDecodeIntegerPrimaryKeyWithUpstreamChecksum(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	checksum := crc32.ChecksumIEEE(binary.LittleEndian.AppendUint64(nil, 42))
	mock.ExpectExec("set @@tidb_snapshot=100").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("select tidb_row_checksum() from test.t where id = 42").
		WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow(checksum))
	mock.ExpectClose()

	dec := &decoder{upstreamTiDB: db, cache: common.NewTableInfoProvider()}
	event := dec.assembleDMLEventFromDecoded(
		map[string]any{"id": int64(42)},
		map[string]any{
			"id": int64(42), tidbCommitTs: int64(100), tidbOp: updateOperation,
			tidbRowLevelChecksum: strconv.FormatUint(uint64(checksum), 10),
		},
		map[string]any{
			"namespace": "default.test", "name": "t",
			"fields": []any{
				map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
			},
		}, false, true, 0)
	require.NotNil(t, event)
	require.Equal(t, int32(2), event.Len())
	deleted, ok := event.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonType.RowTypeDelete, deleted.RowType)
	require.Zero(t, deleted.Checksum.Previous)
	require.Zero(t, deleted.Checksum.Current)
	inserted, ok := event.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonType.RowTypeInsert, inserted.RowType)
	require.Equal(t, checksum, inserted.Checksum.Current)
	_, ok = event.GetNextRow()
	require.False(t, ok)
	t.Cleanup(event.PostFlush)
}

func TestDecodeTimestampChecksumUsesLocalTimeZone(t *testing.T) {
	local, err := util.GetLocalTimezone()
	require.NoError(t, err)
	const timestamp = "2020-02-20 02:20:20.123456"
	key := map[string]any{"id": int64(1)}
	value := map[string]any{"id": int64(1), "ts": timestamp, tidbCommitTs: int64(100), tidbOp: insertOperation}
	schema := map[string]any{
		"namespace": "default.test", "name": "t",
		"fields": []any{
			map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
			map[string]any{"name": "ts", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "TIMESTAMP"}}},
		},
	}
	source, err := assembleEvent(common.NewTableInfoProvider(), key, value, schema, false, true, 0)
	require.NoError(t, err)
	t.Cleanup(source.PostFlush)
	data := rowcodec.RowData{}
	for i, column := range source.TableInfo.GetColumns() {
		datum := source.Rows.GetRow(0).GetDatum(i, &column.FieldType)
		data.Cols = append(data.Cols, rowcodec.ColData{ColumnInfo: column, Datum: &datum})
	}
	checksum, err := data.Checksum(local)
	require.NoError(t, err)
	value[tidbRowLevelChecksum] = strconv.FormatUint(uint64(checksum), 10)

	// Avro carries no timestamp location, so the legacy consumer uses local
	// time even when a different time zone is configured for the decoder.
	_, offset := time.Date(2020, 2, 20, 2, 20, 20, 0, local).Zone()
	cfg := common.NewConfig(config.ProtocolAvro)
	cfg.TimeZone = time.FixedZone("consumer", offset+3600)
	dec := &decoder{config: cfg, cache: common.NewTableInfoProvider()}
	event := dec.assembleDMLEventFromDecoded(key, value, schema, false, true, 0)
	require.NotNil(t, event)
	t.Cleanup(event.PostFlush)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.Equal(t, checksum, row.Checksum.Current)
	require.Equal(t, timestamp, row.Row.GetTime(1).String())
}

func TestAvroTemporalChecksum(t *testing.T) {
	local, err := util.GetLocalTimezone()
	require.NoError(t, err)
	helper := commonEvent.NewEventTestHelperWithTimeZone(t, local)
	defer helper.Close()
	cfg := common.NewConfig(config.ProtocolAvro)
	cfg.EnableTiDBExtension = true
	cfg.EnableRowChecksum = true
	cfg.AvroIncludeBeforeValue = true
	ctx := t.Context()
	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, cfg)
	require.NoError(t, err)
	defer TeardownEncoderAndSchemaRegistry4Testing()

	for _, fsp := range []int{0, 3, 6} {
		t.Run(strconv.Itoa(fsp), func(t *testing.T) {
			table := fmt.Sprintf("temporal%d", fsp)
			helper.DDL2Event(fmt.Sprintf(`create table test.%s (
				id int primary key, d date, dt datetime(%d), ts timestamp(%d), tm time(%d))`, table, fsp, fsp, fsp))
			dml, _ := helper.DML2UpdateEvent("test", table,
				fmt.Sprintf("insert into test.%s values (1, '2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20')", table),
				fmt.Sprintf("update test.%s set dt = '2020-02-20 02:20:21.123456', ts = '2020-02-20 02:20:21.123456', tm = '02:20:21.123456' where id = 1", table))
			row, ok := dml.GetNextRow()
			require.True(t, ok)
			checksum := func(row chunk.Row) uint32 {
				data := rowcodec.RowData{}
				for i, column := range dml.TableInfo.GetColumns() {
					datum := row.GetDatum(i, &column.FieldType)
					data.Cols = append(data.Cols, rowcodec.ColData{ColumnInfo: column, Datum: &datum})
				}
				value, err := data.Checksum(local)
				require.NoError(t, err)
				return value
			}
			for _, change := range []struct {
				name string
				row  commonEvent.RowChange
			}{
				{"insert", commonEvent.RowChange{Row: row.PreRow}},
				{"update", row},
				{"updateToNull", commonEvent.RowChange{PreRow: row.Row, Row: chunk.MutRowFromValues(int64(1), nil, nil, nil, nil).ToRow()}},
				{"delete", commonEvent.RowChange{PreRow: row.Row}},
			} {
				t.Run(change.name, func(t *testing.T) {
					event := newAvroRowEventForTest(dml.TableInfo, dml.CommitTs, change.row.PreRow, change.row.Row)
					event.Checksum = &integrity.Checksum{}
					if !change.row.Row.IsEmpty() {
						event.Checksum.Current = checksum(change.row.Row)
					}
					if !change.row.PreRow.IsEmpty() {
						event.Checksum.Previous = checksum(change.row.PreRow)
					}
					const topic = "temporal-checksum"
					require.NoError(t, encoder.AppendRowChangedEvent(ctx, topic, event))
					messages := encoder.Build()
					require.Len(t, messages, 1)
					dec := NewDecoder(cfg, 0, encoder.schemaM, topic, nil)
					dec.AddKeyValue(messages[0].Key, messages[0].Value)
					messageType, hasNext := dec.HasNext()
					require.True(t, hasNext)
					require.Equal(t, common.MessageTypeRow, messageType)
					decoded := dec.NextDMLMessage().ToDMLEvent()
					t.Cleanup(decoded.PostFlush)
					decodedRow, ok := decoded.GetNextRow()
					require.True(t, ok)
					common.CompareRow(t, change.row, dml.TableInfo, decodedRow, decoded.TableInfo)
					if !change.row.PreRow.IsEmpty() {
						column := decoded.TableInfo.GetColumns()[4]
						duration := decodedRow.PreRow.GetDuration(4, column.GetDecimal())
						require.Equal(t, change.row.PreRow.GetDuration(4, fsp).String(), duration.String())
					}
					if !change.row.Row.IsEmpty() {
						require.Equal(t, event.Checksum.Current, decodedRow.Checksum.Current)
					} else {
						require.Equal(t, event.Checksum.Previous, decodedRow.Checksum.Previous)
					}
				})
			}
		})
	}
}

func TestAvroChecksumAfterColumnTypeChange(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Event(`create table test.checksum_order (
		id int primary key, c1 smallint, c2 int, unique index idx(c1, c2))`)
	helper.DDL2Event("alter table test.checksum_order modify column c1 smallint unsigned default 2345")
	dml, _ := helper.DML2UpdateEvent("test", "checksum_order",
		"insert into test.checksum_order values (1, 23, 59)",
		"update test.checksum_order set c1 = 2345 where id = 1")
	columns := dml.TableInfo.GetColumns()
	require.Greater(t, columns[1].ID, columns[2].ID)
	row, ok := dml.GetNextRow()
	require.True(t, ok)
	checksum := func(row chunk.Row) uint32 {
		data := rowcodec.RowData{}
		for _, i := range []int{0, 2, 1} {
			column := columns[i]
			datum := row.GetDatum(i, &column.FieldType)
			data.Cols = append(data.Cols, rowcodec.ColData{ColumnInfo: column, Datum: &datum})
		}
		value, err := data.Checksum(time.UTC)
		require.NoError(t, err)
		return value
	}
	event := newAvroRowEventForTest(dml.TableInfo, dml.CommitTs, row.PreRow, row.Row)
	event.Checksum = &integrity.Checksum{Current: checksum(row.Row), Previous: checksum(row.PreRow)}
	cfg := common.NewConfig(config.ProtocolAvro)
	cfg.EnableTiDBExtension = true
	cfg.EnableRowChecksum = true
	cfg.AvroIncludeBeforeValue = true
	ctx := t.Context()
	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, cfg)
	require.NoError(t, err)
	defer TeardownEncoderAndSchemaRegistry4Testing()

	for _, name := range []string{"registration", "cached"} {
		t.Run(name, func(t *testing.T) {
			const topic = "checksum-order"
			require.NoError(t, encoder.AppendRowChangedEvent(ctx, topic, event))
			messages := encoder.Build()
			require.Len(t, messages, 1)
			columns := dml.TableInfo.GetColumns()
			require.Equal(t, []string{"id", "c1", "c2"}, []string{
				columns[0].Name.O, columns[1].Name.O, columns[2].Name.O,
			})
			dec := NewDecoder(cfg, 0, encoder.schemaM, topic, nil)
			dec.AddKeyValue(messages[0].Key, messages[0].Value)
			messageType, hasNext := dec.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)
			decoded := dec.NextDMLMessage().ToDMLEvent()
			t.Cleanup(decoded.PostFlush)
			decodedRow, ok := decoded.GetNextRow()
			require.True(t, ok)
			require.Equal(t, event.Checksum.Current, decodedRow.Checksum.Current)
			common.CompareRow(t, row, dml.TableInfo, decodedRow, decoded.TableInfo)
		})
	}
}

func TestChecksumFailureStopsDecode(t *testing.T) {
	dec := &decoder{cache: common.NewTableInfoProvider()}
	require.PanicsWithValue(t, "verify row checksum failed", func() {
		dec.assembleDMLEventFromDecoded(
			map[string]any{"id": int64(1)},
			map[string]any{
				"id": int64(1), tidbCommitTs: int64(100), tidbOp: insertOperation,
				tidbRowLevelChecksum: "1",
			},
			map[string]any{
				"namespace": "default.test", "name": "t",
				"fields": []any{
					map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
				},
			}, false, true, 0)
	})
}

// TestDecodedTableInfoWithoutKeyColumnsHasNoRowLocator checks the empty key
// case: without a key column the message carries no row locator, so the decoder
// must not claim a primary key. An empty primary index would make the sink emit
// a WHERE clause without a column to compare.
func TestDecodedTableInfoWithoutKeyColumnsHasNoRowLocator(t *testing.T) {
	fields := []any{
		map[string]any{"name": "a", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "b", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
	}
	columns, _, _, err := avroData2Columns(map[string]any{"a": int64(1), "b": int64(2)}, fields)
	require.NoError(t, err)

	tableInfo := newTableInfo(common.NewTableInfoProvider(), "test", "t", columns, nil)
	require.False(t, tableInfo.PKIsHandle())
	require.Empty(t, tableInfo.GetIndices())
	require.Empty(t, tableInfo.GetOrderedHandleKeyColumnIDs())
}

// A TIME column takes its decimal from the value, so a NULL row leaves it
// unknown: the layout fills the maximum so another event reusing it still prints
// every digit, and it must not become the cached layout of the version.
func TestNullTimeColumnPrecision(t *testing.T) {
	fields := []any{
		map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		map[string]any{"name": "t", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "TIME"}}},
	}

	columns, _, precise, err := avroData2Columns(map[string]any{"id": int64(1), "t": nil}, fields)
	require.NoError(t, err)
	require.False(t, precise)
	require.Equal(t, types.MaxFsp, columns[1].GetDecimal())

	columns, _, precise, err = avroData2Columns(map[string]any{
		"id": int64(1), "t": "01:02:03.123",
	}, fields)
	require.NoError(t, err)
	require.True(t, precise)
	require.Equal(t, 3, columns[1].GetDecimal())

	cache := common.NewTableInfoProvider()
	queryTableInfo(cache, "test", "t", 100, columns, nil, false)
	_, cached := cache.ReadAt("test", "t", 101)
	require.Nil(t, cached)

	queryTableInfo(cache, "test", "t", 100, columns, nil, true)
	_, cached = cache.ReadAt("test", "t", 101)
	require.NotNil(t, cached)
	require.Equal(t, 3, cached.GetColumns()[1].GetDecimal())
}

func TestUpdateWithoutBeforeValue(t *testing.T) {
	schema := map[string]any{
		"namespace": "default.test", "name": "t",
		"fields": []any{
			map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
			map[string]any{"name": "v", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
		},
	}
	key := map[string]any{"id": int64(1)}
	makeEvent := func(op string, ts, value int64) *commonEvent.DMLEvent {
		e, err := assembleEvent(common.NewTableInfoProvider(), key, map[string]any{
			"id": int64(1), "v": value, tidbOp: op, tidbCommitTs: ts,
		}, schema, false, true, 0)
		require.NoError(t, err)
		return e
	}
	t.Run("row semantics", func(t *testing.T) {
		e := makeEvent(updateOperation, 101, 20)
		defer e.PostFlush()
		require.Equal(t, int32(2), e.Len())
		require.Equal(t, uint64(101), e.CommitTs)
		deleted, ok := e.GetNextRow()
		require.True(t, ok)
		require.Equal(t, commonType.RowTypeDelete, deleted.RowType)
		require.Equal(t, int64(1), deleted.PreRow.GetInt64(0))
		require.True(t, deleted.PreRow.IsNull(1))
		inserted, ok := e.GetNextRow()
		require.True(t, ok)
		require.Equal(t, commonType.RowTypeInsert, inserted.RowType)
		require.Equal(t, int64(20), inserted.Row.GetInt64(1))
		_, ok = e.GetNextRow()
		require.False(t, ok)
	})
}

func TestUpdateWithoutBeforeValueRequiresKey(t *testing.T) {
	schema := map[string]any{
		"namespace": "default.test", "name": "t",
		"fields": []any{map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}}},
	}
	for _, key := range []map[string]any{nil, {"id": nil}, {"missing": int64(1)}} {
		_, err := assembleEvent(common.NewTableInfoProvider(), key, map[string]any{"id": int64(1), tidbOp: updateOperation, tidbCommitTs: int64(100)}, schema, false, true, 0)
		require.ErrorContains(t, err, "handle")
	}
}

func TestAvroNextDDLEventInvalidatesSharedCache(t *testing.T) {
	provider := common.NewTableInfoProvider()
	dec := NewDecoder(common.NewConfig(config.ProtocolAvro), 0, nil, "topic", nil).(*decoder)
	dec.ShareTableInfo(provider)

	t1 := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1"), UpdateTS: 100})
	t2 := commonType.WrapTableInfo("other", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("t2"), UpdateTS: 100})
	provider.WriteTableInfo(t1)
	provider.WriteTableInfo(t2)

	payload, err := json.Marshal(&ddlEvent{
		Query:    "ALTER TABLE test.t1 EXCHANGE PARTITION p0 WITH TABLE test.t_normal",
		Schema:   "test",
		Table:    "t1",
		CommitTs: 120,
	})
	require.NoError(t, err)
	dec.AddKeyValue(nil, append([]byte{ddlByte}, payload...))
	tp, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, tp)
	_ = dec.NextDDLEvent()

	_, before := provider.ReadAt("other", "t2", 120)
	require.Same(t, t2, before)
	_, afterT1 := provider.ReadAt("test", "t1", 121)
	require.Nil(t, afterT1)
	_, afterT2 := provider.ReadAt("other", "t2", 121)
	require.Nil(t, afterT2)
}

func TestAvroNewDecoderDoesNotClearExistingDecoderState(t *testing.T) {
	d1 := NewDecoder(common.NewConfig(config.ProtocolAvro), 0, nil, "topic", nil).(*decoder)
	event := d1.assembleDMLEventFromDecoded(
		map[string]any{"id": int64(1)},
		map[string]any{"id": int64(1), tidbCommitTs: int64(100), tidbOp: insertOperation},
		map[string]any{
			"namespace": "default.test", "name": "person",
			"fields": []any{
				map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
			},
		}, false, true, 0)
	t.Cleanup(event.PostFlush)

	blockedBefore := d1.cache.GetBlockedTables("test", "person")
	require.Equal(t, []int64{event.PhysicalTableID}, blockedBefore)

	d2 := NewDecoder(common.NewConfig(config.ProtocolAvro), 0, nil, "topic", nil).(*decoder)
	require.Equal(t, blockedBefore, d1.cache.GetBlockedTables("test", "person"))
	require.Empty(t, d2.cache.GetBlockedTables("test", "person"))
}
