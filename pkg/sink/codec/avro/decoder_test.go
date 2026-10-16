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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func newAvroRowEventForTest(table *commonType.TableInfo, commitTs uint64, before, after chunk.Row) *commonEvent.RowEvent {
	return &commonEvent.RowEvent{TableInfo: table, PhysicalTableID: table.TableName.TableID, CommitTs: commitTs, Event: commonEvent.RowChange{PreRow: before, Row: after}, ColumnSelector: columnselector.NewDefaultColumnSelector()}
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
	source, err := assembleEvent(key, value, schema, false)
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
	dec := &decoder{config: cfg}
	event := dec.assembleDMLEventFromDecoded(key, value, schema, false, 0)
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
					if change.row.Row.IsEmpty() {
						require.Nil(t, decodedRow.Checksum)
						return
					}
					common.CompareRow(t, commonEvent.RowChange{Row: change.row.Row}, dml.TableInfo, decodedRow, decoded.TableInfo)
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
			common.CompareRow(t, commonEvent.RowChange{Row: row.Row}, dml.TableInfo, decodedRow, decoded.TableInfo)
		})
	}
}

func TestChecksumFailureStopsDecode(t *testing.T) {
	dec := &decoder{}
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
			}, false, 0)
	})
}
