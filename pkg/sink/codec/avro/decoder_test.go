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
	"hash/crc32"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/pingcap/ticdc/pkg/sink/sqlmodel"
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
			columns, _, err := avroData2Columns(map[string]any{
				"a": int64(1), "b": int64(2), "c": int64(3),
			}, fields)
			require.NoError(t, err)
			keyMap := make(map[string]any, len(primaryKeys))
			for _, key := range primaryKeys {
				keyMap[key] = nil
			}

			tableInfo := newTableInfo("test", "t", columns, keyMap)
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

	dec := &decoder{upstreamTiDB: db}
	event := dec.assembleDMLEventFromDecoded(
		map[string]any{"id": int64(42)},
		map[string]any{
			"id": int64(42), tidbCommitTs: int64(100),
			tidbRowLevelChecksum: strconv.FormatUint(uint64(checksum), 10),
		},
		map[string]any{
			"namespace": "default.test", "name": "t",
			"fields": []any{
				map[string]any{"name": "id", "type": map[string]any{"connect.parameters": map[string]any{"tidb_type": "INT"}}},
			},
		}, false, true, 0)
	require.NotNil(t, event)
	t.Cleanup(event.PostFlush)
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
	columns, _, err := avroData2Columns(map[string]any{"a": int64(1), "b": int64(2)}, fields)
	require.NoError(t, err)

	tableInfo := newTableInfo("test", "t", columns, nil)
	require.False(t, tableInfo.PKIsHandle())
	require.Empty(t, tableInfo.GetIndices())
	require.Nil(t, sqlmodel.GetWhereHandle(tableInfo, tableInfo).UniqueNotNullIdx)
}
