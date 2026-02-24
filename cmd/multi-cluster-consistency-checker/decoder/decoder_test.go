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

package decoder_test

import (
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/decoder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

// buildColumnFieldTypes converts a TableDefinition into a map of column name → FieldType,
// mimicking what SchemaDefinitions.SetSchemaDefinition does.
func buildColumnFieldTypes(t *testing.T, td *cloudstorage.TableDefinition) map[string]*ptypes.FieldType {
	t.Helper()
	result := make(map[string]*ptypes.FieldType, len(td.Columns))
	for i, col := range td.Columns {
		colInfo, err := col.ToTiColumnInfo(int64(i))
		require.NoError(t, err)
		result[col.Name] = &colInfo.FieldType
	}
	return result
}

// DataContent uses CRLF (\r\n) as line terminator to match the codec config
const DataContent1 string = "" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770184540709,"ts":1770184542274,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp","id":"int","first_name":"varchar"},"old":null,"data":[{"id":"20","first_name":"t","last_name":"TT","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"_tidb":{"commitTs":464043256649875456}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770184540709,"ts":1770184542274,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"21","first_name":"u","last_name":"UU","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"_tidb":{"commitTs":464043256649875456}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770301693150,"ts":1770301693833,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp","id":"int"},"old":null,"data":[{"id":"5","first_name":"e","last_name":"E","_tidb_origin_ts":"464073966942421014","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464073967049113629}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770301693150,"ts":1770301693833,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"_tidb_softdelete_time":"timestamp","id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint"},"old":null,"data":[{"id":"6","first_name":"f","last_name":"F","_tidb_origin_ts":"464073966942421014","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464073967049113629}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770303499850,"ts":1770303500498,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"7","first_name":"g","last_name":"G","_tidb_origin_ts":"464074440387592202","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464074440664678441}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"UPDATE","es":1770303520951,"ts":1770303522531,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp","id":"int","first_name":"varchar"},"old":[{"id":"7","first_name":"g","last_name":"G","_tidb_origin_ts":"464074440387592202","_tidb_softdelete_time":null}],"data":[{"id":"7","first_name":"g","last_name":"G","_tidb_origin_ts":null,"_tidb_softdelete_time":"2026-02-05 22:58:40.992217"}],"_tidb":{"commitTs":464074446196178963}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1770303498793,"ts":1770303499864,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"8","first_name":"h","last_name":"H","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"_tidb":{"commitTs":464074440387592202}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message","pkNames":["id"],"isDdl":false,"type":"UPDATE","es":1770303522494,"ts":1770303523900,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":[{"id":"8","first_name":"h","last_name":"H","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"data":[{"id":"8","first_name":"h","last_name":"H","_tidb_origin_ts":"464074446196178963","_tidb_softdelete_time":"2026-02-05 22:58:40.992217"}],"_tidb":{"commitTs":464074446600667164}}`

var ExpectedRecords1 = []decoder.Record{
	{CdcVersion: types.CdcVersion{CommitTs: 464043256649875456, OriginTs: 0}, Pk: "038000000000000014", PkStr: "[id: 20]", ColumnValues: map[string]any{"first_name": "t", "last_name": "TT", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464043256649875456, OriginTs: 0}, Pk: "038000000000000015", PkStr: "[id: 21]", ColumnValues: map[string]any{"first_name": "u", "last_name": "UU", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464073967049113629, OriginTs: 464073966942421014}, Pk: "038000000000000005", PkStr: "[id: 5]", ColumnValues: map[string]any{"first_name": "e", "last_name": "E", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464073967049113629, OriginTs: 464073966942421014}, Pk: "038000000000000006", PkStr: "[id: 6]", ColumnValues: map[string]any{"first_name": "f", "last_name": "F", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074440664678441, OriginTs: 464074440387592202}, Pk: "038000000000000007", PkStr: "[id: 7]", ColumnValues: map[string]any{"first_name": "g", "last_name": "G", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074446196178963, OriginTs: 0}, Pk: "038000000000000007", PkStr: "[id: 7]", ColumnValues: map[string]any{"first_name": "g", "last_name": "G", "_tidb_softdelete_time": "2026-02-05 22:58:40.992217"}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074440387592202, OriginTs: 0}, Pk: "038000000000000008", PkStr: "[id: 8]", ColumnValues: map[string]any{"first_name": "h", "last_name": "H", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074446600667164, OriginTs: 464074446196178963}, Pk: "038000000000000008", PkStr: "[id: 8]", ColumnValues: map[string]any{"first_name": "h", "last_name": "H", "_tidb_softdelete_time": "2026-02-05 22:58:40.992217"}},
}

// tableDefinition1 describes the "message" table: id(INT PK), first_name(VARCHAR), last_name(VARCHAR),
// _tidb_origin_ts(BIGINT), _tidb_softdelete_time(TIMESTAMP)
var tableDefinition1 = &cloudstorage.TableDefinition{
	Table:   "message",
	Schema:  "test_active",
	Version: 1,
	Columns: []cloudstorage.TableCol{
		{Name: "id", Tp: "INT", IsPK: "true", Precision: "11"},
		{Name: "first_name", Tp: "VARCHAR", Precision: "255"},
		{Name: "last_name", Tp: "VARCHAR", Precision: "255"},
		{Name: "_tidb_origin_ts", Tp: "BIGINT", Precision: "20"},
		{Name: "_tidb_softdelete_time", Tp: "TIMESTAMP"},
	},
	TotalColumns: 5,
}

func TestCanalJSONDecoder1(t *testing.T) {
	records, err := decoder.Decode([]byte(DataContent1), buildColumnFieldTypes(t, tableDefinition1))
	require.NoError(t, err)
	require.Len(t, records, 8)
	for i, actualRecord := range records {
		expectedRecord := ExpectedRecords1[i]
		require.Equal(t, actualRecord.Pk, expectedRecord.Pk)
		require.Equal(t, actualRecord.PkStr, expectedRecord.PkStr)
		require.Equal(t, actualRecord.ColumnValues, expectedRecord.ColumnValues)
		require.Equal(t, actualRecord.CdcVersion.CommitTs, expectedRecord.CdcVersion.CommitTs)
		require.Equal(t, actualRecord.CdcVersion.OriginTs, expectedRecord.CdcVersion.OriginTs)
	}
}

const DataContent2 string = "" +
	`{"id":0,"database":"test_active","table":"message2","pkNames":["id","first_name"],"isDdl":false,"type":"INSERT","es":1770344412751,"ts":1770344413749,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"100","first_name":"a","last_name":"A","_tidb_origin_ts":"464085165262503958","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464085165736198159}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message2","pkNames":["id","first_name"],"isDdl":false,"type":"INSERT","es":1770344427851,"ts":1770344429772,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"101","first_name":"b","last_name":"B","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"_tidb":{"commitTs":464085169694572575}}` + "\r\n"

var ExpectedRecords2 = []decoder.Record{
	{CdcVersion: types.CdcVersion{CommitTs: 464085165736198159, OriginTs: 464085165262503958}, Pk: "016100000000000000f8038000000000000064", PkStr: "[first_name: a, id: 100]", ColumnValues: map[string]any{"last_name": "A", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464085169694572575, OriginTs: 0}, Pk: "016200000000000000f8038000000000000065", PkStr: "[first_name: b, id: 101]", ColumnValues: map[string]any{"last_name": "B", "_tidb_softdelete_time": nil}},
}

// tableDefinition2 describes the "message2" table: id(INT PK), first_name(VARCHAR PK), last_name(VARCHAR),
// _tidb_origin_ts(BIGINT), _tidb_softdelete_time(TIMESTAMP)
var tableDefinition2 = &cloudstorage.TableDefinition{
	Table:   "message2",
	Schema:  "test_active",
	Version: 1,
	Columns: []cloudstorage.TableCol{
		{Name: "id", Tp: "INT", IsPK: "true", Precision: "11"},
		{Name: "first_name", Tp: "VARCHAR", IsPK: "true", Precision: "255"},
		{Name: "last_name", Tp: "VARCHAR", Precision: "255"},
		{Name: "_tidb_origin_ts", Tp: "BIGINT", Precision: "20"},
		{Name: "_tidb_softdelete_time", Tp: "TIMESTAMP"},
	},
	TotalColumns: 5,
}

func TestCanalJSONDecoder2(t *testing.T) {
	records, err := decoder.Decode([]byte(DataContent2), buildColumnFieldTypes(t, tableDefinition2))
	require.NoError(t, err)
	require.Len(t, records, 2)
	for i, actualRecord := range records {
		expectedRecord := ExpectedRecords2[i]
		require.Equal(t, actualRecord.Pk, expectedRecord.Pk)
		require.Equal(t, actualRecord.PkStr, expectedRecord.PkStr)
		require.Equal(t, actualRecord.ColumnValues, expectedRecord.ColumnValues)
		require.Equal(t, actualRecord.CdcVersion.CommitTs, expectedRecord.CdcVersion.CommitTs)
		require.Equal(t, actualRecord.CdcVersion.OriginTs, expectedRecord.CdcVersion.OriginTs)
	}
}

// TestCanalJSONDecoderWithInvalidMessage verifies that when a malformed message appears in
// the data stream, it is skipped gracefully and subsequent valid messages are still decoded.
// This covers the fix where d.msg is cleared to nil on unmarshal failure to prevent stale
// message data from leaking into decodeNext.
func TestCanalJSONDecoderWithInvalidMessage(t *testing.T) {
	// First line is invalid JSON, second line is a valid message.
	dataWithInvalidLine := `{invalid json}` + "\r\n" +
		`{"id":0,"database":"test_active","table":"message2","pkNames":["id","first_name"],"isDdl":false,"type":"INSERT","es":1770344412751,"ts":1770344413749,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"100","first_name":"a","last_name":"A","_tidb_origin_ts":"464085165262503958","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464085165736198159}}` + "\r\n"

	records, err := decoder.Decode([]byte(dataWithInvalidLine), buildColumnFieldTypes(t, tableDefinition2))
	require.NoError(t, err)
	// The invalid line should be skipped, only the valid record should be returned.
	require.Len(t, records, 1)
	require.Equal(t, ExpectedRecords2[0].Pk, records[0].Pk)
	require.Equal(t, ExpectedRecords2[0].PkStr, records[0].PkStr)
	require.Equal(t, ExpectedRecords2[0].CdcVersion.CommitTs, records[0].CdcVersion.CommitTs)
	require.Equal(t, ExpectedRecords2[0].CdcVersion.OriginTs, records[0].CdcVersion.OriginTs)
}

// TestCanalJSONDecoderAllInvalidMessages verifies that when all messages are malformed,
// the decoder returns an empty result without errors.
func TestCanalJSONDecoderAllInvalidMessages(t *testing.T) {
	allInvalid := `{broken}` + "\r\n" + `{also broken}` + "\r\n"
	records, err := decoder.Decode([]byte(allInvalid), nil)
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestRecord_EqualReplicatedRecord(t *testing.T) {
	tests := []struct {
		name          string
		local         *decoder.Record
		replicated    *decoder.Record
		expectedEqual bool
	}{
		{
			name: "equal records",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
					"col2": 42,
				},
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
					"col2": 42,
				},
			},
			expectedEqual: true,
		},
		{
			name: "replicated is nil",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			replicated:    nil,
			expectedEqual: false,
		},
		{
			name: "different CommitTs and OriginTs",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 200},
				Pk:         "pk1",
			},
			expectedEqual: false,
		},
		{
			name: "different primary keys",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk2",
			},
			expectedEqual: false,
		},
		{
			name: "different column count",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
					"col2": "value2",
				},
			},
			expectedEqual: false,
		},
		{
			name: "different column names",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col2": "value1",
				},
			},
			expectedEqual: false,
		},
		{
			name: "different column values",
			local: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			replicated: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value2",
				},
			},
			expectedEqual: false,
		},
		{
			name: "empty column values",
			local: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:           "pk1",
				ColumnValues: map[string]any{},
			},
			replicated: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:           "pk1",
				ColumnValues: map[string]any{},
			},
			expectedEqual: true,
		},
		{
			name: "nil column values",
			local: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:           "pk1",
				ColumnValues: nil,
			},
			replicated: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:           "pk1",
				ColumnValues: nil,
			},
			expectedEqual: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.local.EqualReplicatedRecord(tt.replicated)
			require.Equal(t, tt.expectedEqual, result)
		})
	}
}
