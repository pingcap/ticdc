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
	"github.com/stretchr/testify/require"
)

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
	{CdcVersion: types.CdcVersion{CommitTs: 464043256649875456, OriginTs: 0}, Pk: "038000000000000014", ColumnValues: map[string]any{"first_name": "t", "last_name": "TT", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464043256649875456, OriginTs: 0}, Pk: "038000000000000015", ColumnValues: map[string]any{"first_name": "u", "last_name": "UU", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464073967049113629, OriginTs: 464073966942421014}, Pk: "038000000000000005", ColumnValues: map[string]any{"first_name": "e", "last_name": "E", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464073967049113629, OriginTs: 464073966942421014}, Pk: "038000000000000006", ColumnValues: map[string]any{"first_name": "f", "last_name": "F", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074440664678441, OriginTs: 464074440387592202}, Pk: "038000000000000007", ColumnValues: map[string]any{"first_name": "g", "last_name": "G", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074446196178963, OriginTs: 0}, Pk: "038000000000000007", ColumnValues: map[string]any{"first_name": "g", "last_name": "G", "_tidb_softdelete_time": "2026-02-05 22:58:40.992217"}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074440387592202, OriginTs: 0}, Pk: "038000000000000008", ColumnValues: map[string]any{"first_name": "h", "last_name": "H", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464074446600667164, OriginTs: 464074446196178963}, Pk: "038000000000000008", ColumnValues: map[string]any{"first_name": "h", "last_name": "H", "_tidb_softdelete_time": "2026-02-05 22:58:40.992217"}},
}

func TestCanalJSONDecoder1(t *testing.T) {
	records, err := decoder.Decode([]byte(DataContent1))
	require.NoError(t, err)
	require.Len(t, records, 8)
	for i, actualRecord := range records {
		expectedRecord := ExpectedRecords1[i]
		require.Equal(t, actualRecord.Pk, expectedRecord.Pk)
		require.Equal(t, actualRecord.ColumnValues, expectedRecord.ColumnValues)
		require.Equal(t, actualRecord.CdcVersion.CommitTs, expectedRecord.CdcVersion.CommitTs)
		require.Equal(t, actualRecord.CdcVersion.OriginTs, expectedRecord.CdcVersion.OriginTs)
	}
}

const DataContent2 string = "" +
	`{"id":0,"database":"test_active","table":"message2","pkNames":["id","first_name"],"isDdl":false,"type":"INSERT","es":1770344412751,"ts":1770344413749,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"100","first_name":"a","last_name":"A","_tidb_origin_ts":"464085165262503958","_tidb_softdelete_time":null}],"_tidb":{"commitTs":464085165736198159}}` + "\r\n" +
	`{"id":0,"database":"test_active","table":"message2","pkNames":["id","first_name"],"isDdl":false,"type":"INSERT","es":1770344427851,"ts":1770344429772,"sql":"","sqlType":{"id":4,"first_name":12,"last_name":12,"_tidb_origin_ts":-5,"_tidb_softdelete_time":93},"mysqlType":{"id":"int","first_name":"varchar","last_name":"varchar","_tidb_origin_ts":"bigint","_tidb_softdelete_time":"timestamp"},"old":null,"data":[{"id":"101","first_name":"b","last_name":"B","_tidb_origin_ts":null,"_tidb_softdelete_time":null}],"_tidb":{"commitTs":464085169694572575}}` + "\r\n"

var ExpectedRecords2 = []decoder.Record{
	{CdcVersion: types.CdcVersion{CommitTs: 464085165736198159, OriginTs: 464085165262503958}, Pk: "016100000000000000f8038000000000000064", ColumnValues: map[string]any{"last_name": "A", "_tidb_softdelete_time": nil}},
	{CdcVersion: types.CdcVersion{CommitTs: 464085169694572575, OriginTs: 0}, Pk: "016200000000000000f8038000000000000065", ColumnValues: map[string]any{"last_name": "B", "_tidb_softdelete_time": nil}},
}

func TestCanalJSONDecoder2(t *testing.T) {
	records, err := decoder.Decode([]byte(DataContent2))
	require.NoError(t, err)
	require.Len(t, records, 2)
	for i, actualRecord := range records {
		expectedRecord := ExpectedRecords2[i]
		require.Equal(t, actualRecord.Pk, expectedRecord.Pk)
		require.Equal(t, actualRecord.ColumnValues, expectedRecord.ColumnValues)
		require.Equal(t, actualRecord.CdcVersion.CommitTs, expectedRecord.CdcVersion.CommitTs)
		require.Equal(t, actualRecord.CdcVersion.OriginTs, expectedRecord.CdcVersion.OriginTs)
	}
}

func TestRecord_EqualDownstreamRecord(t *testing.T) {
	tests := []struct {
		name          string
		upstream      *decoder.Record
		downstream    *decoder.Record
		expectedEqual bool
	}{
		{
			name: "equal records",
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
					"col2": 42,
				},
			},
			downstream: &decoder.Record{
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
			name: "downstream is nil",
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			downstream:    nil,
			expectedEqual: false,
		},
		{
			name: "different CommitTs and OriginTs",
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			downstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 200},
				Pk:         "pk1",
			},
			expectedEqual: false,
		},
		{
			name: "different primary keys",
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
			},
			downstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:         "pk2",
			},
			expectedEqual: false,
		},
		{
			name: "different column count",
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			downstream: &decoder.Record{
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
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			downstream: &decoder.Record{
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
			upstream: &decoder.Record{
				CdcVersion: types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:         "pk1",
				ColumnValues: map[string]any{
					"col1": "value1",
				},
			},
			downstream: &decoder.Record{
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
			upstream: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:           "pk1",
				ColumnValues: map[string]any{},
			},
			downstream: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:           "pk1",
				ColumnValues: map[string]any{},
			},
			expectedEqual: true,
		},
		{
			name: "nil column values",
			upstream: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 100, OriginTs: 0},
				Pk:           "pk1",
				ColumnValues: nil,
			},
			downstream: &decoder.Record{
				CdcVersion:   types.CdcVersion{CommitTs: 101, OriginTs: 100},
				Pk:           "pk1",
				ColumnValues: nil,
			},
			expectedEqual: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.upstream.EqualDownstreamRecord(tt.downstream)
			require.Equal(t, tt.expectedEqual, result)
		})
	}
}
