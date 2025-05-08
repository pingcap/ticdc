// Copyright 2025 PingCAP, Inc.
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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// TestDMLEvent test the Marshal and Unmarshal of DMLEvent.
func TestDMLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	dmlEvent.State = EventSenderStatePaused
	require.NotNil(t, dmlEvent)

	batchDMLEvent := &BatchDMLEvent{
		DMLEvents: []*DMLEvent{dmlEvent},
		Rows:      dmlEvent.Rows,
		TableInfo: dmlEvent.TableInfo,
	}
	data, err := batchDMLEvent.Marshal()
	require.NoError(t, err)

	reverseEvents := &BatchDMLEvent{}
	// Set the TableInfo before unmarshal, it is used in Unmarshal.
	err = reverseEvents.Unmarshal(data)
	require.NoError(t, err)
	reverseEvents.AssembleRows()
	require.Equal(t, len(reverseEvents.DMLEvents), 1)
	reverseEvent := reverseEvents.DMLEvents[0]
	// Compare the content of the two event's rows.
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	require.True(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	require.Equal(t, dmlEvent.TableInfo.GetFieldSlice(), reverseEvent.TableInfo.GetFieldSlice())
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}

func TestEncodeAndDecodeV0(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.encodeV0()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before decode, it is used in decode.
	err = reverseEvent.decodeV0(data)
	require.NoError(t, err)

	require.False(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	require.Equal(t, dmlEvent.TableInfo.GetFieldSlice(), reverseEvent.TableInfo.GetFieldSlice())
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}

var ddl = `create table t (
	c_tinyint   tinyint   null,
	c_smallint  smallint  null,
	c_mediumint mediumint null,
	c_int       int       null,
	c_bigint    bigint    null,
 
	c_unsigned_tinyint   tinyint   unsigned null,
	c_unsigned_smallint  smallint  unsigned null,
	c_unsigned_mediumint mediumint unsigned null,
	c_unsigned_int       int       unsigned null,
	c_unsigned_bigint    bigint    unsigned null,
 
	c_float   float   null,
	c_double  double  null,
	c_decimal decimal null,
	c_decimal_2 decimal(10, 4) null,
 
	c_unsigned_float     float unsigned   null,
	c_unsigned_double    double unsigned  null,
	c_unsigned_decimal   decimal unsigned null,
	c_unsigned_decimal_2 decimal(10, 4) unsigned null,
 
	c_date      date      null,
	c_datetime  datetime  null,
	c_timestamp timestamp null,
	c_time      time      null,
	c_year      year      null,
 
	c_tinytext   tinytext      null,
	c_text       text          null,
	c_mediumtext mediumtext    null,
	c_longtext   longtext      null,
 
	c_tinyblob   tinyblob      null,
	c_blob       blob          null,
	c_mediumblob mediumblob    null,
	c_longblob   longblob      null,
 
	c_char       char(16)      null,
	c_varchar    varchar(16)   null,
	c_binary     binary(16)    null,
	c_varbinary  varbinary(16) null,
 
	c_enum enum ('a','b','c') null,
	c_set  set ('a','b','c')  null,
	c_bit  bit(64)            null,
	c_json json               null,
 
 -- gbk dmls
	name varchar(128) CHARACTER SET gbk,
	country char(32) CHARACTER SET gbk,
	city varchar(64),
	description text CHARACTER SET gbk,
	image tinyblob
 );`

var dml = `insert into t values (
	1, 2, 3, 4, 5,
	1, 2, 3, 4, 5,
	2020.0202, 2020.0303,
	  2020.0404, 2021.1208,
	3.1415, 2.7182, 8000, 179394.233,
	'2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
	'89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
	x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
	'89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
	'b', 'b,c', b'1000001', '{
"key1": "value1",
"key2": "value2",
"key3": "123"
}',
	'测试', "中国", "上海", "你好,世界", 0xC4E3BAC3CAC0BDE7
);`

func createBatchDMLEvent(b *testing.B, dmlNum, rowsNum int) *BatchDMLEvent {
	helper := NewEventTestHelper(b)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(ddl)
	require.NotNil(b, ddlJob)
	batchDMLEvent := new(BatchDMLEvent)
	tableInfo := helper.GetTableInfo(ddlJob)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	for i := 0; i < dmlNum; i++ {
		event := NewDMLEvent(did, tableInfo.TableName.TableID, ts-1, ts+1, tableInfo)
		batchDMLEvent.AppendDMLEvent(event)
		for j := 0; j < rowsNum; j++ {
			rawKvs := helper.DML2RawKv("test", "t", dml)
			for _, rawKV := range rawKvs {
				err := batchDMLEvent.AppendRow(rawKV, helper.mounter.DecodeToChunk)
				require.NoError(b, err)
			}
		}
	}
	return batchDMLEvent
}

func createDMLEvents(b *testing.B, dmlNum, rowsNum int) []*DMLEvent {
	helper := NewEventTestHelper(b)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(ddl)
	require.NotNil(b, ddlJob)
	dmlEvents := make([]*DMLEvent, 0, dmlNum)
	tableInfo := helper.GetTableInfo(ddlJob)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	for i := 0; i < dmlNum; i++ {
		event := NewDMLEvent(did, tableInfo.TableName.TableID, ts-1, ts+1, tableInfo)
		event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), defaultRowCount)
		for j := 0; j < rowsNum; j++ {
			rawKvs := helper.DML2RawKv("test", "t", dml)
			for _, rawKV := range rawKvs {
				err := event.AppendRow(rawKV, helper.mounter.DecodeToChunk)
				require.NoError(b, err)
			}
		}
		dmlEvents = append(dmlEvents, event)
	}
	return dmlEvents
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkBatchDMLEvent_10x100-72    	       1	9149234977 ns/op	871514104 B/op	 5857191 allocs/op
func BenchmarkBatchDMLEvent_10x100(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createBatchDMLEvent(b, 10, 100)
	}
}

// BenchmarkBatchDMLEvent_100x1-72    	       1	3566853403 ns/op	272869856 B/op	 1310711 allocs/op
func BenchmarkBatchDMLEvent_100x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createBatchDMLEvent(b, 100, 1)
	}
}

// BenchmarkBatchDMLEvent_1000x1-72    	       1	9537108405 ns/op	875037128 B/op	 5881643 allocs/op
func BenchmarkBatchDMLEvent_1000x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createBatchDMLEvent(b, 1000, 1)
	}
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkBatchDMLEvent_10000x1-72    	       1	322898121910 ns/op	46365201192 B/op	344590675 allocs/op
func BenchmarkBatchDMLEvent_10000x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createBatchDMLEvent(b, 10000, 1)
	}
}

func BenchmarkDMLEvents_100x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createDMLEvents(b, 100, 1)
	}
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkDMLEvents_1000x1-72    	       1	9304230708 ns/op	875918640 B/op	 6032678 allocs/op
func BenchmarkDMLEvents_1000x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createDMLEvents(b, 1000, 1)
	}
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkDMLEvents_10000x1-72    	       1	328285483293 ns/op	46424896304 B/op	346822452 allocs/op
func BenchmarkDMLEvents_10000x1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		createDMLEvents(b, 10000, 1)
	}
}
