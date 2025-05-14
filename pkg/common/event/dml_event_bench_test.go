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

func createBatchDMLEvent(b *testing.B, dmlNum, rowNum int) {
	helper := NewEventTestHelper(b)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(ddl)
	require.NotNil(b, ddlJob)
	batchDMLEvent := new(BatchDMLEvent)
	tableInfo := helper.GetTableInfo(ddlJob)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	rawKvs := helper.DML2RawKv("test", "t", ts, dml)
	b.ReportAllocs()
	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		for i := 0; i < dmlNum; i++ {
			batchDMLEvent.AppendDMLEvent(did, tableInfo.TableName.TableID, ts-1, ts+1, tableInfo)
			for j := 0; j < rowNum; j++ {
				for _, rawKV := range rawKvs {
					err := batchDMLEvent.AppendRow(rawKV, helper.mounter.DecodeToChunk)
					require.NoError(b, err)
				}
			}
		}
	}
}

func createDMLEvents(b *testing.B, dmlNum, rowNum int) {
	helper := NewEventTestHelper(b)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(ddl)
	require.NotNil(b, ddlJob)
	tableInfo := helper.GetTableInfo(ddlJob)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	rawKvs := helper.DML2RawKv("test", "t", ts, dml)
	b.ReportAllocs()
	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		for i := 0; i < dmlNum; i++ {
			event := newDMLEvent(did, tableInfo.TableName.TableID, ts-1, ts+1, tableInfo)
			event.Rows = chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), defaultRowCount)
			for j := 0; j < rowNum; j++ {
				for _, rawKV := range rawKvs {
					err := event.AppendRow(rawKV, helper.mounter.DecodeToChunk)
					require.NoError(b, err)
				}
			}
		}
	}
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkBatchDMLEvent_100000-72    	       1	5509822685 ns/op	517733800 B/op	 1483694 allocs/op
func BenchmarkBatchDMLEvent_100000(b *testing.B) {
	createBatchDMLEvent(b, 100000, 1)
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkDMLEvents_100000-72    	       1	6356767072 ns/op	912358968 B/op	20444518 allocs/op
func BenchmarkDMLEvents_100000(b *testing.B) {
	createDMLEvents(b, 100000, 1)
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkBatchDMLEvent_1000000-72    	       1	55271596300 ns/op	5128786216 B/op	14807291 allocs/op
func BenchmarkBatchDMLEvent_1000000(b *testing.B) {
	createBatchDMLEvent(b, 1000000, 1)
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkDMLEvents_1000000-72    	       1	59282774010 ns/op	8879427720 B/op	203074980 allocs/op
func BenchmarkDMLEvents_1000000(b *testing.B) {
	createDMLEvents(b, 1000000, 1)
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkBatchDMLEvent_100000x10-72    	       1	54880057654 ns/op	4887498240 B/op	13188528 allocs/op
func BenchmarkBatchDMLEvent_100000x10(b *testing.B) {
	createBatchDMLEvent(b, 100000, 10)
}

// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkDMLEvents_100000x10-72    	       1	53243358758 ns/op	3956960392 B/op	59664667 allocs/op
func BenchmarkDMLEvents_100000x10(b *testing.B) {
	createDMLEvents(b, 100000, 10)
}
