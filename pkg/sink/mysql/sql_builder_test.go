// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// This table has 45 columns
var preCreateTableSQL = `create table t (
	id          int primary key auto_increment,
 
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

var preInsertDataSQL = `insert into t values (
	2,
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

func getRowForTest(t testing.TB) (insert, delete, update commonEvent.RowChange, tableInfo *common.TableInfo) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job(preCreateTableSQL)

	event := helper.DML2Event("test", "t", preInsertDataSQL)
	require.NotNil(t, event)
	insert, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, insert)

	updateSQL := "update t set c_varchar = 'test2' where id = 1;"
	event = helper.DML2Event("test", "t", updateSQL)
	require.NotNil(t, event)
	update, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, update)
	update.PreRow = insert.Row
	update.RowType = common.RowTypeUpdate

	delete = commonEvent.RowChange{
		PreRow:  insert.Row,
		RowType: common.RowTypeDelete,
	}

	return insert, delete, update, event.TableInfo
}

func TestBuildInsert(t *testing.T) {
	insert, _, _, tableInfo := getRowForTest(t)

	exportedArgs := []interface{}{
		// int
		int64(2),
		// tinyint, smallint, mediumint, int, bigint
		int64(1), int64(2), int64(3), int64(4), int64(5),
		// unsigned tinyint, smallint, mediumint, int, bigint
		uint64(1), uint64(2), uint64(3), uint64(4), uint64(5),
		// float, double, decimal, decimal(10, 4)
		float32(2020.0201), float64(2020.0303), "2020", "2021.1208",
		// unsigned float, double, decimal, decimal(10, 4)
		float32(3.1415), float64(2.7182), "8000", "179394.2330",
		// date, datetime, timestamp, time, year
		"2020-02-20", "2020-02-20 02:20:20", "2020-02-20 02:20:20", "02:20:20", int64(2020),
		// tinytext, text, mediumtext, longtext
		"89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A", "89504E470D0A1A0A",
		// tinyblob, blob, mediumblob, longblob
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		// char(16), varchar(16), binary(16), varbinary(16)
		"89504E470D0A1A0A", "89504E470D0A1A0A",
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		[]uint8{0x89, 0x50, 0x4e, 0x47, 0xd, 0xa, 0x1a, 0xa},
		// enum, set, bit,
		uint64(2), uint64(6), uint64(65),
		// json
		"{\"key1\": \"value1\", \"key2\": \"value2\", \"key3\": \"123\"}",
		// gbk: varchar, char, text, text, tinyblob
		"测试", "中国", "上海", "你好,世界",
		[]uint8{0xc4, 0xe3, 0xba, 0xc3, 0xca, 0xc0, 0xbd, 0xe7},
	}

	// case 1: Convert to INSERT INTO
	exportedSQL := "INSERT INTO `test`.`t` (`id`,`c_tinyint`,`c_smallint`,`c_mediumint`,`c_int`,`c_bigint`,`c_unsigned_tinyint`,`c_unsigned_smallint`,`c_unsigned_mediumint`,`c_unsigned_int`,`c_unsigned_bigint`,`c_float`,`c_double`,`c_decimal`,`c_decimal_2`,`c_unsigned_float`,`c_unsigned_double`,`c_unsigned_decimal`,`c_unsigned_decimal_2`,`c_date`,`c_datetime`,`c_timestamp`,`c_time`,`c_year`,`c_tinytext`,`c_text`,`c_mediumtext`,`c_longtext`,`c_tinyblob`,`c_blob`,`c_mediumblob`,`c_longblob`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_enum`,`c_set`,`c_bit`,`c_json`,`name`,`country`,`city`,`description`,`image`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	sql, args := buildInsert(tableInfo, insert, false)
	require.Equal(t, exportedSQL, sql)
	require.Len(t, args, 45)
	require.Equal(t, exportedArgs, args)

	// case 2: Convert to REPLACE INTO
	exportedSQL = "REPLACE INTO `test`.`t` (`id`,`c_tinyint`,`c_smallint`,`c_mediumint`,`c_int`,`c_bigint`,`c_unsigned_tinyint`,`c_unsigned_smallint`,`c_unsigned_mediumint`,`c_unsigned_int`,`c_unsigned_bigint`,`c_float`,`c_double`,`c_decimal`,`c_decimal_2`,`c_unsigned_float`,`c_unsigned_double`,`c_unsigned_decimal`,`c_unsigned_decimal_2`,`c_date`,`c_datetime`,`c_timestamp`,`c_time`,`c_year`,`c_tinytext`,`c_text`,`c_mediumtext`,`c_longtext`,`c_tinyblob`,`c_blob`,`c_mediumblob`,`c_longblob`,`c_char`,`c_varchar`,`c_binary`,`c_varbinary`,`c_enum`,`c_set`,`c_bit`,`c_json`,`name`,`country`,`city`,`description`,`image`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	sql, args = buildInsert(tableInfo, insert, true)
	require.Equal(t, exportedSQL, sql)
	require.Len(t, args, 45)
	require.Equal(t, exportedArgs, args)
}

func TestBuildDelete(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// case 1: delete data from table that has in column primary key
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL := "insert into t values (1, 'test');"
	event := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, event)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	// Manually change row type to delete and set PreRow
	// We do this because the helper does not support delete operation
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL := "DELETE FROM `test`.`t` WHERE `id` = ? LIMIT 1"
	expectedArgs := []interface{}{int64(1)}

	sql, args := buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 1)
	require.Equal(t, expectedArgs, args)

	// case 2: delete data from table that has explicit primary key
	createTableSQL = "create table t2 (id int, name varchar(32), primary key (id));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)
	insertDataSQL = "insert into t2 values (1, 'test');"
	event = helper.DML2Event("test", "t2", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t2` WHERE `id` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 1)
	require.Equal(t, expectedArgs, args)

	// case 3: delete data from table that has composite primary key
	createTableSQL = "create table t3 (id int, name varchar(32), primary key (id, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t3 values (1, 'test');"
	event = helper.DML2Event("test", "t3", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t3` WHERE `id` = ? AND `name` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test"}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 2)
	require.Equal(t, expectedArgs, args)

	// case 4: delete data from table that has composite not null uk
	createTableSQL = "create table t4 (id int, name varchar(32) not null, age int not null, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t4 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t4", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t4` WHERE `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{"test", int64(20)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 2)
	require.Equal(t, expectedArgs, args)

	// case 5: delete data from table that has composite nullable uk
	createTableSQL = "create table t5 (id int, name varchar(32), age int, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t5 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t5", insertDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	row.RowType = common.RowTypeDelete
	row.PreRow = row.Row

	expectedSQL = "DELETE FROM `test`.`t5` WHERE `id` = ? AND `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test", int64(20)}

	sql, args = buildDelete(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 3)
	require.Equal(t, expectedArgs, args)
}

func TestBuildUpdate(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// case 1: table has primary key
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL := "insert into t values (1, 'test');"
	event := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, event)
	oldRow, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, oldRow)

	updateDataSQL := "update t set name = 'test2' where id = 1;"
	event = helper.DML2Event("test", "t", updateDataSQL)
	require.NotNil(t, event)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	// Manually change row type to update and set PreRow
	row.PreRow = oldRow.Row
	row.RowType = common.RowTypeUpdate

	expectedSQL := "UPDATE `test`.`t` SET `id` = ?,`name` = ? WHERE `id` = ? LIMIT 1"
	expectedArgs := []interface{}{int64(1), "test2", int64(1)}
	sql, args := buildUpdate(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 3)
	require.Equal(t, expectedArgs, args)

	// case 2: table has not null uk
	createTableSQL = "create table t2 (id int, name varchar(32) not null, age int not null, unique key (age, name));"
	job = helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	insertDataSQL = "insert into t2 values (1, 'test', 20);"
	event = helper.DML2Event("test", "t2", insertDataSQL)
	require.NotNil(t, event)
	oldRow, ok = event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, oldRow)

	updateDataSQL = "update t2 set name = 'test2' where id = 1;"
	event = helper.DML2Event("test", "t2", updateDataSQL)
	require.NotNil(t, event)
	row, ok = event.GetNextRow()
	require.True(t, ok)
	// Manually change row type to update and set PreRow
	row.PreRow = oldRow.Row
	row.RowType = common.RowTypeUpdate

	expectedSQL = "UPDATE `test`.`t2` SET `id` = ?,`name` = ?,`age` = ? WHERE `name` = ? AND `age` = ? LIMIT 1"
	expectedArgs = []interface{}{int64(1), "test2", int64(20), "test", int64(20)}
	sql, args = buildUpdate(event.TableInfo, row)
	require.Equal(t, expectedSQL, sql)
	require.Len(t, args, 5)
	require.Equal(t, expectedArgs, args)
}

func TestBuildDMLTableRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		createTableSQL  string
		insertSQL       string
		updateSQL       string
		sourceSchema    string
		sourceTable     string
		targetSchema    string
		targetTable     string
		expectedTableID string
		forbidden       string
	}{
		{
			name:            "full route",
			createTableSQL:  "create table t (id int primary key, name varchar(32));",
			insertSQL:       "insert into t values (1, 'test');",
			updateSQL:       "update t set name = 'test2' where id = 1;",
			sourceSchema:    "test",
			sourceTable:     "t",
			targetSchema:    "target_db",
			targetTable:     "target_table",
			expectedTableID: "`target_db`.`target_table`",
			forbidden:       "`test`.`t`",
		},
		{
			name:            "schema only route",
			createTableSQL:  "create table users (id int primary key, name varchar(32));",
			insertSQL:       "insert into users values (1, 'alice');",
			updateSQL:       "update users set name = 'bob' where id = 1;",
			sourceSchema:    "test",
			sourceTable:     "users",
			targetSchema:    "prod",
			targetTable:     "users",
			expectedTableID: "`prod`.`users`",
			forbidden:       "`test`.`users`",
		},
		{
			name:            "table only route",
			createTableSQL:  "create table old_table (id int primary key, value varchar(32));",
			insertSQL:       "insert into old_table values (1, 'data');",
			updateSQL:       "update old_table set value = 'newdata' where id = 1;",
			sourceSchema:    "test",
			sourceTable:     "old_table",
			targetSchema:    "test",
			targetTable:     "new_table",
			expectedTableID: "`test`.`new_table`",
			forbidden:       "old_table",
		},
		{
			name:            "no route",
			createTableSQL:  "create table plain_table (id int primary key, name varchar(32));",
			insertSQL:       "insert into plain_table values (1, 'test');",
			updateSQL:       "update plain_table set name = 'test2' where id = 1;",
			sourceSchema:    "test",
			sourceTable:     "plain_table",
			targetSchema:    "test",
			targetTable:     "plain_table",
			expectedTableID: "`test`.`plain_table`",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			helper := commonEvent.NewEventTestHelper(t)
			defer helper.Close()

			helper.Tk().MustExec("use test")
			job := helper.DDL2Job(tc.createTableSQL)
			require.NotNil(t, job)

			insertEvent := helper.DML2Event(tc.sourceSchema, tc.sourceTable, tc.insertSQL)
			require.NotNil(t, insertEvent)

			tableInfo := insertEvent.TableInfo
			if tc.targetSchema != tc.sourceSchema || tc.targetTable != tc.sourceTable {
				tableInfo = tableInfo.CloneWithRouting(tc.targetSchema, tc.targetTable)
				tableInfo.InitPrivateFields()
			}

			insertRow, ok := insertEvent.GetNextRow()
			require.True(t, ok)

			sql, args := buildInsert(tableInfo, insertRow, false)
			require.Contains(t, sql, "INSERT INTO "+tc.expectedTableID)
			require.Len(t, args, 2)

			sql, args = buildInsert(tableInfo, insertRow, true)
			require.Contains(t, sql, "REPLACE INTO "+tc.expectedTableID)
			require.Len(t, args, 2)

			deleteRow := commonEvent.RowChange{
				PreRow:  insertRow.Row,
				RowType: common.RowTypeDelete,
			}
			sql, _ = buildDelete(tableInfo, deleteRow)
			require.Contains(t, sql, "DELETE FROM "+tc.expectedTableID)

			updateEvent := helper.DML2Event(tc.sourceSchema, tc.sourceTable, tc.updateSQL)
			require.NotNil(t, updateEvent)
			updateRow, ok := updateEvent.GetNextRow()
			require.True(t, ok)
			updateRow.PreRow = insertRow.Row
			updateRow.RowType = common.RowTypeUpdate

			sql, _ = buildUpdate(tableInfo, updateRow)
			require.Contains(t, sql, "UPDATE "+tc.expectedTableID)
			if tc.forbidden != "" {
				require.NotContains(t, sql, tc.forbidden)
			}
		})
	}
}
