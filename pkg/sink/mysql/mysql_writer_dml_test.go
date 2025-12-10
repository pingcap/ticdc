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

package mysql

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// TestShouldGenBatchSQL tests the shouldGenBatchSQL function
func TestShouldGenBatchSQL(t *testing.T) {
	t.Parallel()

	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()

	tests := []struct {
		name           string
		hasPK          bool
		hasVirtualCols bool
		events         []*commonEvent.DMLEvent
		config         *Config
		safemode       bool
		want           bool
	}{
		{
			name:           "table without primary key should not use batch SQL",
			hasPK:          false,
			hasVirtualCols: false,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: true},
			want:           false,
		},
		{
			name:           "table with virtual columns should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: true,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: true},
			want:           false,
		},
		{
			name:           "single row event should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 1, 1),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   false,
		},
		{
			name:           "all rows in same safe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 2, 2),
				newDMLEvent(t, 2, 3, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "multiple rows with primary key in different safe mode should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
				newDMLEvent(t, 1, 2, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   false,
		},
		{
			name:           "multiple rows with primary key in unsafe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
				newDMLEvent(t, 3, 1, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "global safe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
			},
			config: &Config{SafeMode: true, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "batch dml is disabled",
			hasPK:          true,
			hasVirtualCols: false,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: false},
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := writer.shouldGenBatchSQL(tt.hasPK, tt.hasVirtualCols, tt.events)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateBatchSQL(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	writer.cfg.MaxTxnRow = 2
	writer.cfg.SafeMode = false
	dmlInsertEvent := helper.DML2Event("test", "t", "insert into t values (16, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 16")
	dmlInsertEvent2 := helper.DML2Event("test", "t", "insert into t values (17, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 17")
	dmlInsertEvent3 := helper.DML2Event("test", "t", "insert into t values (18, 'test')")
	sql, args := writer.generateBatchSQL([]*commonEvent.DMLEvent{dmlInsertEvent, dmlInsertEvent2, dmlInsertEvent3})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?)", sql[0])
	require.Equal(t, []interface{}{int64(16), "test", int64(17), "test"}, args[0])
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[1])
	require.Equal(t, []interface{}{int64(18), "test"}, args[1])

	writer.cfg.SafeMode = true
	writer.cfg.MaxTxnRow = 3
	sql, args = writer.generateBatchSQL([]*commonEvent.DMLEvent{dmlInsertEvent, dmlInsertEvent2, dmlInsertEvent3})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?)", sql[0])
	expected1 := []interface{}{int64(16), "test", int64(17), "test", int64(18), "test"}
	expected2 := []interface{}{int64(16), "test", int64(18), "test", int64(17), "test"}
	expected3 := []interface{}{int64(17), "test", int64(16), "test", int64(18), "test"}
	expected4 := []interface{}{int64(17), "test", int64(18), "test", int64(16), "test"}
	expected5 := []interface{}{int64(18), "test", int64(16), "test", int64(17), "test"}
	expected6 := []interface{}{int64(18), "test", int64(17), "test", int64(16), "test"}
	require.True(t, (reflect.DeepEqual(expected1, args[0])) ||
		(reflect.DeepEqual(expected2, args[0])) ||
		(reflect.DeepEqual(expected3, args[0])) ||
		(reflect.DeepEqual(expected4, args[0])) ||
		(reflect.DeepEqual(expected5, args[0])) ||
		(reflect.DeepEqual(expected6, args[0])),
		"args[0] should be one of the expected combinations: %v", args[0])

	// Test performance with 1000 rows event
	// Generate 1000 insert statements
	var insertStatements []string
	for i := 1000; i < 2000; i++ {
		insertStatements = append(insertStatements, fmt.Sprintf("insert into t values (%d, 'test%d')", i, i))
	}

	// Create a single event with 1000 rows
	dmlEvent := helper.DML2Event("test", "t", insertStatements...)
	require.Equal(t, int32(1000), dmlEvent.Length, "Event should contain 1000 rows")

	// Set configuration for batch processing
	writer.cfg.MaxTxnRow = 1000
	writer.cfg.SafeMode = false

	// Measure execution time
	start := time.Now()
	sql, args = writer.generateBatchSQL([]*commonEvent.DMLEvent{dmlEvent})
	duration := time.Since(start)

	// Verify performance requirement
	require.Less(t, duration, 500*time.Millisecond, "generateBatchSQL should complete within 500ms, took %v", duration)

	// Verify the generated SQL is correct
	require.Equal(t, 1, len(sql), "Should generate 1 SQL statement for 1000 rows")
	require.Equal(t, 1, len(args), "Should generate 1 args slice for 1000 rows")

	// Verify SQL statement format
	expectedSQL := "INSERT INTO `test`.`t` (`id`,`name`) VALUES "
	require.True(t, strings.HasPrefix(sql[0], expectedSQL), "SQL should start with INSERT statement")

	// Count the number of value placeholders
	valueCount := strings.Count(sql[0], "?")
	require.Equal(t, 2000, valueCount, "Should have 2000 placeholders (1000 rows * 2 columns)")

	// Verify args length
	require.Equal(t, 2000, len(args[0]), "Should have 2000 arguments (1000 rows * 2 columns)")
}

func TestGenerateBatchSQLInUnSafeMode(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Delete A + insert A
	dmlDeleteEvent := helper.DML2DeleteEvent("test", "t", "insert into t values (1, 'test')", "delete from t where id = 1")
	dmlInsertEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	sql, args := writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(1)}, args[0])
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[1])
	require.Equal(t, []interface{}{int64(1), "test"}, args[1])

	// Delete A + Update A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (2, 'test')", "delete from t where id = 2")
	dmlUpdateEvent, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (2, 'test')", "update t set name = 'test2' where id = 2")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlUpdateEvent})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(2)}, args[0])
	require.Equal(t, "UPDATE `test`.`t` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1", sql[1])

	// Insert A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (3, 'test')", "delete from t where id = 3")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (3, 'test')")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(3)}, args[0])

	// Insert A + Update A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (4, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 4")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (4, 'test')", "update t set name = 'test4' where id = 4")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(4), "test4"}, args[0])

	// Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (5, 'test5')", "delete from t where id = 5")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (5, 'test')", "update t set name = 'test5' where id = 5")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlUpdateEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(5)}, args[0])

	// Update A + Update A
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (6, 'test')", "update t set name = 'test6' where id = 6")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 6")
	dmlUpdateEvent2, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (6, 'test6')", "update t set name = 'test7' where id = 6")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlUpdateEvent, dmlUpdateEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "UPDATE `test`.`t` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1", sql[0])
	require.Equal(t, []interface{}{int64(6), "test7", int64(6)}, args[0])

	// Delete A  + Insert A + Delete A + Insert A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (7, 'test')", "delete from t where id = 7")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (7, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 7")
	dmlInsertEvent2 := helper.DML2Event("test", "t", "insert into t values (7, 'test2')")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlDeleteEvent, dmlInsertEvent2})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?) OR (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(7), int64(7)}, args[0])
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[1])
	require.Equal(t, []interface{}{int64(7), "test2"}, args[1])

	// Delete A + Insert A + Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (8, 'test')", "delete from t where id = 8")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (8, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 8")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (8, 'test')", "update t set name = 'test8' where id = 8")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 8")
	dmlDeleteEvent2 := helper.DML2DeleteEvent("test", "t", "insert into t values (8, 'test8')", "delete from t where id = 8")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlUpdateEvent, dmlDeleteEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?) OR (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(8), int64(8)}, args[0])

	// Delete A + Insert A + Update A  + Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (9, 'test')", "delete from t where id = 9")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (9, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (9, 'test')", "update t set name = 'test9' where id = 9")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlUpdateEvent2, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (9, 'test9')", "update t set name = 'test10' where id = 9")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlDeleteEvent2 = helper.DML2DeleteEvent("test", "t", "insert into t values (9, 'test10')", "delete from t where id = 9")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlUpdateEvent, dmlUpdateEvent2, dmlDeleteEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?) OR (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(9), int64(9)}, args[0])

	// Insert A + Delete A + Insert A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (10, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 10")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (10, 'test')", "delete from t where id = 10")
	dmlInsertEvent2 = helper.DML2Event("test", "t", "insert into t values (10, 'test2')")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent, dmlInsertEvent2})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(10)}, args[0])
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[1])
	require.Equal(t, []interface{}{int64(10), "test2"}, args[1])

	// Insert A + Update A + Delete A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (11, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 11")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (11, 'test')", "update t set name = 'test11' where id = 11")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 11")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (11, 'test11')", "delete from t where id = 11")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(11)}, args[0])

	// Insert A + Update A + Update A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (12, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 12")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (12, 'test')", "update t set name = 'test12' where id = 12")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 12")
	dmlUpdateEvent2, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (12, 'test12')", "update t set name = 'test13' where id = 12")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent, dmlUpdateEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(12), "test13"}, args[0])

	// Insert A + Delete B + Update C
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (13, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 13")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (14, 'test')", "delete from t where id = 14")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (15, 'test')", "update t set name = 'test15' where id = 15")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent, dmlUpdateEvent})
	require.Equal(t, 3, len(sql))
	require.Equal(t, 3, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(14)}, args[0])
	require.Equal(t, "UPDATE `test`.`t` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1", sql[1])
	require.Equal(t, []interface{}{int64(15), "test15", int64(15)}, args[1])
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[2])
	require.Equal(t, []interface{}{int64(13), "test"}, args[2])

	// Insert A + Insert B + Insert C
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (16, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 16")
	dmlInsertEvent2 = helper.DML2Event("test", "t", "insert into t values (17, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 17")
	dmlInsertEvent3 := helper.DML2Event("test", "t", "insert into t values (18, 'test')")
	sql, args = writer.generateBatchSQLInUnSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlInsertEvent2, dmlInsertEvent3})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "INSERT INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?)", sql[0])
	require.Equal(t, []interface{}{int64(16), "test", int64(17), "test", int64(18), "test"}, args[0])
}

func TestGenerateBatchSQLInSafeMode(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Delete A + insert A
	dmlDeleteEvent := helper.DML2DeleteEvent("test", "t", "insert into t values (1, 'test')", "delete from t where id = 1")
	dmlInsertEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	sql, args := writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(1), "test"}, args[0])

	// Insert A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (3, 'test')", "delete from t where id = 3")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (3, 'test')")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(3)}, args[0])

	// Insert A + Update A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (4, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 4")
	dmlUpdateEvent, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (4, 'test')", "update t set name = 'test4' where id = 4")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(4), "test4"}, args[0])

	// Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (5, 'test5')", "delete from t where id = 5")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (5, 'test')", "update t set name = 'test5' where id = 5")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlUpdateEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(5)}, args[0])

	// Update A + Update A
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (6, 'test')", "update t set name = 'test6' where id = 6")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 6")
	dmlUpdateEvent2, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (6, 'test6')", "update t set name = 'test7' where id = 6")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlUpdateEvent, dmlUpdateEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(6), "test7"}, args[0])

	// Delete A  + Insert A + Delete A + Insert A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (7, 'test')", "delete from t where id = 7")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (7, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 7")
	dmlInsertEvent2 := helper.DML2Event("test", "t", "insert into t values (7, 'test2')")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlDeleteEvent, dmlInsertEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(7), "test2"}, args[0])

	// Delete A + Insert A + Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (8, 'test')", "delete from t where id = 8")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (8, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 8")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (8, 'test')", "update t set name = 'test8' where id = 8")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 8")
	dmlDeleteEvent2 := helper.DML2DeleteEvent("test", "t", "insert into t values (8, 'test8')", "delete from t where id = 8")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlUpdateEvent, dmlDeleteEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(8)}, args[0])

	// Delete A + Insert A + Update A  + Update A + Delete A
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (9, 'test')", "delete from t where id = 9")
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (9, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (9, 'test')", "update t set name = 'test9' where id = 9")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlUpdateEvent2, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (9, 'test9')", "update t set name = 'test10' where id = 9")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 9")
	dmlDeleteEvent2 = helper.DML2DeleteEvent("test", "t", "insert into t values (9, 'test10')", "delete from t where id = 9")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlDeleteEvent, dmlInsertEvent, dmlUpdateEvent, dmlUpdateEvent2, dmlDeleteEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(9)}, args[0])

	// Insert A + Delete A + Insert A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (10, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 10")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (10, 'test')", "delete from t where id = 10")
	dmlInsertEvent2 = helper.DML2Event("test", "t", "insert into t values (10, 'test2')")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent, dmlInsertEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(10), "test2"}, args[0])

	// Insert A + Update A + Delete A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (11, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 11")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (11, 'test')", "update t set name = 'test11' where id = 11")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 11")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (11, 'test11')", "delete from t where id = 11")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent, dmlDeleteEvent})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(11)}, args[0])

	// Insert A + Update A + Update A
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (12, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 12")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (12, 'test')", "update t set name = 'test12' where id = 12")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 12")
	dmlUpdateEvent2, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (12, 'test12')", "update t set name = 'test13' where id = 12")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlUpdateEvent, dmlUpdateEvent2})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?)", sql[0])
	require.Equal(t, []interface{}{int64(12), "test13"}, args[0])

	// Insert A + Delete B + Update C
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (13, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 13")
	dmlDeleteEvent = helper.DML2DeleteEvent("test", "t", "insert into t values (14, 'test')", "delete from t where id = 14")
	dmlUpdateEvent, _ = helper.DML2UpdateEvent("test", "t", "insert into t values (15, 'test')", "update t set name = 'test15' where id = 15")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlDeleteEvent, dmlUpdateEvent})
	require.Equal(t, 2, len(sql))
	require.Equal(t, 2, len(args))
	require.Equal(t, "DELETE FROM `test`.`t` WHERE (`id` = ?)", sql[0])
	require.Equal(t, []interface{}{int64(14)}, args[0])
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?)", sql[1])
	// The order of args in unsafe mode is not deterministic due to map iteration
	// Check that both expected combinations are possible
	expected1 := []interface{}{int64(13), "test", int64(15), "test15"}
	expected2 := []interface{}{int64(15), "test15", int64(13), "test"}
	require.Equal(t, 4, len(args[1]), "args[1] should have 4 elements")
	require.True(t, (reflect.DeepEqual(expected1, args[1])) || (reflect.DeepEqual(expected2, args[1])),
		"args[1] should be one of the expected combinations: %v", args[1])

	// Insert A + Insert B + Insert C
	dmlInsertEvent = helper.DML2Event("test", "t", "insert into t values (16, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 16")
	dmlInsertEvent2 = helper.DML2Event("test", "t", "insert into t values (17, 'test')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 17")
	dmlInsertEvent3 := helper.DML2Event("test", "t", "insert into t values (18, 'test')")
	sql, args = writer.generateBatchSQLInSafeMode([]*commonEvent.DMLEvent{dmlInsertEvent, dmlInsertEvent2, dmlInsertEvent3})
	require.Equal(t, 1, len(sql))
	require.Equal(t, 1, len(args))
	require.Equal(t, "REPLACE INTO `test`.`t` (`id`,`name`) VALUES (?,?),(?,?),(?,?)", sql[0])
	expected1 = []interface{}{int64(16), "test", int64(17), "test", int64(18), "test"}
	expected2 = []interface{}{int64(16), "test", int64(18), "test", int64(17), "test"}
	expected3 := []interface{}{int64(17), "test", int64(16), "test", int64(18), "test"}
	expected4 := []interface{}{int64(17), "test", int64(18), "test", int64(16), "test"}
	expected5 := []interface{}{int64(18), "test", int64(16), "test", int64(17), "test"}
	expected6 := []interface{}{int64(18), "test", int64(17), "test", int64(16), "test"}
	require.True(t, (reflect.DeepEqual(expected1, args[0])) ||
		(reflect.DeepEqual(expected2, args[0])) ||
		(reflect.DeepEqual(expected3, args[0])) ||
		(reflect.DeepEqual(expected4, args[0])) ||
		(reflect.DeepEqual(expected5, args[0])) ||
		(reflect.DeepEqual(expected6, args[0])),
		"args[0] should be one of the expected combinations: %v", args[0])
}

// newDMLEvent creates a mock DMLEvent for testing
func newDMLEvent(_ *testing.T, commitTs, replicatingTs, rowCount uint64) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		CommitTs:      commitTs,
		ReplicatingTs: replicatingTs,
		Length:        int32(rowCount),
	}
}

func TestGenerateBatchSQLWithDifferentTableVersion(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	// Step 1: Create table with 2 columns
	createTableSQL := "create table t (id int primary key, name varchar(32), age int);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	writer.cfg.MaxTxnRow = 10
	writer.cfg.SafeMode = false

	// Step 2: Create 2 insert events with 3 columns
	dmlInsertEvent1 := helper.DML2Event("test", "t", "insert into t values (1, 'test1', 20)")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 1")
	dmlInsertEvent2 := helper.DML2Event("test", "t", "insert into t values (2, 'test2', 25)")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 2")
	// set table info version
	dmlInsertEvent1.TableInfoVersion = job.BinlogInfo.FinishedTS
	dmlInsertEvent2.TableInfoVersion = job.BinlogInfo.FinishedTS

	// tableInfo1 := dmlInsertEvent1.TableInfo

	// rawKvs := helper.DML2RawKv(job.BinlogInfo.TableInfo.ID, job.BinlogInfo.FinishedTS, "test", "t", "insert into t values (20, 'testRawKV', 20)", "insert into t values (21, 'testRawKV2', 21)")

	// Step 3: Drop the age column
	dropColumnSQL := "alter table t drop column age;"
	dropJob := helper.DDL2Job(dropColumnSQL)
	require.NotNil(t, dropJob)

	// Step 4: Create 2 more insert events with 2 columns (after drop)
	dmlInsertEvent3 := helper.DML2Event("test", "t", "insert into t values (3, 'test3')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 3")
	dmlInsertEvent4 := helper.DML2Event("test", "t", "insert into t values (4, 'test4')")
	helper.ExecuteDeleteDml("test", "t", "delete from t where id = 4")
	// set table info version
	dmlInsertEvent3.TableInfoVersion = dropJob.BinlogInfo.FinishedTS
	dmlInsertEvent4.TableInfoVersion = dropJob.BinlogInfo.FinishedTS

	// chink1 := chunk.NewChunkWithCapacity(tableInfo1.GetFieldSlice(), 1)

	// Step 5: Try to put all 4 events in one group and call generateBatchSQL
	// This should potentially cause a panic due to different table versions
	events := []*commonEvent.DMLEvent{dmlInsertEvent1, dmlInsertEvent2, dmlInsertEvent3, dmlInsertEvent4}

	// This should panic since events have different table schema
	require.Panics(t, func() {
		writer.generateBatchSQL(events)
	})

	// This should not return an error instead of panic since we grouped the events by table version
	dmls, err := writer.prepareDMLs(events)
	require.NoError(t, err)
	require.NotNil(t, dmls)
}

// TestAllRowInSameSafeMode tests the allRowInSameSafeMode function which determines
// if all rows in a batch of DML events have the same safe mode status
func TestAllRowInSameSafeMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		safemode bool
		events   []*commonEvent.DMLEvent
		want     bool
	}{
		{
			name:     "global safe mode enabled",
			safemode: true,
			events:   []*commonEvent.DMLEvent{newDMLEvent(t, 2, 1, 1)},
			want:     true,
		},
		{
			name:     "all events have same safe mode status (all CommitTs > ReplicatingTs)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1),
				newDMLEvent(t, 3, 2, 1),
			},
			want: true,
		},
		{
			name:     "all events have same safe mode status (all CommitTs <= ReplicatingTs)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 1, 1),
				newDMLEvent(t, 1, 2, 1),
			},
			want: true,
		},
		{
			name:     "events have mixed safe mode status",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1), // CommitTs > ReplicatingTs
				newDMLEvent(t, 1, 2, 1), // CommitTs < ReplicatingTs
			},
			want: false,
		},
		{
			name:     "events have mixed safe mode status (equal case)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1), // CommitTs > ReplicatingTs
				newDMLEvent(t, 2, 2, 1), // CommitTs = ReplicatingTs
			},
			want: false,
		},
		{
			name:     "empty events array",
			safemode: false,
			events:   []*commonEvent.DMLEvent{},
			want:     false,
		},
		{
			name:     "single event",
			safemode: false,
			events:   []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			want:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := allRowInSameSafeMode(tt.safemode, tt.events)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestGroupEventsByTable(t *testing.T) {
	t.Run("empty events", func(t *testing.T) {
		result := groupEventsByTable([]*commonEvent.DMLEvent{})
		require.Equal(t, 0, len(result))
	})

	t.Run("single event", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		createTableSQL := "create table t (id int primary key, name varchar(32));"
		job := helper.DDL2Job(createTableSQL)
		require.NotNil(t, job)

		// Create a real event using the helper
		event := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
		events := []*commonEvent.DMLEvent{event}

		result := groupEventsByTable(events)

		// Should have 1 table group
		require.Equal(t, 1, len(result))

		// Get the table ID and verify the structure
		tableID := event.GetTableID()
		tableGroups, exists := result[tableID]
		require.True(t, exists, "Table ID should exist in result")
		require.Equal(t, 1, len(tableGroups), "Should have 1 update timestamp group")
		require.Equal(t, 1, len(tableGroups[0]), "Should have 1 event in the group")
		require.Equal(t, event, tableGroups[0][0])
	})

	t.Run("multiple events different tables", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")

		// Create first table
		createTableSQL1 := "create table t1 (id int primary key, name varchar(32));"
		job1 := helper.DDL2Job(createTableSQL1)
		require.NotNil(t, job1)

		// Create second table
		createTableSQL2 := "create table t2 (id int primary key, name varchar(32), age int);"
		job2 := helper.DDL2Job(createTableSQL2)
		require.NotNil(t, job2)

		// Create events with different tables
		event1 := helper.DML2Event("test", "t1", "insert into t1 values (1, 'test1')")
		event2 := helper.DML2Event("test", "t2", "insert into t2 values (2, 'test2', 20)")

		events := []*commonEvent.DMLEvent{event1, event2}
		result := groupEventsByTable(events)

		// Should have 2 table groups
		require.Equal(t, 2, len(result))

		// Check table 1 group
		tableID1 := event1.GetTableID()
		tableGroups1, exists1 := result[tableID1]
		require.True(t, exists1, "Table 1 ID should exist in result")
		require.Equal(t, 1, len(tableGroups1), "Table 1 should have 1 update timestamp group")
		require.Equal(t, 1, len(tableGroups1[0]), "Table 1 should have 1 event in the group")
		require.Equal(t, event1, tableGroups1[0][0])

		// Check table 2 group
		tableID2 := event2.GetTableID()
		tableGroups2, exists2 := result[tableID2]
		require.True(t, exists2, "Table 2 ID should exist in result")
		require.Equal(t, 1, len(tableGroups2), "Table 2 should have 1 update timestamp group")
		require.Equal(t, 1, len(tableGroups2[0]), "Table 2 should have 1 event in the group")
		require.Equal(t, event2, tableGroups2[0][0])
	})

	t.Run("multiple events same table same update timestamp", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		createTableSQL := "create table t (id int primary key, name varchar(32));"
		job := helper.DDL2Job(createTableSQL)
		require.NotNil(t, job)

		// Create events with same table and same update timestamp
		event1 := helper.DML2Event("test", "t", "insert into t values (1, 'test1')")
		helper.ExecuteDeleteDml("test", "t", "delete from t where id = 1")
		event2 := helper.DML2Event("test", "t", "insert into t values (2, 'test2')")
		helper.ExecuteDeleteDml("test", "t", "delete from t where id = 2")
		event3 := helper.DML2Event("test", "t", "insert into t values (3, 'test3')")

		events := []*commonEvent.DMLEvent{event1, event2, event3}
		result := groupEventsByTable(events)

		// Should have 1 table group
		require.Equal(t, 1, len(result))

		// Get the table ID and verify the structure
		tableID := event1.GetTableID()
		tableGroups, exists := result[tableID]
		require.True(t, exists, "Table ID should exist in result")
		require.Equal(t, 1, len(tableGroups), "Should have 1 update timestamp group")
		require.Equal(t, 3, len(tableGroups[0]), "Should have 3 events in the group")

		// Check that all events are in the group
		eventSet := make(map[*commonEvent.DMLEvent]bool)
		for _, event := range tableGroups[0] {
			eventSet[event] = true
		}
		require.True(t, eventSet[event1], "Event1 should be in the group")
		require.True(t, eventSet[event2], "Event2 should be in the group")
		require.True(t, eventSet[event3], "Event3 should be in the group")
	})

	t.Run("events with different table versions should be grouped separately", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")

		// Create table with 3 columns
		createTableSQL := "create table t (id int primary key, name varchar(32), age int);"
		job1 := helper.DDL2Job(createTableSQL)
		require.NotNil(t, job1)

		// Create events with original table structure
		event1 := helper.DML2Event("test", "t", "insert into t values (1, 'test1', 20)")
		helper.ExecuteDeleteDml("test", "t", "delete from t where id = 1")
		event2 := helper.DML2Event("test", "t", "insert into t values (2, 'test2', 25)")
		helper.ExecuteDeleteDml("test", "t", "delete from t where id = 2")

		// Drop column to create new table version
		dropColumnSQL := "alter table t drop column age;"
		job2 := helper.DDL2Job(dropColumnSQL)
		require.NotNil(t, job2)

		// Create events with new table structure
		event3 := helper.DML2Event("test", "t", "insert into t values (3, 'test3')")
		helper.ExecuteDeleteDml("test", "t", "delete from t where id = 3")
		event4 := helper.DML2Event("test", "t", "insert into t values (4, 'test4')")

		events := []*commonEvent.DMLEvent{event1, event2, event3, event4}
		result := groupEventsByTable(events)

		// Should have 1 table group (same table ID) but 2 update timestamp groups
		require.Equal(t, 1, len(result))

		// Get the table ID and verify the structure
		tableID := event1.GetTableID()
		tableGroups, exists := result[tableID]
		require.True(t, exists, "Table ID should exist in result")
		require.Equal(t, 2, len(tableGroups), "Should have 2 update timestamp groups")

		// Check that groups are sorted by update timestamp (ascending)
		updateTS1 := event1.TableInfo.GetUpdateTS()
		updateTS2 := event3.TableInfo.GetUpdateTS()
		require.True(t, updateTS1 < updateTS2, "First group should have smaller update timestamp")

		// First group should contain events 1 and 2 (old table version)
		require.Equal(t, 2, len(tableGroups[0]), "First group should have 2 events")
		// Second group should contain events 3 and 4 (new table version)
		require.Equal(t, 2, len(tableGroups[1]), "Second group should have 2 events")
	})
}

func TestPrepareDMLsWithNotNullUniqueKey(t *testing.T) {
	writer, _, _ := newTestMysqlWriter(t)
	defer writer.db.Close()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t3 (a int not null unique key, b int);"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// enable batch DML and set limits
	writer.cfg.BatchDMLEnable = true
	writer.cfg.SafeMode = false
	writer.cfg.MaxTxnRow = 10

	// prepare a few insert events for table t3
	dml1 := helper.DML2Event("test", "t3", "insert into t3 values (1, 10)")
	helper.ExecuteDeleteDml("test", "t3", "delete from t3 where a = 1")
	dml2 := helper.DML2Event("test", "t3", "insert into t3 values (2, 20)")
	helper.ExecuteDeleteDml("test", "t3", "delete from t3 where a = 2")
	dml3 := helper.DML2Event("test", "t3", "insert into t3 values (3, 30)")
	helper.ExecuteDeleteDml("test", "t3", "delete from t3 where a = 3")

	events := []*commonEvent.DMLEvent{dml1, dml2, dml3}

	dmls, err := writer.prepareDMLs(events)
	require.NoError(t, err)
	require.NotNil(t, dmls)

	// Expect at least one batched INSERT for t3
	found := false
	for i, q := range dmls.sqls {
		if strings.HasPrefix(q, "INSERT INTO `test`.`t3`") || strings.HasPrefix(q, "REPLACE INTO `test`.`t3`") {
			found = true
			// check corresponding args length: 3 rows * 2 cols = 6
			require.Equal(t, 6, len(dmls.values[i]))
			// check placeholders count consistent with args
			placeholderCount := strings.Count(q, "?")
			require.Equal(t, 6, placeholderCount)
			break
		}
	}
	require.True(t, found, "expected batched INSERT/REPLACE for table t3")
}
