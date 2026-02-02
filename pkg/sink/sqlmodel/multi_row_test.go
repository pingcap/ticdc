// Copyright 2024 PingCAP, Inc.
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

package sqlmodel

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// getTestTableInfoForMultiRow creates a simple TableInfo for testing using EventTestHelper
func getTestTableInfoForMultiRow(t *testing.T, schema, table string) *common.TableInfo {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use " + schema)
	createTableSQL := "create table " + table + " (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create a dummy event to get the TableInfo
	insertDataSQL := "insert into " + table + " values (1, 'dummy');"
	event := helper.DML2Event(schema, table, insertDataSQL)
	require.NotNil(t, event)

	return event.TableInfo
}

// TestGenInsertSQLWithRouting tests that GenInsertSQL uses target schema/table
// when routing is configured via TableInfo.CloneWithRouting().
func TestGenInsertSQLWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	// Create 5 row changes to properly test multi-row batch insert
	row1 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(1), "alice"}, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(2), "bob"}, routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(3), "charlie"}, routedTableInfo, nil, nil)
	row4 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(4), "david"}, routedTableInfo, nil, nil)
	row5 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(5), "eve"}, routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3, row4, row5}

	// Test INSERT
	sql, args := GenInsertSQL(DMLInsert, rows...)
	require.Contains(t, sql, "INSERT INTO `target_db`.`target_table`", "INSERT should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "INSERT should not contain source schema.table")
	// Verify all 5 rows' values are in args (2 columns * 5 rows = 10 args)
	require.Len(t, args, 10)
	expectedArgs := []interface{}{
		int64(1), "alice",
		int64(2), "bob",
		int64(3), "charlie",
		int64(4), "david",
		int64(5), "eve",
	}
	require.Equal(t, expectedArgs, args, "INSERT args should contain all row values in order")

	// Test REPLACE
	sql, args = GenInsertSQL(DMLReplace, rows...)
	require.Contains(t, sql, "REPLACE INTO `target_db`.`target_table`", "REPLACE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "REPLACE should not contain source schema.table")
	require.Len(t, args, 10)
	require.Equal(t, expectedArgs, args, "REPLACE args should contain all row values in order")

	// Test INSERT ON DUPLICATE KEY UPDATE
	sql, args = GenInsertSQL(DMLInsertOnDuplicateUpdate, rows...)
	require.Contains(t, sql, "INSERT INTO `target_db`.`target_table`", "INSERT ON DUP should use target schema and table")
	require.Contains(t, sql, "ON DUPLICATE KEY UPDATE", "Should have ON DUPLICATE KEY UPDATE clause")
	require.NotContains(t, sql, "`test`.`t`", "INSERT ON DUP should not contain source schema.table")
	require.Len(t, args, 10)
	require.Equal(t, expectedArgs, args, "INSERT ON DUP args should contain all row values in order")
}

// TestGenDeleteSQLWithRouting tests that GenDeleteSQL uses target schema/table
func TestGenDeleteSQLWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	// Create 4 row changes for delete (preValues only)
	row1 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1), "alice"}, nil, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(2), "bob"}, nil, routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(3), "charlie"}, nil, routedTableInfo, nil, nil)
	row4 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(4), "david"}, nil, routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3, row4}

	sql, args := GenDeleteSQL(rows...)
	require.Contains(t, sql, "DELETE FROM `target_db`.`target_table`", "DELETE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "DELETE should not contain source schema.table")
	// DELETE uses WHERE with primary key (id column only for this table)
	// 4 rows * 1 PK column = 4 args
	require.Len(t, args, 4)
	expectedArgs := []interface{}{int64(1), int64(2), int64(3), int64(4)}
	require.Equal(t, expectedArgs, args, "DELETE args should contain PK values for all rows")
	// Verify OR clauses for multi-row delete
	require.Contains(t, sql, "OR", "Multi-row DELETE should have OR clauses")
}

// TestGenUpdateSQLWithRouting tests that GenUpdateSQL uses target schema/table
func TestGenUpdateSQLWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	// Create 3 row changes for update (both preValues and postValues)
	row1 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1), "alice_old"},
		[]interface{}{int64(1), "alice_new"},
		routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(2), "bob_old"},
		[]interface{}{int64(2), "bob_new"},
		routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(3), "charlie_old"},
		[]interface{}{int64(3), "charlie_new"},
		routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3}

	sql, args := GenUpdateSQL(rows...)
	require.Contains(t, sql, "UPDATE `target_db`.`target_table`", "UPDATE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "UPDATE should not contain source schema.table")
	// UPDATE uses CASE WHEN for multi-row updates
	require.Contains(t, sql, "CASE", "Multi-row UPDATE should use CASE expression")
	require.Contains(t, sql, "WHEN", "Multi-row UPDATE should use WHEN clauses")
	require.Contains(t, sql, "OR", "Multi-row UPDATE WHERE should have OR clauses")
	// Args should contain values for all rows
	// Multi-row UPDATE args: for each column: [WHEN pk=? THEN new_val] repeated for each row, then WHERE pk values
	// 2 columns (id, name) * 3 rows * (1 pk + 1 new_val) + 3 WHERE pk values = 2*3*2 + 3 = 15
	require.Len(t, args, 15, "Multi-row UPDATE should have correct number of args")
	// Verify args contain expected new values
	require.Contains(t, args, "alice_new")
	require.Contains(t, args, "bob_new")
	require.Contains(t, args, "charlie_new")
}

// TestGenInsertSQLWithSchemaOnlyRouting tests routing where only schema changes
func TestGenInsertSQLWithSchemaOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "users")
	routedTableInfo := sourceTableInfo.CloneWithRouting("prod", "users")

	// Create 3 row changes
	row1 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(100), "user1"}, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(200), "user2"}, routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(300), "user3"}, routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3}

	sql, args := GenInsertSQL(DMLInsert, rows...)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema with original table name")
	require.NotContains(t, sql, "`test`.`users`", "Should not contain source schema")
	require.Len(t, args, 6)
	expectedArgs := []interface{}{int64(100), "user1", int64(200), "user2", int64(300), "user3"}
	require.Equal(t, expectedArgs, args)
}

// TestGenInsertSQLWithTableOnlyRouting tests routing where only table name changes
func TestGenInsertSQLWithTableOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "old_table")
	routedTableInfo := sourceTableInfo.CloneWithRouting("test", "new_table")

	// Create 3 row changes
	row1 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(10), "data1"}, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(20), "data2"}, routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(30), "data3"}, routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3}

	sql, args := GenInsertSQL(DMLInsert, rows...)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "old_table", "Should not contain source table name")
	require.Len(t, args, 6)
	expectedArgs := []interface{}{int64(10), "data1", int64(20), "data2", int64(30), "data3"}
	require.Equal(t, expectedArgs, args)
}

// TestGenDeleteSQLWithSchemaOnlyRouting tests DELETE with schema-only routing
func TestGenDeleteSQLWithSchemaOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "orders")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "orders")

	// Create 4 row changes for delete
	row1 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1001), "order1"}, nil, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1002), "order2"}, nil, routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1003), "order3"}, nil, routedTableInfo, nil, nil)
	row4 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1004), "order4"}, nil, routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3, row4}

	sql, args := GenDeleteSQL(rows...)
	require.Contains(t, sql, "`target_db`.`orders`", "Should use target schema with original table name")
	require.NotContains(t, sql, "`test`.`orders`", "Should not contain source schema.table")
	require.Len(t, args, 4)
	expectedArgs := []interface{}{int64(1001), int64(1002), int64(1003), int64(1004)}
	require.Equal(t, expectedArgs, args)
}

// TestGenUpdateSQLWithTableOnlyRouting tests UPDATE with table-only routing
func TestGenUpdateSQLWithTableOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "old_products")
	routedTableInfo := sourceTableInfo.CloneWithRouting("test", "new_products")

	// Create 3 row changes for update
	row1 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(1), "old_name1"},
		[]interface{}{int64(1), "new_name1"},
		routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(2), "old_name2"},
		[]interface{}{int64(2), "new_name2"},
		routedTableInfo, nil, nil)
	row3 := NewRowChange(&routedTableInfo.TableName, nil,
		[]interface{}{int64(3), "old_name3"},
		[]interface{}{int64(3), "new_name3"},
		routedTableInfo, nil, nil)

	rows := []*RowChange{row1, row2, row3}

	sql, args := GenUpdateSQL(rows...)
	require.Contains(t, sql, "`test`.`new_products`", "Should use original schema with target table name")
	require.NotContains(t, sql, "old_products", "Should not contain source table name")
	// 2 columns * 3 rows * 2 (pk + new_val) + 3 WHERE pk values = 15
	require.Len(t, args, 15, "Multi-row UPDATE should have correct number of args")
	require.Contains(t, args, "new_name1")
	require.Contains(t, args, "new_name2")
	require.Contains(t, args, "new_name3")
}

// TestGenMultiRowSQLWithoutRouting verifies that when no routing is configured,
// source schema/table names are used.
func TestGenMultiRowSQLWithoutRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "t")

	// Test INSERT without routing - 3 rows
	row1 := NewRowChange(&sourceTableInfo.TableName, nil, nil,
		[]interface{}{int64(1), "val1"}, sourceTableInfo, nil, nil)
	row2 := NewRowChange(&sourceTableInfo.TableName, nil, nil,
		[]interface{}{int64(2), "val2"}, sourceTableInfo, nil, nil)
	row3 := NewRowChange(&sourceTableInfo.TableName, nil, nil,
		[]interface{}{int64(3), "val3"}, sourceTableInfo, nil, nil)

	insertRows := []*RowChange{row1, row2, row3}
	sql, args := GenInsertSQL(DMLInsert, insertRows...)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")
	require.Len(t, args, 6)
	expectedArgs := []interface{}{int64(1), "val1", int64(2), "val2", int64(3), "val3"}
	require.Equal(t, expectedArgs, args)

	// Test DELETE without routing - 3 rows
	delRow1 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(1), "val1"}, nil, sourceTableInfo, nil, nil)
	delRow2 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(2), "val2"}, nil, sourceTableInfo, nil, nil)
	delRow3 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(3), "val3"}, nil, sourceTableInfo, nil, nil)

	deleteRows := []*RowChange{delRow1, delRow2, delRow3}
	sql, args = GenDeleteSQL(deleteRows...)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")
	require.Len(t, args, 3)
	expectedDeleteArgs := []interface{}{int64(1), int64(2), int64(3)}
	require.Equal(t, expectedDeleteArgs, args)

	// Test UPDATE without routing - 3 rows
	updRow1 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(1), "old1"},
		[]interface{}{int64(1), "new1"},
		sourceTableInfo, nil, nil)
	updRow2 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(2), "old2"},
		[]interface{}{int64(2), "new2"},
		sourceTableInfo, nil, nil)
	updRow3 := NewRowChange(&sourceTableInfo.TableName, nil,
		[]interface{}{int64(3), "old3"},
		[]interface{}{int64(3), "new3"},
		sourceTableInfo, nil, nil)

	updateRows := []*RowChange{updRow1, updRow2, updRow3}
	sql, args = GenUpdateSQL(updateRows...)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")
	require.Len(t, args, 15, "Multi-row UPDATE should have correct number of args")
	require.Contains(t, args, "new1")
	require.Contains(t, args, "new2")
	require.Contains(t, args, "new3")
}

// TestSameTypeTargetAndColumnsWithRouting tests that SameTypeTargetAndColumns
// works correctly when routing is configured.
func TestSameTypeTargetAndColumnsWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfoForMultiRow(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row1 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(1), "alice"}, routedTableInfo, nil, nil)
	row2 := NewRowChange(&routedTableInfo.TableName, nil, nil,
		[]interface{}{int64(2), "bob"}, routedTableInfo, nil, nil)

	// Same source table, same type - should be compatible
	require.True(t, SameTypeTargetAndColumns(row1, row2),
		"Rows with same source table and type should be compatible")
}
