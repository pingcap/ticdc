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

// getTestTableInfo creates a simple TableInfo for testing using EventTestHelper
func getTestTableInfo(t *testing.T, schema, table string) *common.TableInfo {
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

// TestGenSQLInsertWithRouting tests that GenSQL for INSERT uses target schema/table
// when routing is configured via TableInfo.CloneWithRouting().
func TestGenSQLInsertWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "t")

	// Clone TableInfo with routing: test.t -> target_db.target_table
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil, // preValues (nil for INSERT)
		[]interface{}{int64(1), "test_value"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLInsert)
	require.Contains(t, sql, "`target_db`.`target_table`", "INSERT should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "INSERT should not contain source schema.table")
	require.Len(t, args, 2)

	// Test REPLACE
	sql, args = row.GenSQL(DMLReplace)
	require.Contains(t, sql, "`target_db`.`target_table`", "REPLACE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "REPLACE should not contain source schema.table")
	require.Len(t, args, 2)
}

// TestGenSQLDeleteWithRouting tests that GenSQL for DELETE uses target schema/table
func TestGenSQLDeleteWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "test_value"}, // preValues
		nil,                                   // postValues (nil for DELETE)
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLDelete)
	require.Contains(t, sql, "DELETE FROM `target_db`.`target_table`", "DELETE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "DELETE should not contain source schema.table")
	require.Greater(t, len(args), 0)
}

// TestGenSQLUpdateWithRouting tests that GenSQL for UPDATE uses target schema/table
func TestGenSQLUpdateWithRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old_value"},
		[]interface{}{int64(1), "new_value"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLUpdate)
	require.Contains(t, sql, "UPDATE `target_db`.`target_table`", "UPDATE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "UPDATE should not contain source schema.table")
	require.Len(t, args, 3, "UPDATE should have 3 args: new id, new name, old id (WHERE)")
	// Args: SET id=?, name=? WHERE id=?
	require.Equal(t, []interface{}{int64(1), "new_value", int64(1)}, args)
}

// TestGenSQLWithSchemaOnlyRouting tests routing where only schema changes
func TestGenSQLWithSchemaOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "users")
	routedTableInfo := sourceTableInfo.CloneWithRouting("prod", "users")

	// Test INSERT
	insertRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "alice"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema with original table name")
	require.NotContains(t, sql, "`test`.`users`", "Should not contain source schema")

	// Test DELETE
	deleteRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "alice"},
		nil,
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema with original table name")
	require.NotContains(t, sql, "`test`.`users`", "Should not contain source schema")

	// Test UPDATE
	updateRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "alice"},
		[]interface{}{int64(1), "bob"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema with original table name")
	require.NotContains(t, sql, "`test`.`users`", "Should not contain source schema")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "bob", int64(1)}, args)
}

// TestGenSQLWithTableOnlyRouting tests routing where only table name changes
func TestGenSQLWithTableOnlyRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "old_table")
	routedTableInfo := sourceTableInfo.CloneWithRouting("test", "new_table")

	// Test INSERT
	insertRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "data"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "old_table", "Should not contain source table name")

	// Test DELETE
	deleteRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "data"},
		nil,
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "old_table", "Should not contain source table name")

	// Test UPDATE
	updateRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old_data"},
		[]interface{}{int64(1), "new_data"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "old_table", "Should not contain source table name")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "new_data", int64(1)}, args)
}

// TestGenSQLWithoutRouting verifies that when no routing is configured,
// source schema/table names are used.
func TestGenSQLWithoutRouting(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "t")

	// Test INSERT without routing
	insertRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "test"},
		sourceTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")

	// Test DELETE
	deleteRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "test"},
		nil,
		sourceTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")

	// Test UPDATE
	updateRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old"},
		[]interface{}{int64(1), "new"},
		sourceTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "new", int64(1)}, args)
}

// TestTargetTableID verifies that TargetTableID returns the correct quoted target string
func TestTargetTableID(t *testing.T) {
	sourceTableInfo := getTestTableInfo(t, "test", "t")
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "test"},
		routedTableInfo,
		nil,
		nil,
	)

	require.Equal(t, "`target_db`.`target_table`", row.TargetTableID())
}
