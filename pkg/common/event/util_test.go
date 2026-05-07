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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestIsSplitable(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	testCases := []struct {
		name        string
		createSQL   string
		expected    bool
		description string
	}{
		{
			name:        "table_with_pk_no_uk",
			createSQL:   "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(32));",
			expected:    true,
			description: "Table with primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_pk_and_uk",
			createSQL:   "CREATE TABLE t2 (student_id INT PRIMARY KEY, first_name VARCHAR(50) NOT NULL, last_name VARCHAR(50) NOT NULL, email VARCHAR(100) UNIQUE);",
			expected:    false,
			description: "Table with primary key and unique key should not be splitable",
		},
		{
			name:        "table_with_no_pk",
			createSQL:   "CREATE TABLE t3 (id INT, name VARCHAR(32));",
			expected:    false,
			description: "Table with no primary key should not be splitable",
		},
		{
			name:        "table_with_varchar_pk",
			createSQL:   "CREATE TABLE t4 (id VARCHAR(32) PRIMARY KEY, name VARCHAR(32));",
			expected:    true,
			description: "Table with varchar primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_nonclustered_pk",
			createSQL:   "CREATE TABLE t5 (a VARCHAR(200), b INT, PRIMARY KEY(a) NONCLUSTERED);",
			expected:    true,
			description: "Table with nonclustered primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_composite_pk",
			createSQL:   "CREATE TABLE t6 (a INT, b INT, PRIMARY KEY(a, b));",
			expected:    true,
			description: "Table with composite primary key and no unique key should be splitable",
		},
		{
			name:        "table_with_uk_no_pk",
			createSQL:   "CREATE TABLE t7 (a INT, b INT, UNIQUE KEY(a, b));",
			expected:    false,
			description: "Table with unique key but no primary key should not be splitable",
		},
		{
			name:        "table_with_multiple_uk",
			createSQL:   "CREATE TABLE t8 (id INT PRIMARY KEY, email VARCHAR(100) UNIQUE, phone VARCHAR(20) UNIQUE);",
			expected:    false,
			description: "Table with primary key and multiple unique keys should not be splitable",
		},
		{
			name:        "table_with_pk_and_index",
			createSQL:   "CREATE TABLE t9 (id INT PRIMARY KEY, name VARCHAR(32), INDEX idx_name(name));",
			expected:    true,
			description: "Table with primary key and non-unique index should be splitable",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create table using DDL
			job := helper.DDL2Job(tc.createSQL)
			modelTableInfo := helper.GetModelTableInfo(job)

			// Convert model.TableInfo to common.TableInfo
			commonTableInfo := common.WrapTableInfo("test", modelTableInfo)

			commonResult := IsSplitable(commonTableInfo)

			// Verify the result matches expected value
			require.Equal(t, tc.expected, commonResult,
				"Test case: %s - %s", tc.name, tc.description)
		})
	}
}

func TestDDL2EventFillsSingleTableMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `test_db`")
	helper.Tk().MustExec("CREATE TABLE `test_db`.`t1` (`id` INT PRIMARY KEY)")
	ddlEvent := helper.DDL2Event("ALTER TABLE `test_db`.`t1` ADD COLUMN `c1` INT")

	require.Equal(t, []SchemaTableName{{SchemaName: "test_db", TableName: "t1"}}, ddlEvent.BlockedTableNames)
	require.NotNil(t, ddlEvent.TableInfo)
	require.Equal(t, "test_db", ddlEvent.TableInfo.GetSchemaName())
	require.Equal(t, "t1", ddlEvent.TableInfo.GetTableName())
}

func TestDDL2EventFillsAddIndexMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `test_db`")
	helper.Tk().MustExec("CREATE TABLE `test_db`.`t1` (`id` INT PRIMARY KEY)")
	ddlEvent := helper.DDL2Event("ALTER TABLE `test_db`.`t1` ADD INDEX `idx_id`(`id`)")

	require.Equal(t, byte(timodel.ActionAddIndex), ddlEvent.Type)
	require.Equal(t, "test_db", ddlEvent.SchemaName)
	require.Equal(t, "t1", ddlEvent.TableName)
	require.Equal(t, []SchemaTableName{{SchemaName: "test_db", TableName: "t1"}}, ddlEvent.BlockedTableNames)
	require.NotNil(t, ddlEvent.TableInfo)
	require.Equal(t, "test_db", ddlEvent.TableInfo.GetSchemaName())
	require.Equal(t, "t1", ddlEvent.TableInfo.GetTableName())
}

func TestDDL2EventFillsCreateTableLikeMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `test_db`")
	helper.Tk().MustExec("CREATE TABLE `test_db`.`src` (`id` INT PRIMARY KEY)")
	ddlEvent := helper.DDL2Event("CREATE TABLE `test_db`.`dst` LIKE `test_db`.`src`")

	require.Equal(t, []SchemaTableName{{SchemaName: "test_db", TableName: "src"}}, ddlEvent.BlockedTableNames)
	require.Equal(t, []SchemaTableName{{SchemaName: "test_db", TableName: "dst"}}, ddlEvent.TableNameChange.AddName)
}

func TestDDL2EventFillsRenameTableMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `old_db`")
	helper.Tk().MustExec("CREATE DATABASE `new_db`")
	helper.Tk().MustExec("CREATE TABLE `old_db`.`orders` (`id` INT PRIMARY KEY)")
	ddlEvent := helper.DDL2Event("RENAME TABLE `old_db`.`orders` TO `new_db`.`orders_archive`")

	require.Equal(t, "old_db", ddlEvent.ExtraSchemaName)
	require.Equal(t, "orders", ddlEvent.ExtraTableName)
	require.Equal(t, "new_db", ddlEvent.SchemaName)
	require.Equal(t, "orders_archive", ddlEvent.TableName)
	require.Equal(t, []SchemaTableName{{SchemaName: "old_db", TableName: "orders"}}, ddlEvent.BlockedTableNames)
	require.Equal(t, []SchemaTableName{{SchemaName: "new_db", TableName: "orders_archive"}}, ddlEvent.TableNameChange.AddName)
	require.Equal(t, []SchemaTableName{{SchemaName: "old_db", TableName: "orders"}}, ddlEvent.TableNameChange.DropName)
}

func TestDDL2EventFillsRenameTablesMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `test_db`")
	helper.Tk().MustExec("CREATE TABLE `test_db`.`t1` (`id` INT PRIMARY KEY)")
	helper.Tk().MustExec("CREATE TABLE `test_db`.`t2` (`id` INT PRIMARY KEY)")
	ddlEvent := helper.DDL2Event("RENAME TABLE `test_db`.`t1` TO `test_db`.`t1_new`, `test_db`.`t2` TO `test_db`.`t2_new`")

	require.Len(t, ddlEvent.MultipleTableInfos, 2)
	require.Equal(t, "test_db", ddlEvent.MultipleTableInfos[0].GetSchemaName())
	require.Equal(t, "t1_new", ddlEvent.MultipleTableInfos[0].GetTableName())
	require.Equal(t, "test_db", ddlEvent.MultipleTableInfos[1].GetSchemaName())
	require.Equal(t, "t2_new", ddlEvent.MultipleTableInfos[1].GetTableName())
	require.Equal(t, []SchemaTableName{
		{SchemaName: "test_db", TableName: "t1"},
		{SchemaName: "test_db", TableName: "t2"},
	}, ddlEvent.BlockedTableNames)
	require.Equal(t, []SchemaTableName{
		{SchemaName: "test_db", TableName: "t1_new"},
		{SchemaName: "test_db", TableName: "t2_new"},
	}, ddlEvent.TableNameChange.AddName)
	require.Equal(t, []SchemaTableName{
		{SchemaName: "test_db", TableName: "t1"},
		{SchemaName: "test_db", TableName: "t2"},
	}, ddlEvent.TableNameChange.DropName)
}

func TestBatchCreateTableDDLs2EventFillsMetadata(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("CREATE DATABASE `test_db`")
	ddlEvent := helper.BatchCreateTableDDLs2Event("test_db",
		"CREATE TABLE `test_db`.`t1` (`id` INT PRIMARY KEY)",
		"CREATE TABLE `test_db`.`t2` (`id` INT PRIMARY KEY)",
	)

	require.Equal(t, byte(timodel.ActionCreateTables), ddlEvent.Type)
	require.Equal(t, "test_db", ddlEvent.SchemaName)
	require.Nil(t, ddlEvent.TableInfo)
	require.Len(t, ddlEvent.MultipleTableInfos, 2)
	require.Equal(t, "test_db", ddlEvent.MultipleTableInfos[0].GetSchemaName())
	require.Equal(t, "t1", ddlEvent.MultipleTableInfos[0].GetTableName())
	require.Equal(t, "test_db", ddlEvent.MultipleTableInfos[1].GetSchemaName())
	require.Equal(t, "t2", ddlEvent.MultipleTableInfos[1].GetTableName())
	require.Equal(t, []SchemaTableName{
		{SchemaName: "test_db", TableName: "t1"},
		{SchemaName: "test_db", TableName: "t2"},
	}, ddlEvent.TableNameChange.AddName)
}
