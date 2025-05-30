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

	"github.com/stretchr/testify/require"
)

// TestIsUKChanged tests the IsUKChanged function to verify it can correctly
// determine whether an update type rowChange has modified unique keys.
func TestIsUKChanged(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")

	// Test case 1: Table with single unique key - UK changed
	createTableSQL1 := "create table t1 (id int primary key, name varchar(32), age int, unique key uk_age (age));"
	helper.DDL2Job(createTableSQL1)

	insertSQL1 := "insert into t1 values (1, 'alice', 20);"
	updateSQL1 := "update t1 set age = 25 where id = 1;"
	dmlEvent1, rawKV1 := helper.DML2UpdateEvent("test", "t1", insertSQL1, updateSQL1)

	isChanged, err := IsUKChanged(rawKV1, dmlEvent1.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when unique key column is modified")

	// Test case 2: Table with single unique key - UK not changed
	insertSQL2 := "insert into t1 values (2, 'bob', 30);"
	updateSQL2 := "update t1 set name = 'robert' where id = 2;"
	dmlEvent2, rawKV2 := helper.DML2UpdateEvent("test", "t1", insertSQL2, updateSQL2)

	isChanged, err = IsUKChanged(rawKV2, dmlEvent2.TableInfo)
	require.NoError(t, err)
	require.False(t, isChanged, "UK should not be detected as changed when non-UK column is modified")

	// Test case 3: Table with composite unique key - UK changed
	createTableSQL3 := "create table t3 (id int primary key, name varchar(32), age int, city varchar(32), unique key uk_name_age (name, age));"
	helper.DDL2Job(createTableSQL3)

	insertSQL3 := "insert into t3 values (1, 'charlie', 25, 'shanghai');"
	updateSQL3 := "update t3 set name = 'charles' where id = 1;"
	dmlEvent3, rawKV3 := helper.DML2UpdateEvent("test", "t3", insertSQL3, updateSQL3)

	isChanged, err = IsUKChanged(rawKV3, dmlEvent3.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when one column of composite UK is modified")

	// Test case 4: Table with composite unique key - UK not changed
	insertSQL4 := "insert into t3 values (2, 'david', 28, 'beijing');"
	updateSQL4 := "update t3 set city = 'guangzhou' where id = 2;"
	dmlEvent4, rawKV4 := helper.DML2UpdateEvent("test", "t3", insertSQL4, updateSQL4)

	isChanged, err = IsUKChanged(rawKV4, dmlEvent4.TableInfo)
	require.NoError(t, err)
	require.False(t, isChanged, "UK should not be detected as changed when non-UK column is modified")

	// Test case 5: Table with multiple unique keys - first UK changed
	createTableSQL5 := "create table t5 (id int primary key, email varchar(64), phone varchar(20), name varchar(32), unique key uk_email (email), unique key uk_phone (phone));"
	helper.DDL2Job(createTableSQL5)

	insertSQL5 := "insert into t5 values (1, 'alice@example.com', '1234567890', 'alice');"
	updateSQL5 := "update t5 set email = 'alice.new@example.com' where id = 1;"
	dmlEvent5, rawKV5 := helper.DML2UpdateEvent("test", "t5", insertSQL5, updateSQL5)

	isChanged, err = IsUKChanged(rawKV5, dmlEvent5.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when first unique key is modified")

	// Test case 6: Table with multiple unique keys - second UK changed
	insertSQL6 := "insert into t5 values (2, 'bob@example.com', '0987654321', 'bob');"
	updateSQL6 := "update t5 set phone = '1111111111' where id = 2;"
	dmlEvent6, rawKV6 := helper.DML2UpdateEvent("test", "t5", insertSQL6, updateSQL6)

	isChanged, err = IsUKChanged(rawKV6, dmlEvent6.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when second unique key is modified")

	// Test case 7: Table with multiple unique keys - none changed
	insertSQL7 := "insert into t5 values (3, 'charlie@example.com', '2222222222', 'charlie');"
	updateSQL7 := "update t5 set name = 'charles' where id = 3;"
	dmlEvent7, rawKV7 := helper.DML2UpdateEvent("test", "t5", insertSQL7, updateSQL7)

	isChanged, err = IsUKChanged(rawKV7, dmlEvent7.TableInfo)
	require.NoError(t, err)
	require.False(t, isChanged, "UK should not be detected as changed when non-UK column is modified")

	// Test case 8: Table with no unique keys (only primary key)
	createTableSQL8 := "create table t8 (id int primary key, name varchar(32), age int);"
	helper.DDL2Job(createTableSQL8)

	insertSQL8 := "insert into t8 values (1, 'eve', 22);"
	updateSQL8 := "update t8 set name = 'eva', age = 23 where id = 1;"
	dmlEvent8, rawKV8 := helper.DML2UpdateEvent("test", "t8", insertSQL8, updateSQL8)

	isChanged, err = IsUKChanged(rawKV8, dmlEvent8.TableInfo)
	require.NoError(t, err)
	require.False(t, isChanged, "UK should not be detected as changed when table has no unique keys")

	// Test case 9: Composite UK with partial change (both columns in same UK)
	createTableSQL9 := "create table t9 (id int primary key, first_name varchar(32), last_name varchar(32), age int, unique key uk_full_name (first_name, last_name));"
	helper.DDL2Job(createTableSQL9)

	insertSQL9 := "insert into t9 values (1, 'john', 'doe', 30);"
	updateSQL9 := "update t9 set first_name = 'johnny', last_name = 'smith' where id = 1;"
	dmlEvent9, rawKV9 := helper.DML2UpdateEvent("test", "t9", insertSQL9, updateSQL9)

	isChanged, err = IsUKChanged(rawKV9, dmlEvent9.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when multiple columns of the same UK are modified")

	// Test case 10: UK value changed to same value (should still be detected as changed)
	insertSQL10 := "insert into t1 values (10, 'test', 40);"
	// Update to same value
	updateSQL10 := "update t1 set age = 40 where id = 10;"
	dmlEvent10, rawKV10 := helper.DML2UpdateEvent("test", "t1", insertSQL10, updateSQL10)

	isChanged, err = IsUKChanged(rawKV10, dmlEvent10.TableInfo)
	require.NoError(t, err)
	// Note: This depends on implementation - the function might return false for same values
	// But if it uses string comparison, it might return false as expected
	require.False(t, isChanged, "UK should not be detected as changed when UK value is updated to the same value")

	// Test case 11: UK is declared in the column list
	createTableSQL11 := "create table t11 (id int primary key, name varchar(32), age int unique key);"
	helper.DDL2Job(createTableSQL11)

	insertSQL11 := "insert into t11 values (1, 'alice', 20);"
	updateSQL11 := "update t11 set age = 33 where id = 1;"
	dmlEvent11, rawKV11 := helper.DML2UpdateEvent("test", "t11", insertSQL11, updateSQL11)

	isChanged, err = IsUKChanged(rawKV11, dmlEvent11.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when UK is changed")

	// Test case 12: table with PK, UK, Key and key changed
	createTableSQL12 := "create table t12 (id int primary key, name varchar(32), age int, unique key uk_name (name), key idx_age (age));"
	helper.DDL2Job(createTableSQL12)

	insertSQL12 := "insert into t12 values (1, 'alice', 20);"
	updateSQL12 := "update t12 set age = 30 where id = 1;"
	dmlEvent12, rawKV12 := helper.DML2UpdateEvent("test", "t12", insertSQL12, updateSQL12)

	isChanged, err = IsUKChanged(rawKV12, dmlEvent12.TableInfo)
	require.NoError(t, err)
	require.False(t, isChanged, "UK should not be detected as changed when only key is modified")

	insertSQL13 := "insert into t12 values (2, 'bob', 30);"
	updateSQL13 := "update t12 set name = 'dongmen' where id = 2;"
	dmlEvent13, rawKV13 := helper.DML2UpdateEvent("test", "t12", insertSQL13, updateSQL13)

	isChanged, err = IsUKChanged(rawKV13, dmlEvent13.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when UK is changed")

	// UK is int type and is in the column list
	createTableSQL14 := "create table t14 (id int primary key, name varchar(32), age int unique key);"
	helper.DDL2Job(createTableSQL14)

	insertSQL14 := "insert into t14 values (1, 'alice', 20);"
	updateSQL14 := "update t14 set age = '30' where id = 1;"
	dmlEvent14, rawKV14 := helper.DML2UpdateEvent("test", "t14", insertSQL14, updateSQL14)

	isChanged, err = IsUKChanged(rawKV14, dmlEvent14.TableInfo)
	require.NoError(t, err)
	require.True(t, isChanged, "UK should be detected as changed when UK is changed")
}
