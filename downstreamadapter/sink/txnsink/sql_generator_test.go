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

package txnsink

import (
	"strings"
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestSQLGenerator_ConvertTxnGroupToSQL(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create test DML events
	event1 := helper.DML2Event("test", "t", "insert into t values (1, 'test1')")
	event1.CommitTs = 100
	event1.StartTs = 50

	event2 := helper.DML2Event("test", "t", "insert into t values (2, 'test2')")
	event2.CommitTs = 100
	event2.StartTs = 50

	// Create transaction group
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{event1, event2},
	}

	// Convert to SQL
	txnSQL, err := generator.ConvertTxnGroupToSQL(txnGroup)
	require.NoError(t, err)
	require.NotNil(t, txnSQL)

	// Verify transaction structure
	require.Equal(t, txnGroup, txnSQL.TxnGroup)
	require.NotEmpty(t, txnSQL.SQL)

	// Verify SQL format: should start with BEGIN and end with COMMIT
	sql := txnSQL.SQL
	require.True(t, strings.HasPrefix(sql, "BEGIN;"))
	require.True(t, strings.HasSuffix(sql, ";COMMIT;"))

	// Should contain INSERT ON DUPLICATE KEY UPDATE
	require.Contains(t, sql, "INSERT INTO")
	require.Contains(t, sql, "ON DUPLICATE KEY UPDATE")
}

func TestSQLGenerator_ConvertTxnGroupToSQL_EmptyGroup(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()

	// Create empty transaction group
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	// Convert to SQL
	txnSQL, err := generator.ConvertTxnGroupToSQL(txnGroup)
	require.NoError(t, err)
	require.NotNil(t, txnSQL)

	// Should have empty SQL
	require.Empty(t, txnSQL.SQL)
}

func TestSQLGenerator_ConvertTxnGroupToSQL_MultiTable(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test tables
	helper.Tk().MustExec("use test")
	createTableSQL1 := "create table t1 (id int primary key, name varchar(32));"
	job1 := helper.DDL2Job(createTableSQL1)
	require.NotNil(t, job1)

	createTableSQL2 := "create table t2 (id int primary key, age int);"
	job2 := helper.DDL2Job(createTableSQL2)
	require.NotNil(t, job2)

	// Create events for different tables
	event1 := helper.DML2Event("test", "t1", "insert into t1 values (1, 'test1')")
	event1.CommitTs = 100
	event1.StartTs = 50

	event2 := helper.DML2Event("test", "t2", "insert into t2 values (1, 25)")
	event2.CommitTs = 100
	event2.StartTs = 50

	// Create transaction group
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{event1, event2},
	}

	// Convert to SQL
	txnSQL, err := generator.ConvertTxnGroupToSQL(txnGroup)
	require.NoError(t, err)
	require.NotNil(t, txnSQL)

	// Should have one transaction SQL
	require.NotEmpty(t, txnSQL.SQL)
	sql := txnSQL.SQL

	// Should contain both tables
	require.Contains(t, sql, "`test`.`t1`")
	require.Contains(t, sql, "`test`.`t2`")
	require.True(t, strings.HasPrefix(sql, "BEGIN;"))
	require.True(t, strings.HasSuffix(sql, ";COMMIT;"))
}

func TestSQLGenerator_groupRowsByType(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create different types of events
	insertEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test1')")
	updateEvent, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (2, 'test2')", "update t set name = 'updated' where id = 2")
	deleteEvent := helper.DML2DeleteEvent("test", "t", "insert into t values (3, 'test3')", "delete from t where id = 3")

	events := []*commonEvent.DMLEvent{insertEvent, updateEvent, deleteEvent}
	tableInfo := insertEvent.TableInfo

	// Group rows by type
	insertRows, updateRows, deleteRows := generator.groupRowsByType(events, tableInfo)

	// Verify grouping
	require.Len(t, insertRows, 1) // One batch of insert rows
	require.Len(t, updateRows, 1) // One batch of update rows
	require.Len(t, deleteRows, 1) // One batch of delete rows

	// Verify row counts in each batch
	require.Len(t, insertRows[0], 1) // One insert row
	require.Len(t, updateRows[0], 1) // One update row
	require.Len(t, deleteRows[0], 1) // One delete row
}

func TestSQLGenerator_generateTableSQL(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create test events
	insertEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test1')")
	deleteEvent := helper.DML2DeleteEvent("test", "t", "insert into t values (2, 'test2')", "delete from t where id = 2")

	events := []*commonEvent.DMLEvent{insertEvent, deleteEvent}

	// Generate table SQL
	sqls, args, err := generator.generateTableSQL(events)
	require.NoError(t, err)

	// Should generate 2 SQL statements (one DELETE, one INSERT ON DUPLICATE KEY UPDATE)
	require.Len(t, sqls, 2)
	require.Len(t, args, 2)

	// Verify SQL types
	foundDelete := false
	foundInsert := false

	for _, sql := range sqls {
		if strings.Contains(sql, "DELETE FROM") {
			foundDelete = true
		}
		if strings.Contains(sql, "INSERT INTO") && strings.Contains(sql, "ON DUPLICATE KEY UPDATE") {
			foundInsert = true
		}
	}

	require.True(t, foundDelete, "Should generate DELETE SQL")
	require.True(t, foundInsert, "Should generate INSERT ON DUPLICATE KEY UPDATE SQL")
}

func TestSQLGenerator_generateTableSQL_EmptyEvents(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()

	// Test with empty events
	sqls, args, err := generator.generateTableSQL([]*commonEvent.DMLEvent{})
	require.NoError(t, err)
	require.Len(t, sqls, 0)
	require.Len(t, args, 0)
}

func TestSQLGenerator_getArgsWithGeneratedColumn(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create test event
	event := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	tableInfo := event.TableInfo

	// Get first row
	event.Rewind()
	row, ok := event.GetNextRow()
	require.True(t, ok)

	// Extract arguments
	args := generator.getArgsWithGeneratedColumn(&row.Row, tableInfo)

	// Should extract arguments for all columns
	require.Equal(t, len(tableInfo.GetColumns()), len(args))
}

// Test mixed operations in the same transaction
func TestSQLGenerator_MixedOperations(t *testing.T) {
	t.Parallel()

	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	// Create mixed events: insert, update, delete
	insertEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test1')")
	updateEvent, _ := helper.DML2UpdateEvent("test", "t", "insert into t values (2, 'test2')", "update t set name = 'updated' where id = 2")
	deleteEvent := helper.DML2DeleteEvent("test", "t", "insert into t values (3, 'test3')", "delete from t where id = 3")

	// Set same transaction timestamps
	insertEvent.CommitTs = 100
	insertEvent.StartTs = 50
	updateEvent.CommitTs = 100
	updateEvent.StartTs = 50
	deleteEvent.CommitTs = 100
	deleteEvent.StartTs = 50

	// Create transaction group
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{insertEvent, updateEvent, deleteEvent},
	}

	// Convert to SQL
	txnSQL, err := generator.ConvertTxnGroupToSQL(txnGroup)
	require.NoError(t, err)
	require.NotNil(t, txnSQL)

	// Should have one transaction SQL
	require.NotEmpty(t, txnSQL.SQL)
	sql := txnSQL.SQL

	// Verify transaction format
	require.True(t, strings.HasPrefix(sql, "BEGIN;"))
	require.True(t, strings.HasSuffix(sql, ";COMMIT;"))

	// Should contain all operation types
	require.Contains(t, sql, "DELETE FROM")
	require.Contains(t, sql, "INSERT INTO")
	require.Contains(t, sql, "ON DUPLICATE KEY UPDATE")
}

// Benchmark tests for SQL generation
func BenchmarkSQLGenerator_ConvertTxnGroupToSQL(b *testing.B) {
	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(b)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(b, job)

	// Create multiple events
	events := make([]*commonEvent.DMLEvent, 100)
	for i := 0; i < 100; i++ {
		events[i] = helper.DML2Event("test", "t", "insert into t values (?, 'test')")
		events[i].CommitTs = 100
		events[i].StartTs = 50
	}

	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   events,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := generator.ConvertTxnGroupToSQL(txnGroup)
		require.NoError(b, err)
	}
}

func BenchmarkSQLGenerator_groupRowsByType(b *testing.B) {
	generator := NewSQLGenerator()
	helper := commonEvent.NewEventTestHelper(b)
	defer helper.Close()

	// Setup test table
	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(b, job)

	// Create mixed events
	events := make([]*commonEvent.DMLEvent, 1000)
	for i := 0; i < 1000; i++ {
		switch i % 3 {
		case 0:
			events[i] = helper.DML2Event("test", "t", "insert into t values (?, 'test')")
		case 1:
			events[i], _ = helper.DML2UpdateEvent("test", "t", "insert into t values (?, 'test')", "update t set name = 'updated' where id = ?")
		case 2:
			events[i] = helper.DML2DeleteEvent("test", "t", "insert into t values (?, 'test')", "delete from t where id = ?")
		}
	}

	tableInfo := events[0].TableInfo

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = generator.groupRowsByType(events, tableInfo)
	}
}
