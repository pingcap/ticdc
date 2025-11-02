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
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestDBExecutor_ExecuteTransaction(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO test VALUES (1, 'test');UPDATE test SET name = 'updated' WHERE id = 1;COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"key1": {}},
	}

	// Set up mock expectations - expect the full SQL with BEGIN/COMMIT
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.NoError(t, err)

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_EmptySQL(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL with empty SQL list
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{},
	}

	// Execute transaction - should succeed without any database operations
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.NoError(t, err)

	// Verify no database operations were performed
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_BeginError(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO test VALUES (1, 'test');COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"key1": {}},
	}

	// Set up mock to fail on SQL execution
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WillReturnError(errors.New("connection failed"))

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection failed")

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_ExecError(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO test VALUES (1, 'test');UPDATE test SET name = 'updated' WHERE id = 1;COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"key1": {}},
	}

	// Set up mock expectations - SQL execution fails
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WillReturnError(errors.New("update failed"))

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.Error(t, err)
	require.Contains(t, err.Error(), "update failed")

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_CommitError(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO test VALUES (1, 'test');COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"key1": {}},
	}

	// Set up mock expectations - SQL execution fails
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WillReturnError(errors.New("commit failed"))

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit failed")

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_Timeout(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO test VALUES (1, 'test');COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"key1": {}},
	}

	// Set up mock expectations with delay
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WillDelayFor(35 * time.Second).WillReturnResult(sqlmock.NewResult(1, 1))
	// Should timeout after 30 seconds

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.Error(t, err)
	require.Contains(t, err.Error(), "canceling query due to user request")

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_ExecuteTransaction_MultipleSQL(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := NewDBExecutor(db)

	// Create test transaction SQL with multiple statements
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	txnSQL := &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      "BEGIN;INSERT INTO users VALUES (1, 'alice');INSERT INTO users VALUES (2, 'bob');UPDATE users SET name = 'alice_updated' WHERE id = 1;DELETE FROM users WHERE id = 2;COMMIT;",
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"user1": {}, "user2": {}},
	}

	// Set up mock expectations for the combined SQL
	mock.ExpectExec("BEGIN;INSERT INTO users VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

	// Execute transaction
	err = executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
	require.NoError(t, err)

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBExecutor_Close(t *testing.T) {
	t.Parallel()

	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	executor := NewDBExecutor(db)

	// Set up mock expectation for Close
	mock.ExpectClose()

	// Close the executor
	err = executor.Close()
	require.NoError(t, err)

	// Verify all expectations were met
	require.NoError(t, mock.ExpectationsWereMet())
}

// Test helper function to create a mock database with specific behavior
func createMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	cleanup := func() {
		db.Close()
	}

	return db, mock, cleanup
}

// Test helper function to create a test TxnSQL
func createTestTxnSQL(sql string) *TxnSQL {
	txnGroup := &TxnGroup{
		CommitTs: 100,
		StartTs:  50,
		Events:   []*commonEvent.DMLEvent{},
	}

	return &TxnSQL{
		TxnGroup: txnGroup,
		SQL:      sql,
		Args:     []interface{}{},
		Keys:     map[string]struct{}{"test_key": {}},
	}
}

// Benchmark tests
func BenchmarkDBExecutor_ExecuteTransaction(b *testing.B) {
	db, mock, cleanup := createMockDB(&testing.T{})
	defer cleanup()

	executor := NewDBExecutor(db)

	// Create test transaction SQL
	txnSQL := createTestTxnSQL("BEGIN;INSERT INTO test VALUES (1, 'test');UPDATE test SET name = 'updated' WHERE id = 1;COMMIT;")

	// Set up mock expectations
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset mock expectations for each iteration
		mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

		err := executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
		require.NoError(b, err)
	}
}

func BenchmarkDBExecutor_ExecuteTransaction_SingleSQL(b *testing.B) {
	db, mock, cleanup := createMockDB(&testing.T{})
	defer cleanup()

	executor := NewDBExecutor(db)

	// Create test transaction SQL with single statement
	txnSQL := createTestTxnSQL("BEGIN;INSERT INTO test VALUES (1, 'test');COMMIT;")

	// Set up mock expectations
	mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset mock expectations for each iteration
		mock.ExpectExec("BEGIN;INSERT INTO test VALUES").WithArgs().WillReturnResult(sqlmock.NewResult(1, 1))

		err := executor.ExecuteSQLBatch([]*TxnSQL{txnSQL})
		require.NoError(b, err)
	}
}
