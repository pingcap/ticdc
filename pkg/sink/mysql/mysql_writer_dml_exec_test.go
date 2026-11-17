// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestSequenceExecuteRedactsArgs(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	const rowValue = "sensitive-row-value"
	for _, tt := range []struct {
		name        string
		mode        string
		containsRow bool
	}{
		{name: "redaction disabled", mode: errors.RedactLogDisable, containsRow: true},
		{name: "redaction enabled", mode: errors.RedactLogEnable, containsRow: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			errors.RedactLogEnabled.Store(tt.mode)
			db, mock := newTestMockDB(t)
			defer db.Close()

			mock.ExpectBegin()
			mock.ExpectExec("INSERT INTO t VALUES (?)").
				WithArgs(rowValue).
				WillReturnError(errors.New("exec failed"))
			mock.ExpectRollback()

			tx, err := db.Begin()
			require.NoError(t, err)
			cfg := New()
			cfg.CachePrepStmts = false
			writer := &Writer{ctx: context.Background(), cfg: cfg}
			dmls := &preparedDMLs{
				sqls:   []string{"INSERT INTO t VALUES (?)"},
				values: [][]any{{rowValue}},
			}

			err = writer.sequenceExecute(dmls, tx, time.Second)
			require.Error(t, err)
			if tt.containsRow {
				require.Contains(t, err.Error(), rowValue)
			} else {
				require.NotContains(t, err.Error(), rowValue)
				require.Contains(t, err.Error(), "args:(?)")
				require.NotContains(t, dmls.String(), rowValue)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMultiStmtExecuteRedactsArgs(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	const (
		rowValue = "sensitive-row-value"
		query    = "BEGIN;INSERT INTO t VALUES (?);COMMIT;"
	)
	for _, tt := range []struct {
		name        string
		mode        string
		containsRow bool
	}{
		{name: "redaction disabled", mode: errors.RedactLogDisable, containsRow: true},
		{name: "redaction enabled", mode: errors.RedactLogEnable, containsRow: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			errors.RedactLogEnabled.Store(tt.mode)
			db, mock := newTestMockDB(t)
			defer db.Close()

			mock.ExpectExec(query).
				WithArgs(rowValue).
				WillReturnError(errors.New("exec failed"))
			mock.ExpectExec("ROLLBACK").WillReturnResult(nil)

			writer := &Writer{ctx: context.Background(), db: db}
			dmls := &preparedDMLs{
				sqls:   []string{"INSERT INTO t VALUES (?)"},
				values: [][]any{{rowValue}},
			}

			_, err := writer.multiStmtExecute(dmls, time.Second)
			require.Error(t, err)
			if tt.containsRow {
				require.Contains(t, err.Error(), rowValue)
			} else {
				require.NotContains(t, err.Error(), rowValue)
				require.Contains(t, err.Error(), "args:(?)")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
