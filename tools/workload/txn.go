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

package main

import (
	"context"
	"database/sql"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

func (app *WorkloadApp) runTransaction(conn *sql.Conn, doOne func() (uint64, error)) (uint64, error) {
	if !app.Config.BatchInTxn {
		return doOne()
	}

	if err := app.beginTransaction(conn); err != nil {
		return 0, err
	}

	flushedRows, err := doOne()
	if err != nil {
		app.rollbackTransaction(conn)
		return 0, err
	}

	if err := app.commitTransaction(conn); err != nil {
		app.rollbackTransaction(conn)
		return 0, err
	}

	return flushedRows, nil
}

func (app *WorkloadApp) beginTransaction(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "BEGIN")
	if err != nil {
		plog.Info("begin transaction failed", zap.Error(err))
	}
	return err
}

func (app *WorkloadApp) commitTransaction(conn *sql.Conn) error {
	_, err := conn.ExecContext(context.Background(), "COMMIT")
	if err != nil {
		plog.Info("commit transaction failed", zap.Error(err))
	}
	return err
}

func (app *WorkloadApp) rollbackTransaction(conn *sql.Conn) {
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		plog.Info("rollback transaction failed", zap.Error(err))
	}
}
