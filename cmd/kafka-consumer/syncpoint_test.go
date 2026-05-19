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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMysqlConsumerSyncpointStoreWriteReadsCurrentTsInTxn(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	store := &mysqlConsumerSyncpointStore{
		db:         db,
		consumerID: "consumer-1",
		topic:      "topic-1",
	}

	mock.ExpectBegin()
	mock.ExpectQuery("select @@tidb_current_ts").
		WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow("456"))
	mock.ExpectExec("INSERT IGNORE INTO tidb_cdc.consumer_syncpoint_v1 (ticdc_cluster_id, consumer_id, topic, primary_ts, secondary_ts) VALUES (?, ?, ?, ?, ?)").
		WithArgs("default", "consumer-1", "topic-1", "123", "456").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("SET GLOBAL tidb_external_ts = 456").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	require.NoError(t, store.Write(context.Background(), 123))
	require.NoError(t, mock.ExpectationsWereMet())
}
