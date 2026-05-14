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
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSyncpointManagerWriteIsIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	manager := &syncpointManager{
		enabled:       true,
		consumer:      "consumer-a",
		topic:         "topic-a",
		retention:     time.Hour,
		db:            db,
		lastCleanTime: time.Now(),
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("select @@tidb_current_ts")).
		WillReturnRows(sqlmock.NewRows([]string{"@@tidb_current_ts"}).AddRow("456"))
	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO tidb_cdc.consumer_syncpoint_v1
(consumer_id, topic, primary_ts, secondary_ts)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE secondary_ts = secondary_ts`)).
		WithArgs("consumer-a", "topic-a", "123", "456").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	secondaryTs, err := manager.Write(context.Background(), 123)
	require.NoError(t, err)
	require.Equal(t, uint64(456), secondaryTs)
	require.Equal(t, uint64(123), manager.lastSyncedTs)
	require.NoError(t, mock.ExpectationsWereMet())
}
