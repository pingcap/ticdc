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

package mysql

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func ddlSessionTimestampForTest(event *commonEvent.DDLEvent, timezone string) (string, bool) {
	if event == nil {
		return "", false
	}
	ts, ok := ddlSessionTimestampFromOriginDefault(event, timezone)
	if !ok {
		return "", false
	}
	return formatUnixTimestamp(ts), true
}

func expectDDLExec(mock sqlmock.Sqlmock, event *commonEvent.DDLEvent, timezone string) {
	ddlTimestamp, ok := ddlSessionTimestampForTest(event, timezone)
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").
		WillReturnResult(sqlmock.NewResult(1, 1))
	if ok {
		mock.ExpectExec("SET TIMESTAMP = " + ddlTimestamp).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectExec(event.GetDDLQuery()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if ok {
		mock.ExpectExec("SET TIMESTAMP = DEFAULT").
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
}

func TestExecDDL_UsesOriginDefaultTimestampForCurrentTimestampDefault(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()
	writer.cfg.Timezone = "\"UTC\""

	helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.Tk().MustExec("set time_zone = 'UTC'")
	helper.Tk().MustExec("set @@timestamp = 1720000000.123456")
	helper.DDL2Event("create table t (id int primary key)")

	ddlEvent := helper.DDL2Event("alter table t add column updatetime datetime(6) default current_timestamp(6)")

	originTs, ok := ddlSessionTimestampFromOriginDefault(ddlEvent, writer.cfg.Timezone)
	require.True(t, ok)
	ddlTimestamp, ok := ddlSessionTimestampForTest(ddlEvent, writer.cfg.Timezone)
	require.True(t, ok)
	require.Equal(t, ddlTimestamp, formatUnixTimestamp(originTs))

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, writer.cfg.Timezone)
	mock.ExpectCommit()

	err := writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecDDL_DoesNotSetTimestampWhenNoCurrentTimestampDefault(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()
	writer.cfg.Timezone = "\"UTC\""

	helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key)")

	ddlEvent := helper.DDL2Event("alter table t add column age int default 1")
	_, ok := ddlSessionTimestampForTest(ddlEvent, writer.cfg.Timezone)
	require.False(t, ok)

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, writer.cfg.Timezone)
	mock.ExpectCommit()

	err := writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
