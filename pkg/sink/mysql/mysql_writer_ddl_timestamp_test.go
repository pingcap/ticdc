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
	"github.com/tikv/client-go/v2/oracle"
)

func ddlSessionTimestampForTest(event *commonEvent.DDLEvent, timezone string) string {
	if event == nil {
		return formatUnixTimestamp(0)
	}
	ts, ok := ddlSessionTimestampFromOriginDefault(event, timezone)
	if !ok {
		tsToUse := event.GetStartTs()
		if tsToUse == 0 {
			tsToUse = event.GetCommitTs()
		}
		ts = float64(oracle.GetTimeFromTS(tsToUse).Unix())
	}
	return formatUnixTimestamp(ts)
}

func expectDDLExec(mock sqlmock.Sqlmock, event *commonEvent.DDLEvent, timezone string) {
	mock.ExpectExec("SET TIMESTAMP = " + ddlSessionTimestampForTest(event, timezone)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(event.GetDDLQuery()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("SET TIMESTAMP = DEFAULT").
		WillReturnResult(sqlmock.NewResult(1, 1))
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
	ddlEvent.StartTs = oracle.GoTimeToTS(time.Unix(100, 0))

	originTs, ok := ddlSessionTimestampFromOriginDefault(ddlEvent, writer.cfg.Timezone)
	require.True(t, ok)
	startTs := float64(oracle.GetTimeFromTS(ddlEvent.StartTs).Unix())
	require.NotEqual(t, formatUnixTimestamp(startTs), formatUnixTimestamp(originTs))

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, writer.cfg.Timezone)
	mock.ExpectCommit()

	err := writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecDDL_UsesStartTsWhenNoCurrentTimestampDefault(t *testing.T) {
	writer, db, mock := newTestMysqlWriter(t)
	defer db.Close()
	writer.cfg.Timezone = "\"UTC\""

	helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	helper.DDL2Event("create table t (id int primary key)")

	ddlEvent := helper.DDL2Event("alter table t add column age int default 1")
	fixedTime := time.Date(2025, 9, 25, 16, 10, 36, 0, time.UTC)
	ddlEvent.StartTs = oracle.GoTimeToTS(fixedTime)

	expected := formatUnixTimestamp(float64(oracle.GetTimeFromTS(ddlEvent.StartTs).Unix()))
	require.Equal(t, expected, ddlSessionTimestampForTest(ddlEvent, writer.cfg.Timezone))

	mock.ExpectBegin()
	mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
	expectDDLExec(mock, ddlEvent, writer.cfg.Timezone)
	mock.ExpectCommit()

	err := writer.execDDL(ddlEvent)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
