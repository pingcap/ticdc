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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
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

func TestOriginDefaultTimestampOffset(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		timezone string
		value    string
	}{
		{timezone: `"+00:00"`, value: "2026-01-01 00:00:00.123456"},
		{timezone: `"+08:00"`, value: "2026-01-01 08:00:00.123456"},
		{timezone: `"-06:00"`, value: "2025-12-31 18:00:00.123456"},
		{timezone: `"UTC"`, value: "2026-01-01 00:00:00.123456"},
		{timezone: `"+5:45"`, value: "2026-01-01 05:45:00.123456"},
		{timezone: `"-03:30"`, value: "2025-12-31 20:30:00.123456"},
		{timezone: `"-13:59"`, value: "2025-12-31 10:01:00.123456"},
		{timezone: `"+14:00"`, value: "2026-01-01 14:00:00.123456"},
		{timezone: `"-0:00"`, value: "2026-01-01 00:00:00.123456"},
	} {
		t.Run(tc.timezone, func(t *testing.T) {
			t.Parallel()
			ts, err := parseOriginDefaultTimestamp(tc.value, nil, tc.timezone)
			require.NoError(t, err)
			require.Equal(t, "1767225600.123456", formatUnixTimestamp(ts))
			for _, column := range []*timodel.ColumnInfo{
				{FieldType: *types.NewFieldType(mysql.TypeTimestamp)},
				{Version: timodel.ColumnInfoVersion1, FieldType: *types.NewFieldType(mysql.TypeDatetime)},
			} {
				ts, err := parseOriginDefaultTimestamp(tc.value, column, tc.timezone)
				require.NoError(t, err)
				require.Equal(t, "1767225600.123456", formatUnixTimestamp(ts))
			}
			// Version 1 TIMESTAMP defaults are already UTC, even for a
			// nonzero sink offset. They must not be converted a second time.
			column := &timodel.ColumnInfo{
				Version: timodel.ColumnInfoVersion1, FieldType: *types.NewFieldType(mysql.TypeTimestamp),
			}
			ts, err = parseOriginDefaultTimestamp("2026-01-01 00:00:00.123456", column, tc.timezone)
			require.NoError(t, err)
			require.Equal(t, "1767225600.123456", formatUnixTimestamp(ts))
		})
	}
	_, err := parseOriginDefaultTimestamp("2026-01-01 00:00:00", nil, `"+14:01"`)
	require.Error(t, err)
	// Empty configuration retains its existing UTC fallback.
	loc, err := resolveOriginDefaultLocation(nil, "")
	require.NoError(t, err)
	require.Equal(t, time.UTC, loc)
}

func TestExecDDLNumericTimezone(t *testing.T) {
	for _, timezone := range []string{"+00:00", "+08:00", "+05:45", "-03:30"} {
		t.Run(timezone, func(t *testing.T) {
			writer, db, mock := newTestMysqlWriter(t)
			defer db.Close()
			writer.cfg.Timezone = `"` + timezone + `"`
			helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
			defer helper.Close()
			helper.Tk().MustExec("use test")
			helper.Tk().MustExec("set time_zone = '" + timezone + "'")
			helper.Tk().MustExec("set @@timestamp = 1767225600.123456")
			helper.DDL2Event("create table t (id int primary key)")
			ddl := helper.DDL2Event("alter table t add column added datetime(6) default current_timestamp(6)")
			ts, ok := ddlSessionTimestampFromOriginDefault(ddl, writer.cfg.Timezone)
			require.True(t, ok)
			require.Equal(t, "1767225600.123456", formatUnixTimestamp(ts))

			mock.ExpectBegin()
			mock.ExpectExec("USE `test`;").WillReturnResult(sqlmock.NewResult(1, 1))
			expectDDLExec(mock, ddl, writer.cfg.Timezone)
			mock.ExpectCommit()
			require.NoError(t, writer.execDDL(ddl))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
