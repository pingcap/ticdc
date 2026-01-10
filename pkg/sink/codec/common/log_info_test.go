// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"testing"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonModel "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// withRedactMode is a test helper that sets redaction mode and returns a cleanup function.
func withRedactMode(t *testing.T, mode string) func() {
	t.Helper()
	original := perrors.RedactLogEnabled.Load()
	perrors.RedactLogEnabled.Store(mode)
	return func() {
		perrors.RedactLogEnabled.Store(original)
	}
}

func TestBuildMessageLogInfo(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogDisable)()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table test.t (id int primary key, name varchar(32))")
	tableInfo := helper.GetTableInfo(job)

	dml := helper.DML2Event("test", "t", `insert into test.t values (1, "alice")`)
	row, ok := dml.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		StartTs:        dml.StartTs,
		CommitTs:       dml.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}

	info := buildMessageLogInfo([]*commonEvent.RowEvent{rowEvent})
	require.NotNil(t, info)
	require.Len(t, info.Rows, 1)
	rowInfo := info.Rows[0]
	require.Equal(t, "insert", rowInfo.Type)
	require.Equal(t, "test", rowInfo.Database)
	require.Equal(t, "t", rowInfo.Table)
	require.Equal(t, dml.StartTs, rowInfo.StartTs)
	require.Equal(t, dml.GetCommitTs(), rowInfo.CommitTs)
	require.Len(t, rowInfo.PrimaryKeys, 1)
	require.Equal(t, "id", rowInfo.PrimaryKeys[0].Name)
	require.Equal(t, "1", rowInfo.PrimaryKeys[0].Value)
}

func TestAttachMessageLogInfo(t *testing.T) {
	defer withRedactMode(t, perrors.RedactLogDisable)()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table test.t (id int primary key, name varchar(32))")
	tableInfo := helper.GetTableInfo(job)

	dml := helper.DML2Event("test", "t", `insert into test.t values (1, "alice")`)
	row, ok := dml.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		StartTs:        dml.StartTs,
		CommitTs:       dml.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}

	message := NewMsg(nil, nil)
	message.SetRowsCount(1)
	err := AttachMessageLogInfo([]*Message{message}, []*commonEvent.RowEvent{rowEvent})
	require.NoError(t, err)

	require.NotNil(t, message.LogInfo)
	require.Len(t, message.LogInfo.Rows, 1)
	require.Equal(t, "insert", message.LogInfo.Rows[0].Type)
	require.Equal(t, dml.StartTs, message.LogInfo.Rows[0].StartTs)
	require.Len(t, message.LogInfo.Rows[0].PrimaryKeys, 1)
	require.Equal(t, "1", message.LogInfo.Rows[0].PrimaryKeys[0].Value)
}

func TestSetDDLMessageLogInfo(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlEvent := helper.DDL2Event("create table test.ddl_t (id int primary key, val varchar(10))")
	message := NewMsg(nil, nil)

	SetDDLMessageLogInfo(message, ddlEvent)

	require.NotNil(t, message.LogInfo)
	require.NotNil(t, message.LogInfo.DDL)
	require.Equal(t, ddlEvent.Query, message.LogInfo.DDL.Query)
	require.Equal(t, ddlEvent.GetCommitTs(), message.LogInfo.DDL.CommitTs)
	require.Nil(t, message.LogInfo.Rows)
	require.Nil(t, message.LogInfo.Checkpoint)
}

func TestSetCheckpointMessageLogInfo(t *testing.T) {
	message := NewMsg(nil, nil)
	SetCheckpointMessageLogInfo(message, 789)
	require.NotNil(t, message.LogInfo)
	require.NotNil(t, message.LogInfo.Checkpoint)
	require.Equal(t, uint64(789), message.LogInfo.Checkpoint.CommitTs)
	require.Nil(t, message.LogInfo.Rows)
	require.Nil(t, message.LogInfo.DDL)

	SetCheckpointMessageLogInfo(message, 900)
	require.Equal(t, uint64(900), message.LogInfo.Checkpoint.CommitTs)
}

func makeTestRowEvents(
	t *testing.T,
	helper *commonEvent.EventTestHelper,
	tableInfo *commonModel.TableInfo,
	sql string,
) []*commonEvent.RowEvent {
	dml := helper.DML2Event("test", "t", sql)
	events := make([]*commonEvent.RowEvent, 0)
	for {
		row, ok := dml.GetNextRow()
		if !ok {
			break
		}
		events = append(events, &commonEvent.RowEvent{
			TableInfo:      tableInfo,
			StartTs:        dml.StartTs,
			CommitTs:       dml.GetCommitTs(),
			Event:          row,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
	}
	require.NotEmpty(t, events)
	return events
}

func TestExtractPrimaryKeysRedaction(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table test.t (id int primary key, name varchar(32))")
	tableInfo := helper.GetTableInfo(job)

	dml := helper.DML2Event("test", "t", `insert into test.t values (42, "secret")`)
	row, ok := dml.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		StartTs:        dml.StartTs,
		CommitTs:       dml.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}

	tests := []struct {
		name     string
		mode     string
		expected string
	}{
		{
			name:     "OFF mode - raw value",
			mode:     perrors.RedactLogDisable,
			expected: "42",
		},
		{
			name:     "MARKER mode - wrapped value",
			mode:     perrors.RedactLogMarker,
			expected: "‹42›",
		},
		{
			name:     "ON mode - fully redacted",
			mode:     perrors.RedactLogEnable,
			expected: "?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer withRedactMode(t, tt.mode)()

			info := buildMessageLogInfo([]*commonEvent.RowEvent{rowEvent})
			require.NotNil(t, info)
			require.Len(t, info.Rows, 1)
			require.Len(t, info.Rows[0].PrimaryKeys, 1)
			require.Equal(t, "id", info.Rows[0].PrimaryKeys[0].Name)
			require.Equal(t, tt.expected, info.Rows[0].PrimaryKeys[0].Value)
		})
	}
}

func TestExtractPrimaryKeysRedactionWithStringPK(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table test.t2 (email varchar(64) primary key, data text)")
	tableInfo := helper.GetTableInfo(job)

	dml := helper.DML2Event("test", "t2", `insert into test.t2 values ("user@example.com", "sensitive")`)
	row, ok := dml.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		StartTs:        dml.StartTs,
		CommitTs:       dml.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}

	tests := []struct {
		name     string
		mode     string
		expected string
	}{
		{
			name:     "OFF mode - raw value",
			mode:     perrors.RedactLogDisable,
			expected: "user@example.com",
		},
		{
			name:     "MARKER mode - wrapped value",
			mode:     perrors.RedactLogMarker,
			expected: "‹user@example.com›",
		},
		{
			name:     "ON mode - fully redacted",
			mode:     perrors.RedactLogEnable,
			expected: "?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer withRedactMode(t, tt.mode)()

			info := buildMessageLogInfo([]*commonEvent.RowEvent{rowEvent})
			require.NotNil(t, info)
			require.Len(t, info.Rows, 1)
			require.Len(t, info.Rows[0].PrimaryKeys, 1)
			require.Equal(t, "email", info.Rows[0].PrimaryKeys[0].Name)
			require.Equal(t, tt.expected, info.Rows[0].PrimaryKeys[0].Value)
		})
	}
}
