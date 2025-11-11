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

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonModel "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestBuildMessageLogInfo(t *testing.T) {
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
	require.Equal(t, int64(1), rowInfo.PrimaryKeys[0].Value)
}

func TestAttachMessageLogInfo(t *testing.T) {
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
	require.Equal(t, int64(1), message.LogInfo.Rows[0].PrimaryKeys[0].Value)
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
