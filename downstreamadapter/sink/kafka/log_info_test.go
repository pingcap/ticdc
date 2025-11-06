package kafka

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
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
		CommitTs:       dml.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}

	message := common.NewMsg(nil, nil)
	message.SetRowsCount(1)
	attachMessageLogInfo([]*common.Message{message}, []*commonEvent.RowEvent{rowEvent})

	require.NotNil(t, message.LogInfo)
	require.Len(t, message.LogInfo.Rows, 1)
	require.Equal(t, "insert", message.LogInfo.Rows[0].Type)
	require.Len(t, message.LogInfo.Rows[0].PrimaryKeys, 1)
	require.Equal(t, int64(1), message.LogInfo.Rows[0].PrimaryKeys[0].Value)
}

func TestSetDDLMessageLogInfo(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	ddlEvent := helper.DDL2Event("create table test.ddl_t (id int primary key, val varchar(10))")
	message := common.NewMsg(nil, nil)

	setDDLMessageLogInfo(message, ddlEvent)

	require.NotNil(t, message.LogInfo)
	require.NotNil(t, message.LogInfo.DDL)
	require.Equal(t, ddlEvent.Query, message.LogInfo.DDL.Query)
	require.Equal(t, ddlEvent.GetCommitTs(), message.LogInfo.DDL.CommitTs)

	// Ensure existing LogInfo is preserved.
	message.LogInfo.Rows = []common.RowLogInfo{{Type: "insert"}}
	setDDLMessageLogInfo(message, ddlEvent)
	require.Len(t, message.LogInfo.Rows, 1)
	require.Equal(t, "insert", message.LogInfo.Rows[0].Type)
}

func TestSetCheckpointMessageLogInfo(t *testing.T) {
	message := common.NewMsg(nil, nil)
	setCheckpointMessageLogInfo(message, 789)
	require.NotNil(t, message.LogInfo)
	require.NotNil(t, message.LogInfo.Checkpoint)
	require.Equal(t, uint64(789), message.LogInfo.Checkpoint.CommitTs)

	// Ensure existing info preserved.
	message.LogInfo.Rows = []common.RowLogInfo{{Type: "insert"}}
	setCheckpointMessageLogInfo(message, 900)
	require.Equal(t, uint64(900), message.LogInfo.Checkpoint.CommitTs)
	require.Len(t, message.LogInfo.Rows, 1)
}
