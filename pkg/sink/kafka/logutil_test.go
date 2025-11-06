package kafka

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildDMLLogFields(t *testing.T) {
	fields := BuildDMLLogFields(nil)
	require.Nil(t, fields)

	info := &common.MessageLogInfo{
		Rows: []common.RowLogInfo{
			{
				Type:     "insert",
				Database: "test",
				Table:    "t",
				CommitTs: 100,
				PrimaryKeys: []common.ColumnLogInfo{
					{Name: "id", Value: 1},
				},
			},
		},
	}
	result := BuildDMLLogFields(info)
	require.Len(t, result, 1)
	require.Equal(t, "dmlInfo", result[0].Key)

	rows, ok := result[0].Interface.([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, rows, 1)
	require.Equal(t, "insert", rows[0]["type"])
	require.Equal(t, "test", rows[0]["database"])
	require.Equal(t, "t", rows[0]["table"])
	require.Equal(t, uint64(100), rows[0]["commitTs"])
	pk, ok := rows[0]["primaryPK"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, 1, pk["id"])
}

func TestBuildDDLLogFields(t *testing.T) {
	fields := BuildDDLLogFields(nil)
	require.Nil(t, fields)

	info := &common.MessageLogInfo{
		DDL: &common.DDLLogInfo{
			Query:    "ALTER TABLE t ADD COLUMN c INT",
			CommitTs: 123,
		},
	}
	result := BuildDDLLogFields(info)
	require.Len(t, result, 1)
	require.Equal(t, "ddlInfo", result[0].Key)

	value, ok := result[0].Interface.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "ALTER TABLE t ADD COLUMN c INT", value["query"])
	require.Equal(t, uint64(123), value["commitTs"])
}

func TestBuildCheckpointLogFields(t *testing.T) {
	fields := BuildCheckpointLogFields(nil)
	require.Nil(t, fields)

	info := &common.MessageLogInfo{
		Checkpoint: &common.CheckpointLogInfo{CommitTs: 456},
	}
	result := BuildCheckpointLogFields(info)
	require.Len(t, result, 1)
	require.Equal(t, "checkpointTs", result[0].Key)
	require.EqualValues(t, 456, result[0].Integer)
}

func TestDetermineEventType(t *testing.T) {
	require.Equal(t, "unknown", DetermineEventType(nil))
	require.Equal(t, "dml", DetermineEventType(&common.MessageLogInfo{Rows: []common.RowLogInfo{{}}}))
	require.Equal(t, "ddl", DetermineEventType(&common.MessageLogInfo{DDL: &common.DDLLogInfo{}}))
	require.Equal(t, "checkpoint", DetermineEventType(&common.MessageLogInfo{Checkpoint: &common.CheckpointLogInfo{CommitTs: 1}}))
	require.Equal(t, "unknown", DetermineEventType(&common.MessageLogInfo{}))
}
