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
