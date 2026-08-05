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
package kafka

import (
	"encoding/json"
	"strings"
	"testing"

	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDetermineEventType(t *testing.T) {
	require.Equal(t, "unknown", DetermineEventType(nil))
	require.Equal(t, "dml", DetermineEventType(&codecCommon.MessageLogInfo{Rows: []codecCommon.RowLogInfo{{}}}))
	require.Equal(t, "ddl", DetermineEventType(&codecCommon.MessageLogInfo{DDL: &codecCommon.DDLLogInfo{}}))
	require.Equal(t, "checkpoint", DetermineEventType(&codecCommon.MessageLogInfo{Checkpoint: &codecCommon.CheckpointLogInfo{CommitTs: 1}}))
	require.Equal(t, "unknown", DetermineEventType(&codecCommon.MessageLogInfo{}))
}

func TestBuildEventLogContextRowsIncluded(t *testing.T) {
	rows := []codecCommon.RowLogInfo{
		{
			Type:     "insert",
			Database: "db1",
			Table:    "t1",
			CommitTs: 1,
			PrimaryKeys: []codecCommon.ColumnLogInfo{
				{Name: "id", Value: 1},
			},
		},
		{
			Type:     "delete",
			Database: "db2",
			Table:    "t2",
			CommitTs: 2,
		},
	}
	info := &codecCommon.MessageLogInfo{Rows: rows}
	ctx := BuildEventLogContext("ks", "cf", info)
	data, err := json.Marshal(rows)
	require.NoError(t, err)
	expected := string(data)
	require.Contains(t, ctx, "dmlInfo="+expected)
	require.NotContains(t, ctx, "dmlInfoTruncated")
	require.NotContains(t, ctx, "truncatedRows")
}

func TestBuildEventLogContextLargeData(t *testing.T) {
	largeValue := strings.Repeat("a", 12*1024)
	info := &codecCommon.MessageLogInfo{
		Rows: []codecCommon.RowLogInfo{
			{Type: "insert", Table: largeValue},
		},
	}
	ctx := BuildEventLogContext("ks", "cf", info)
	require.Contains(t, ctx, largeValue)
	require.NotContains(t, ctx, "...(truncated)")
}

func TestBuildEventLogContextBlockEvents(t *testing.T) {
	t.Run("ddl", func(t *testing.T) {
		ctx := BuildEventLogContext("ks", "cf", &codecCommon.MessageLogInfo{
			DDL: &codecCommon.DDLLogInfo{
				Query:    "CREATE TABLE t(id INT PRIMARY KEY)",
				StartTs:  1,
				CommitTs: 2,
			},
		})

		require.Contains(t, ctx, "eventType=ddl")
		require.Contains(t, ctx, "ddlQuery=\"CREATE TABLE t(id INT PRIMARY KEY)\"")
		require.Contains(t, ctx, "ddlStartTs=1")
		require.Contains(t, ctx, "ddlCommitTs=2")
	})

	t.Run("checkpoint", func(t *testing.T) {
		ctx := BuildEventLogContext("ks", "cf", &codecCommon.MessageLogInfo{
			Checkpoint: &codecCommon.CheckpointLogInfo{CommitTs: 3},
		})

		require.Contains(t, ctx, "eventType=checkpoint")
		require.Contains(t, ctx, "checkpointTs=3")
	})
}
