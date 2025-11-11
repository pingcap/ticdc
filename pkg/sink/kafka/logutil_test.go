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
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDetermineEventType(t *testing.T) {
	require.Equal(t, "unknown", DetermineEventType(nil))
	require.Equal(t, "dml", DetermineEventType(&common.MessageLogInfo{Rows: []common.RowLogInfo{{}}}))
	require.Equal(t, "ddl", DetermineEventType(&common.MessageLogInfo{DDL: &common.DDLLogInfo{}}))
	require.Equal(t, "checkpoint", DetermineEventType(&common.MessageLogInfo{Checkpoint: &common.CheckpointLogInfo{CommitTs: 1}}))
	require.Equal(t, "unknown", DetermineEventType(&common.MessageLogInfo{}))
}

func TestBuildEventLogContextRowsIncluded(t *testing.T) {
	rows := []common.RowLogInfo{
		{
			Type:     "insert",
			Database: "db1",
			Table:    "t1",
			CommitTs: 1,
			PrimaryKeys: []common.ColumnLogInfo{
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
	info := &common.MessageLogInfo{Rows: rows}
	ctx := BuildEventLogContext("ks", "cf", info)
	expected := formatDMLInfo(rows)
	require.Contains(t, ctx, "dmlInfo="+expected)
	require.NotContains(t, ctx, "dmlInfoTruncated")
	require.NotContains(t, ctx, "truncatedRows")
}

func TestBuildEventLogContextLargeData(t *testing.T) {
	largeValue := strings.Repeat("a", 12*1024)
	info := &common.MessageLogInfo{
		Rows: []common.RowLogInfo{
			{Type: "insert", Table: largeValue},
		},
	}
	ctx := BuildEventLogContext("ks", "cf", info)
	require.Contains(t, ctx, largeValue)
	require.NotContains(t, ctx, "...(truncated)")
}
