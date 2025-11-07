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
	"strconv"
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

func TestBuildEventLogContextTruncateRows(t *testing.T) {
	rows := make([]common.RowLogInfo, 0, maxEventLogRows+5)
	for i := 0; i < maxEventLogRows+5; i++ {
		rows = append(rows, common.RowLogInfo{
			Type:     "insert",
			Database: "db",
			Table:    "t",
			CommitTs: uint64(i + 1),
			PrimaryKeys: []common.ColumnLogInfo{
				{Name: "id", Value: i},
			},
		})
	}
	info := &common.MessageLogInfo{Rows: rows}
	ctx := BuildEventLogContext("ks", "cf", info)
	require.Contains(t, ctx, "dmlInfo=")
	require.Contains(t, ctx, "dmlInfoTruncated=true")
	require.Contains(t, ctx, "truncatedRows=5")
	require.Contains(t, ctx, "totalRows="+strconv.Itoa(len(rows)))
}

func TestBuildEventLogContextTruncateBySize(t *testing.T) {
	largeValue := strings.Repeat("a", maxEventLogJSONBytes)
	info := &common.MessageLogInfo{
		Rows: []common.RowLogInfo{
			{Type: "insert", Table: largeValue},
		},
	}
	ctx := BuildEventLogContext("ks", "cf", info)
	require.Contains(t, ctx, "dmlInfoTruncated=true")
	require.Contains(t, ctx, "totalRows=1")
	require.Contains(t, ctx, "...(truncated)")
}
