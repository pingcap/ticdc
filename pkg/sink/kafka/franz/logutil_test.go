// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package franz

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestEventType(t *testing.T) {
	require.Equal(t, "unknown", eventType(nil))
	require.Equal(t, "ddl", eventType(&common.MessageLogInfo{DDL: &common.DDLLogInfo{}}))
	require.Equal(t, "checkpoint", eventType(&common.MessageLogInfo{
		Checkpoint: &common.CheckpointLogInfo{},
	}))
	require.Equal(t, "dml", eventType(&common.MessageLogInfo{Rows: []common.RowLogInfo{{}}}))
	require.Equal(t, "unknown", eventType(&common.MessageLogInfo{}))
}

func TestBuildEventLogContext(t *testing.T) {
	info := &common.MessageLogInfo{
		Rows: []common.RowLogInfo{
			{
				Type:     "insert",
				Database: "database",
				Table:    "table",
				CommitTs: 1,
			},
		},
	}

	context := buildEventLogContext("keyspace", "changefeed", info)
	require.Contains(t, context, "keyspace=keyspace")
	require.Contains(t, context, "changefeed=changefeed")
	require.Contains(t, context, "eventType=dml")
	require.Contains(t, context, `dmlInfo=[{"Type":"insert"`)
	require.Contains(t, context, `"Database":"database"`)
	require.Contains(t, context, `"Table":"table"`)
	require.Contains(t, context, `"CommitTs":1`)
}

func TestBuildEventLogContextForBlockEvents(t *testing.T) {
	ddlContext := buildEventLogContext("keyspace", "changefeed", &common.MessageLogInfo{
		DDL: &common.DDLLogInfo{
			Query:    "CREATE TABLE t(id INT PRIMARY KEY)",
			StartTs:  1,
			CommitTs: 2,
		},
	})

	require.Contains(t, ddlContext, "eventType=ddl")
	require.Contains(t, ddlContext, `ddlQuery="CREATE TABLE t(id INT PRIMARY KEY)"`)
	require.Contains(t, ddlContext, "ddlStartTs=1")
	require.Contains(t, ddlContext, "ddlCommitTs=2")

	checkpointContext := buildEventLogContext("keyspace", "changefeed", &common.MessageLogInfo{
		Checkpoint: &common.CheckpointLogInfo{CommitTs: 3},
	})
	require.Contains(t, checkpointContext, "eventType=checkpoint")
	require.Contains(t, checkpointContext, "checkpointTs=3")
}
