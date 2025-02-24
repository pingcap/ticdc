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

package dispatcher

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableProgress(t *testing.T) {
	tp := NewTableProgress()
	// Test Empty
	assert.True(t, tp.Empty())

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')")
	dmlEvent.StartTs = 1
	dmlEvent.CommitTs = 2

	// Add an event
	tp.Add(dmlEvent)
	assert.False(t, tp.Empty())

	// Verify GetCheckpointTs
	checkpointTs, isEmpty := tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.False(t, isEmpty)

	// Verify maxCommitTs
	assert.Equal(t, uint64(2), tp.maxCommitTs)

	// verify after event is flushed
	dmlEvent.PostFlush()
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(1), checkpointTs)
	assert.True(t, isEmpty)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 4,
	}

	tp.Pass(ddlEvent)
	assert.Equal(t, uint64(4), tp.maxCommitTs, "Expected maxCommitTs to be 3 after Pass")
	checkpointTs, isEmpty = tp.GetCheckpointTs()
	assert.Equal(t, uint64(3), checkpointTs)
	assert.True(t, isEmpty)
}
