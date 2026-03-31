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
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

func TestAddDMLEventUsesTargetNames(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(id int primary key, name varchar(32))`)
	require.NotNil(t, job)

	routedTableInfo := helper.GetTableInfo(job).CloneWithRouting("target_db", "target_table")
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 'alice')`)
	dmlEvent.TableInfo = routedTableInfo

	writers := &dmlWriters{
		msgCh: chann.NewUnlimitedChannelDefault[*task](),
	}

	writers.addDMLEvent(dmlEvent)

	task, ok, err := writers.msgCh.GetWithContext(context.Background())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, task)
	require.Equal(t, commonType.TableName{
		Schema:      "target_db",
		Table:       "target_table",
		TableID:     dmlEvent.PhysicalTableID,
		IsPartition: routedTableInfo.IsPartitionTable(),
	}, task.versionedTable.TableNameWithPhysicTableID)
}
