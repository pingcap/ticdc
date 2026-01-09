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

package open

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestOnlyOutputUpdatedColumn(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t (a int primary key, b int, c int)`)
	require.NotNil(t, job)
	_ = helper.DML2Event("test", "t", `insert into test.t values (1, 1, 1)`)
	event := helper.DML2Event("test", "t", `update test.t set a=1,b=1,c=1 where a=1`)
	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OnlyOutputUpdatedColumns = true

	// column not updated, so ignore it.
	columnFlags := initColumnFlags(event.TableInfo)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, row)
	_, value, _, err := encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      event.TableInfo,
		Event:          row,
		CommitTs:       event.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, codecConfig, false, "")
	require.NoError(t, err)
	require.NotContains(t, string(value), "d")
	require.NotContains(t, string(value), "p")
}

func TestRowChanged2MsgOnlyHandleKeyColumns(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.ForceReplicate = util.AddressOf(true)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(id int primary key, a int)`)
	require.NotNil(t, job)
	insertEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 1)`)

	config := common.NewConfig(config.ProtocolOpen)
	config.DeleteOnlyHandleKeyColumns = true

	// column not updated, so ignore it.
	columnFlags := initColumnFlags(insertEvent.TableInfo)
	row, ok := insertEvent.GetNextRow()
	insertEvent.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	_, value, _, err := encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, false, "")
	require.NoError(t, err)
	require.Contains(t, string(value), "id")
	require.Contains(t, string(value), "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, _, err := encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.NoError(t, err)
	require.Contains(t, string(key), "ohk")
	require.Contains(t, string(value), "id")
	require.NotContains(t, string(value), "a")

	_ = helper.DDL2Event(`create table test.t1(id varchar(10), a varchar(10))`)
	insertEventNoHandleKey := helper.DML2Event("test", "t1", `insert into test.t1 values ("1", "1")`)
	columnFlags = initColumnFlags(insertEventNoHandleKey.TableInfo)
	row, ok = insertEventNoHandleKey.GetNextRow()
	insertEventNoHandleKey.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEventNoHandleKey.TableInfo,
		Event:          row,
		CommitTs:       insertEventNoHandleKey.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	row, ok = insertEvent.GetNextRow()
	insertEvent.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	row.PreRow = row.Row
	config.DeleteOnlyHandleKeyColumns = true
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, false, "")
	require.NoError(t, err)
	require.Contains(t, string(value), "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.NoError(t, err)
	require.Contains(t, string(key), "ohk")
	require.NotContains(t, string(value), "a")
	require.NotContains(t, string(value), "a")

	row, ok = insertEventNoHandleKey.GetNextRow()
	insertEventNoHandleKey.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	row.PreRow = row.Row
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEventNoHandleKey.TableInfo,
		Event:          row,
		CommitTs:       insertEventNoHandleKey.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	row, ok = insertEvent.GetNextRow()
	insertEvent.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	row.PreRow = row.Row
	row.Row = chunk.Row{}
	config.DeleteOnlyHandleKeyColumns = true
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, false, "")
	require.NoError(t, err)
	require.Contains(t, string(value), "id")
	require.NotContains(t, string(value), "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, false, "")
	require.NoError(t, err)
	require.Contains(t, string(value), "id")
	require.Contains(t, string(value), "a")

	config.DeleteOnlyHandleKeyColumns = false
	// key, value, err = encodeRowChangedEvent(&deleteEvent, config, true)
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		Event:          row,
		CommitTs:       insertEvent.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.NoError(t, err)
	require.Contains(t, string(key), "ohk")
	require.NotContains(t, string(value), "a")

	row, ok = insertEventNoHandleKey.GetNextRow()
	insertEventNoHandleKey.Rewind()
	require.True(t, ok)
	require.NotNil(t, row)
	row.PreRow = row.Row
	row.Row = chunk.Row{}
	key, value, _, err = encodeRowChangedEvent(&commonEvent.RowEvent{
		TableInfo:      insertEventNoHandleKey.TableInfo,
		Event:          row,
		CommitTs:       insertEventNoHandleKey.CommitTs,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}, columnFlags, config, true, "")
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)
}
