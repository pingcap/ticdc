// Copyright 2022 PingCAP, Inc.
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
	"context"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildOpenProtocolBatchEncoder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	codecConfig := codecCommon.NewConfig(config.ProtocolOpen)
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	impl, ok := encoder.(*batchEncoder)
	require.True(t, ok)
	require.NotNil(t, impl.config)
}

func TestMaxMessageBytes(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a int primary key, b varchar(10))`)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, "aa")`)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		PhysicalTableID: dmlEvent.PhysicalTableID,
		StartTs:         dmlEvent.StartTs,
		CommitTs:        dmlEvent.CommitTs,
		TableInfo:       tableInfo,
		Event:           row,
		ColumnSelector:  columnselector.NewDefaultColumnSelector(),
		Callback:        func() {},
	}

	ctx := context.Background()

	cfg := codecCommon.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1)
	encoder, err := NewBatchEncoder(ctx, cfg)
	require.NoError(t, err)
	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.ErrorIs(t, err, errors.ErrMessageTooLarge)

	cfg = codecCommon.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1024 * 1024)
	encoder, err = NewBatchEncoder(ctx, cfg)
	require.NoError(t, err)
	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	msgs := encoder.Build()
	require.Len(t, msgs, 1)
	require.LessOrEqual(t, msgs[0].Length(), cfg.MaxMessageBytes)
	require.Equal(t, 1, msgs[0].GetRowsCount())
}

func TestMaxBatchSizeAndCallback(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a int primary key, b varchar(10))`)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, "aa")`)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	baseEvent := commonEvent.RowEvent{
		PhysicalTableID: dmlEvent.PhysicalTableID,
		StartTs:         dmlEvent.StartTs,
		CommitTs:        dmlEvent.CommitTs,
		TableInfo:       tableInfo,
		Event:           row,
		ColumnSelector:  columnselector.NewDefaultColumnSelector(),
	}

	cfg := codecCommon.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1024 * 1024)
	cfg.MaxBatchSize = 2

	ctx := context.Background()
	encoder, err := NewBatchEncoder(ctx, cfg)
	require.NoError(t, err)

	count := 0
	callbacks := []func(){
		func() { count += 1 },
		func() { count += 2 },
		func() { count += 3 },
		func() { count += 4 },
		func() { count += 5 },
	}
	for _, cb := range callbacks {
		ev := baseEvent
		ev.Callback = cb
		require.NoError(t, encoder.AppendRowChangedEvent(ctx, "", &ev))
	}
	require.Equal(t, 0, count)

	msgs := encoder.Build()
	require.Len(t, msgs, 3)

	msgs[0].Callback()
	require.Equal(t, 3, count)
	msgs[1].Callback()
	require.Equal(t, 10, count)
	msgs[2].Callback()
	require.Equal(t, 15, count)
}
