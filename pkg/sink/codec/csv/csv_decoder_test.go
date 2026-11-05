// Copyright 2022 PingCAP, Inc.
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
package csv

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchDecoder(t *testing.T) {
	csvData := `"I","employee","hr",433305438660591626,101,"Smith","Bob","2014-06-04","New York"
"U","employee","hr",433305438660591627,101,"Smith","Bob","2015-10-08","Los Angeles"
"D","employee","hr",433305438660591629,101,"Smith","Bob","2017-03-13","Dallas"
"I","employee","hr",433305438660591630,102,"Alex","Alice","2017-03-14","Shanghai"
"U","employee","hr",433305438660591630,102,"Alex","Alice","2018-06-15","Beijing"
`
	ctx := context.Background()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Job("create database hr")
	createTableDDL := helper.DDL2Event("create table hr.employee(Id int, LastName varchar(255), FirstName varchar(255), HireDate date, OfficeLocation varchar(255), primary key(Id, LastName))")

	codecConfig := &common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	}
	decoder, err := NewDecoder(ctx, codecConfig, createTableDDL.TableInfo, []byte(csvData))
	require.NoError(t, err)

	for i, commitTs := range []uint64{
		433305438660591626, 433305438660591627, 433305438660591629,
		433305438660591630, 433305438660591630,
	} {
		tp, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, tp)
		message := decoder.NextDMLMessage()
		require.Equal(t, commitTs, message.GetCommitTs())
		event := message.ToDMLEvent()
		require.Equal(t, commitTs, event.CommitTs)
		if i == 1 || i == 4 {
			require.Equal(t, []commonType.RowType{commonType.RowTypeDelete, commonType.RowTypeInsert}, event.RowTypes)
			require.EqualValues(t, 2, event.Len())
			before, after := event.Rows.GetRow(0), event.Rows.GetRow(1)
			require.Equal(t, after.GetInt64(0), before.GetInt64(0))
			require.Equal(t, after.GetBytes(1), before.GetBytes(1))
			for _, column := range []int{2, 3, 4} {
				require.True(t, before.IsNull(column))
			}
			location := "Los Angeles"
			if i == 4 {
				location = "Beijing"
			}
			require.Equal(t, location, string(after.GetBytes(4)))
		} else {
			rowType := commonType.RowTypeInsert
			if i == 2 {
				rowType = commonType.RowTypeDelete
			}
			require.Equal(t, []commonType.RowType{rowType}, event.RowTypes)
			require.EqualValues(t, 1, event.Len())
		}
		event.PostFlush()
	}

	_, hasNext := decoder.HasNext()
	require.False(t, hasNext)
}

func TestCSVBatchDecoderWithColumnSelector(t *testing.T) {
	csvData := `"I","t","test",433305438660591626,1,"visible-value"
`
	ctx := context.Background()
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	createTableDDL := helper.DDL2Event(
		"create table test.t(id int primary key, visible varchar(255), secret varchar(255))")

	selectors, err := columnselector.New(&config.SinkConfig{
		ColumnSelectors: []*config.ColumnSelector{
			{Matcher: []string{"test.t"}, Columns: []string{"id", "visible"}},
		},
	}, false)
	require.NoError(t, err)

	codecConfig := &common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	}
	decoder, err := NewDecoderWithColumnSelector(
		ctx,
		codecConfig,
		createTableDDL.TableInfo,
		[]byte(csvData),
		selectors.GetForTableInfo(createTableDDL.TableInfo),
	)
	require.NoError(t, err)

	tp, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, tp)

	event := decoder.NextDMLMessage().ToDMLEvent()
	require.Len(t, event.TableInfo.GetColumns(), 2)
	require.Equal(t, "id", event.TableInfo.GetColumns()[0].Name.O)
	require.Equal(t, "visible", event.TableInfo.GetColumns()[1].Name.O)
	row, ok := event.GetNextRow()
	require.True(t, ok)
	require.Equal(t, int64(1), row.Row.GetInt64(0))
	require.Equal(t, "visible-value", string(row.Row.GetBytes(1)))

	_, hasNext = decoder.HasNext()
	require.False(t, hasNext)
}
