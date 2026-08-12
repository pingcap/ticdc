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

package csv

import (
	"strings"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	sinkhelper "github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func appendTxnEventForTest(
	encoder common.TxnEventEncoder,
	event *commonEvent.DMLEvent,
	selector commonEvent.Selector,
) error {
	return encoder.AppendTxnEvent(sinkhelper.NewRowEvents(event, selector, nil))
}

func TestCSVBatchCodec(t *testing.T) {
	s := commonEvent.NewEventTestHelper(t)
	defer s.Close()
	s.DDL2Job("create table test.table1(col1 int primary key)")
	event1 := s.DML2Event("test", "table1", "insert into test.table1 values (1)")
	event2 := s.DML2Event("test", "table1", "insert into test.table1 values (2)")
	testCases := []*commonEvent.DMLEvent{event1, event2}

	for _, cs := range testCases {
		encoder := NewTxnEventEncoder(&common.Config{
			Delimiter:       ",",
			Quote:           "\"",
			Terminator:      "\n",
			NullString:      "\\N",
			IncludeCommitTs: true,
		})
		err := appendTxnEventForTest(encoder, cs, nil)
		require.Nil(t, err)
		messages := encoder.Build()
		if cs.Len() == 0 {
			require.Nil(t, messages)
			continue
		}
		require.Len(t, messages, 1)
		require.Equal(t, int(cs.Len()), messages[0].GetRowsCount())
	}
}

func TestCSVAppendTxnEventWithCallback(t *testing.T) {
	encoder := NewTxnEventEncoder(&common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	})
	require.NotNil(t, encoder)

	s := commonEvent.NewEventTestHelper(t)
	defer s.Close()
	s.DDL2Job("create table test.table1(col1 int primary key)")
	txn := s.DML2Event("test", "table1", "insert into test.table1 values (1)")
	count := 0

	// Empty build makes sure the build path handles an empty encoder.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the event.
	err := encoder.AppendTxnEvent(sinkhelper.NewRowEvents(txn, nil, func() {
		count++
	}))
	require.Nil(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	require.NotNil(t, msgs[0].Callback)
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected one callback be called")
}

func TestCSVBatchCodecWithHeader(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ddl := helper.DDL2Event("create table test.table1(col1 int primary key)")
	require.NotNil(t, ddl)
	event := helper.DML2Event("test", "table1", "insert into test.table1 values (1)", "insert into test.table1 values (2)")

	cfg := &common.Config{
		Delimiter:            ",",
		Quote:                "\"",
		Terminator:           "\n",
		NullString:           "\\N",
		IncludeCommitTs:      true,
		CSVOutputFieldHeader: true,
	}
	encoder := NewTxnEventEncoder(cfg)
	err := appendTxnEventForTest(encoder, event, nil)
	require.Nil(t, err)
	messages := encoder.Build()
	require.Len(t, messages, 1)
	header := strings.Split(string(messages[0].Key), cfg.Terminator)[0]
	require.Equal(t, "ticdc-meta$operation,ticdc-meta$table,ticdc-meta$schema,ticdc-meta$commit-ts,col1", header)
	require.Equal(t, int(event.Length), messages[0].GetRowsCount())

	cfg.CSVOutputFieldHeader = false
	encoder = NewTxnEventEncoder(cfg)
	err = appendTxnEventForTest(encoder, event, nil)
	require.Nil(t, err)
	messages1 := encoder.Build()
	require.Len(t, messages1, 1)
	require.Equal(t, messages1[0].Value, messages[0].Value)
	require.Equal(t, int(event.Length), messages1[0].GetRowsCount())

	cfg.CSVOutputFieldHeader = true
	event.RowTypes = nil
	encoder = NewTxnEventEncoder(cfg)
	err = appendTxnEventForTest(encoder, event, nil)
	require.Nil(t, err)
	messages = encoder.Build()
	require.Len(t, messages, 0)
}

func TestCSVTxnEventEncoderWithColumnSelector(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.DDL2Event("create table test.table1(col1 int primary key, col2 varchar(255))")
	event := helper.DML2Event("test", "table1", `insert into test.table1 values (1, "filtered")`)

	selectors, err := columnselector.New(&config.SinkConfig{
		ColumnSelectors: []*config.ColumnSelector{
			{Matcher: []string{"test.table1"}, Columns: []string{"col1"}},
		},
	})
	require.NoError(t, err)

	cfg := &common.Config{
		Delimiter:            ",",
		Quote:                "\"",
		Terminator:           "\n",
		NullString:           "\\N",
		IncludeCommitTs:      true,
		CSVOutputFieldHeader: true,
	}
	encoder := NewTxnEventEncoder(cfg)
	require.NoError(t, appendTxnEventForTest(encoder, event, selectors.GetForTableInfo(event.TableInfo)))
	messages := encoder.Build()
	require.Len(t, messages, 1)
	require.Equal(t, "ticdc-meta$operation,ticdc-meta$table,ticdc-meta$schema,ticdc-meta$commit-ts,col1\n", string(messages[0].Key))
	require.NotContains(t, string(messages[0].Key), "col2")
	require.NotContains(t, string(messages[0].Value), "filtered")
}

func TestCSVTxnEventEncoderWithColumnSelectorForUpdateAndDelete(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.DDL2Event("create table test.table1(id int primary key, visible varchar(255), secret varchar(255))")
	updateEvent, _ := helper.DML2UpdateEvent(
		"test",
		"table1",
		`insert into test.table1 values (1, "visible-before", "secret-before")`,
		`update test.table1 set visible = "visible-after", secret = "secret-after" where id = 1`,
	)
	deleteEvent := helper.DML2DeleteEvent(
		"test",
		"table1",
		`insert into test.table1 values (2, "delete-visible", "delete-secret")`,
		`delete from test.table1 where id = 2`,
	)

	selectors, err := columnselector.New(&config.SinkConfig{
		ColumnSelectors: []*config.ColumnSelector{
			{Matcher: []string{"test.table1"}, Columns: []string{"id", "visible"}},
		},
	})
	require.NoError(t, err)

	cfg := &common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		OutputOldValue:  true,
		IncludeCommitTs: false,
	}
	selector := selectors.GetForTableInfo(updateEvent.TableInfo)
	encoder := NewTxnEventEncoder(cfg)
	require.NoError(t, appendTxnEventForTest(encoder, updateEvent, selector))
	require.NoError(t, appendTxnEventForTest(encoder, deleteEvent, selector))

	messages := encoder.Build()
	require.Len(t, messages, 1)
	value := string(messages[0].Value)
	require.Contains(t, value, "visible-before")
	require.Contains(t, value, "visible-after")
	require.Contains(t, value, "delete-visible")
	require.NotContains(t, value, "secret-before")
	require.NotContains(t, value, "secret-after")
	require.NotContains(t, value, "delete-secret")
}
