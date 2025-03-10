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
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCSVBatchCodec(t *testing.T) {
	s := commonEvent.NewEventTestHelper(t)
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
		err := encoder.AppendTxnEvent(cs)
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

func TestCSVAppendRowChangedEventWithCallback(t *testing.T) {
	encoder := NewTxnEventEncoder(&common.Config{
		Delimiter:       ",",
		Quote:           "\"",
		Terminator:      "\n",
		NullString:      "\\N",
		IncludeCommitTs: true,
	})
	require.NotNil(t, encoder)

	count := 0

	s := commonEvent.NewEventTestHelper(t)
	s.DDL2Job("create table test.table1(col1 int primary key)")
	txn := s.DML2Event("test", "table1", "insert into test.table1 values (1)")
	callback := func() {
		count += 1
	}
	txn.AddPostFlushFunc(callback)

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the event.
	err := encoder.AppendTxnEvent(txn)
	require.Nil(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected all callbacks to be called")
}
