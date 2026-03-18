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

package canal

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONTxnEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	encoder := NewJSONTxnEventEncoder(cfg)
	require.NotNil(t, encoder)
}

func TestCanalJSONTxnEventEncoderMaxMessageBytes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)
	testEvent := helper.DML2Event("test", "t", `insert into test.t values("aa")`)

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	cfg := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)
	encoder := NewJSONTxnEventEncoder(cfg)
	err := encoder.AppendTxnEvent(testEvent)
	require.Nil(t, err)

	// the test message length is larger than max-message-bytes
	cfg = cfg.WithMaxMessageBytes(100)
	encoder = NewJSONTxnEventEncoder(cfg)
	err = encoder.AppendTxnEvent(testEvent)
	require.NotNil(t, err)
}

func TestCanalJSONAppendTxnEventEncoderWithCallback(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)

	cfg := common.NewConfig(config.ProtocolCanalJSON)
	encoder := NewJSONTxnEventEncoder(cfg)
	require.NotNil(t, encoder)

	event := helper.DML2Event("test", "t", `insert into test.t values("aa")`, `insert into test.t values("bb")`)

	count := 0

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	event.AddPostFlushFunc(func() {
		count++
	})
	err := encoder.AppendTxnEvent(event)
	require.Nil(t, err)
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 1, "expected one message")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected one callback be called")
	// Assert the build reset all the internal states.
	require.Nil(t, encoder.(*JSONTxnEventEncoder).txnSchema)
	require.Nil(t, encoder.(*JSONTxnEventEncoder).txnTable)
	require.Nil(t, encoder.(*JSONTxnEventEncoder).callback)
	require.Equal(t, 0, encoder.(*JSONTxnEventEncoder).batchSize)
	require.Equal(t, 0, encoder.(*JSONTxnEventEncoder).valueBuf.Len())
}
