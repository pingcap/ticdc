// Copyright 2023 PingCAP, Inc.
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
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONTxnEventEncoder(t *testing.T) {
	t.Parallel()

	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
	encoder := NewJSONTxnEventEncoder(cfg)
	impl, ok := encoder.(*JSONTxnEventEncoder)
	require.True(t, ok)
	require.NotNil(t, impl.config)
}

func TestCanalJSONTxnEventEncoderMaxMessageBytes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Event(`create table test.t(a varchar(255) primary key)`)
	testEvent := helper.DML2Event("test", "t", `insert into test.t values("aa")`)

	// Large limit should pass.
	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(1024 * 1024)
	encoder := NewJSONTxnEventEncoder(cfg)
	require.NoError(t, encoder.AppendTxnEvent(testEvent))

	// Very small limit should fail.
	cfg = codecCommon.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(1)
	encoder = NewJSONTxnEventEncoder(cfg)
	require.Error(t, encoder.AppendTxnEvent(testEvent))
}

func TestCanalJSONAppendTxnEventEncoderWithCallback(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Event(`create table test.t(a varchar(255) primary key)`)

	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
	encoder := NewJSONTxnEventEncoder(cfg)
	require.NotNil(t, encoder)

	// Empty build makes sure the build logic is safe.
	msgs := encoder.Build()
	require.Nil(t, msgs)

	event := helper.DML2Event("test", "t", `insert into test.t values("aa"),("bb")`)
	count := 0
	event.AddPostFlushFunc(func() { count++ })

	require.NoError(t, encoder.AppendTxnEvent(event))
	require.Equal(t, 0, count)

	msgs = encoder.Build()
	require.Len(t, msgs, 1)
	require.NotNil(t, msgs[0].Callback)
	msgs[0].Callback()
	require.Equal(t, 1, count)

	// Assert build resets internal states.
	impl := encoder.(*JSONTxnEventEncoder)
	require.Nil(t, impl.txnSchema)
	require.Nil(t, impl.txnTable)
	require.Nil(t, impl.callback)
	require.Equal(t, 0, impl.batchSize)
	require.Equal(t, 0, impl.valueBuf.Len())
}

