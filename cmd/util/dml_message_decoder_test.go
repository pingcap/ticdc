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

package util

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDMLMessageDecoderAttachesSharedData(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(11)
	decoder := &dmlMessageDecoderStub{messages: []*codeccommon.DMLMessage{first, second}}
	wrapped := NewDMLMessageDecoder(decoder)

	wrapped.AddKeyValue([]byte("key"), []byte("value"))
	decodedFirst := wrapped.NextDMLMessage()
	firstData, firstIndex := decodedFirst.SpillData()
	require.Equal(t, []byte("key"), firstData.Key)
	require.Equal(t, []byte("value"), firstData.Value)
	require.Zero(t, firstIndex)

	decodedSecond := wrapped.NextDMLMessage()
	secondData, secondIndex := decodedSecond.SpillData()
	require.Same(t, firstData, secondData)
	require.Equal(t, uint64(1), secondIndex)
}

type dmlMessageDecoderStub struct {
	messages []*codeccommon.DMLMessage
}

func (d *dmlMessageDecoderStub) AddKeyValue(_, _ []byte) {}

func (d *dmlMessageDecoderStub) HasNext() (codeccommon.MessageType, bool) {
	return codeccommon.MessageTypeRow, len(d.messages) > 0
}

func (d *dmlMessageDecoderStub) NextResolvedEvent() uint64 { return 0 }

func (d *dmlMessageDecoderStub) NextDMLMessage() *codeccommon.DMLMessage {
	if len(d.messages) == 0 {
		return nil
	}
	message := d.messages[0]
	d.messages = d.messages[1:]
	return message
}

func (d *dmlMessageDecoderStub) NextDDLEvent() *commonEvent.DDLEvent { return nil }
