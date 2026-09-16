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

func TestDMLMessageDecoderSharesRestorerAcrossInputs(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(11)
	decoder := &dmlMessageDecoderStub{messages: []*codeccommon.DMLMessage{first, second}}
	wrapped := NewDMLMessageDecoder(decoder)

	wrapped.SetSourcePosition(100)
	wrapped.AddKeyValue([]byte("first-key"), []byte("first-value"))
	firstData, _ := wrapped.NextDMLMessage().SpillData()

	wrapped.SetSourcePosition(101)
	wrapped.AddKeyValue([]byte("second-key"), []byte("second-value"))
	secondData, _ := wrapped.NextDMLMessage().SpillData()

	require.NotSame(t, firstData, secondData)
	require.Same(t, firstData.Restorer, secondData.Restorer)
	require.Equal(t, int64(100), firstData.SourcePosition)
	require.Equal(t, int64(101), secondData.SourcePosition)
}

func TestDMLMessageDecoderKeepsCustomRestorersPerInput(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(11)
	decoder := &dmlMessageDecoderStub{messages: []*codeccommon.DMLMessage{first, second}}
	wrapped := NewDMLMessageDecoderWithDataFactory(decoder,
		func(_ codeccommon.Decoder, key, value []byte) *codeccommon.DMLMessageData {
			return codeccommon.NewDMLMessageData(key, value,
				func([]byte) ([]*codeccommon.DMLMessage, error) { return nil, nil })
		})

	wrapped.AddKeyValue([]byte("first-key"), []byte("first-value"))
	firstData, _ := wrapped.NextDMLMessage().SpillData()
	wrapped.AddKeyValue([]byte("second-key"), []byte("second-value"))
	secondData, _ := wrapped.NextDMLMessage().SpillData()

	require.NotSame(t, firstData.Restorer, secondData.Restorer)
}

func TestSharedRestorerDecodesMultipleInputs(t *testing.T) {
	decoder := &resettableDMLDecoder{}
	wrapped := NewDMLMessageDecoder(decoder)
	group := NewEventsGroup(0, 1)

	for _, commitTs := range []byte{20, 10} {
		wrapped.AddKeyValue(nil, []byte{commitTs})
		message := wrapped.NextDMLMessage()
		require.NotNil(t, message)
		require.NoError(t, group.AppendMessage(message))
	}

	messages, err := group.GetAllMessages()
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 20}, []uint64{
		messages[0].GetCommitTs(), messages[1].GetCommitTs(),
	})
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

type resettableDMLDecoder struct {
	message *codeccommon.DMLMessage
}

func (d *resettableDMLDecoder) AddKeyValue(_, value []byte) {
	if len(value) == 0 {
		d.message = nil
		return
	}
	d.message = newTestDMLMessage(uint64(value[0]))
}

func (d *resettableDMLDecoder) HasNext() (codeccommon.MessageType, bool) {
	return codeccommon.MessageTypeRow, d.message != nil
}

func (d *resettableDMLDecoder) NextResolvedEvent() uint64 { return 0 }

func (d *resettableDMLDecoder) NextDMLMessage() *codeccommon.DMLMessage {
	message := d.message
	d.message = nil
	return message
}

func (d *resettableDMLDecoder) NextDDLEvent() *commonEvent.DDLEvent { return nil }
