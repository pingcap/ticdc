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
	"math"
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDMLMessageDecoderAttachesSharedData(t *testing.T) {
	first := newTestDMLMessage(10)
	second := newTestDMLMessage(11)
	decoder := &dmlMessageDecoderStub{messages: []*codecCommon.DMLMessage{first, second}}
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
	decoder := &dmlMessageDecoderStub{messages: []*codecCommon.DMLMessage{first, second}}
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
	decoder := &dmlMessageDecoderStub{messages: []*codecCommon.DMLMessage{first, second}}
	wrapped := NewDMLMessageDecoderWithDataFactory(decoder,
		func(_ codecCommon.Decoder, key, value []byte) *codecCommon.DMLMessageData {
			return codecCommon.NewDMLMessageData(key, value,
				func([]byte) ([]*codecCommon.DMLMessage, error) { return nil, nil })
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
	messages []*codecCommon.DMLMessage
}

func (d *dmlMessageDecoderStub) AddKeyValue(_, _ []byte) {}

func (d *dmlMessageDecoderStub) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeRow, len(d.messages) > 0
}

func (d *dmlMessageDecoderStub) NextResolvedEvent() uint64 { return 0 }

func (d *dmlMessageDecoderStub) NextDMLMessage() *codecCommon.DMLMessage {
	if len(d.messages) == 0 {
		return nil
	}
	message := d.messages[0]
	d.messages = d.messages[1:]
	return message
}

func (d *dmlMessageDecoderStub) NextDDLEvent() *commonEvent.DDLEvent { return nil }

type resettableDMLDecoder struct {
	message *codecCommon.DMLMessage
}

func (d *resettableDMLDecoder) AddKeyValue(_, value []byte) {
	if len(value) == 0 {
		d.message = nil
		return
	}
	d.message = newTestDMLMessage(uint64(value[0]))
}

func (d *resettableDMLDecoder) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeRow, d.message != nil
}

func (d *resettableDMLDecoder) NextResolvedEvent() uint64 { return 0 }

func (d *resettableDMLDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	message := d.message
	d.message = nil
	return message
}

func (d *resettableDMLDecoder) NextDDLEvent() *commonEvent.DDLEvent { return nil }

// TestRestoreUsesItsOwnDecoder pins the constraint the parallel resolve path has
// to keep: the read loop decodes an input with the decoder of the partition
// while the resolve pipeline restores spilled payloads, and one codec decoder
// holds the cursor of the input it decodes, so a restore must not run on the
// decoder of the read loop.
func TestRestoreUsesItsOwnDecoder(t *testing.T) {
	read := &recordingDMLDecoder{message: newTestDMLMessage(10)}
	restore := &recordingDMLDecoder{message: newTestDMLMessage(10)}
	created := 0
	wrapped := NewDMLMessageDecoderWithRestoreFactory(read,
		func() (codecCommon.Decoder, error) {
			created++
			return restore, nil
		})

	// The read loop decodes one input and keeps the message for the spill.
	wrapped.AddKeyValue([]byte("key"), []byte("value"))
	message := wrapped.NextDMLMessage()
	require.NotNil(t, message)
	require.Equal(t, 1, read.inputs)

	// Resolving the spilled payload restores it with the decoder of the restore
	// path, and leaves the decoder of the read loop alone.
	store := NewSpillStore()
	t.Cleanup(func() { require.NoError(t, store.Cleanup()) })
	group := NewEventsGroup(0, 1, store)
	require.NoError(t, group.AppendMessage(message))
	require.Equal(t, 1, read.inputs)

	batch, hasMore, err := group.PrepareResolve(math.MaxUint64, store.ResolveLimit())
	require.NoError(t, err)
	require.False(t, hasMore)
	require.NotNil(t, batch)
	require.Len(t, batch.Messages, 1)
	require.NoError(t, batch.Ack())

	require.Equal(t, 1, created, "the restore decoder is built on the first restore")
	require.Equal(t, 1, restore.inputs)
	require.Equal(t, 1, read.inputs, "the read decoder stays with the read loop")
}

// recordingDMLDecoder counts the inputs it decodes and returns one message per
// input, so a test can tell which decoder decoded or restored a payload.
type recordingDMLDecoder struct {
	message  *codecCommon.DMLMessage
	inputs   int
	returned bool
}

func (d *recordingDMLDecoder) AddKeyValue(_, _ []byte) {
	d.inputs++
	d.returned = false
}

func (d *recordingDMLDecoder) HasNext() (codecCommon.MessageType, bool) {
	return codecCommon.MessageTypeRow, !d.returned
}

func (d *recordingDMLDecoder) NextResolvedEvent() uint64 { return 0 }

func (d *recordingDMLDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	d.returned = true
	return d.message
}

func (d *recordingDMLDecoder) NextDDLEvent() *commonEvent.DDLEvent { return nil }
