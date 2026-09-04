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

import codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"

// DMLMessageDataFactory creates data shared by DML messages decoded from one
// input. It is called lazily when the decoder first returns a DML message.
type DMLMessageDataFactory func(codeccommon.Decoder, []byte, []byte) *codeccommon.DMLMessageData

// DMLMessageDecoder attaches spill data to DML messages as they are decoded.
// It keeps raw input only until EventsGroup has written it to the spill file.
type DMLMessageDecoder struct {
	codeccommon.Decoder

	key, value []byte
	data       *codeccommon.DMLMessageData
	factory    DMLMessageDataFactory
	restore    func(*codeccommon.DMLMessage) *codeccommon.DMLMessage
}

// NewDMLMessageDecoder wraps a decoder with the standard raw-message restorer.
func NewDMLMessageDecoder(decoder codeccommon.Decoder) *DMLMessageDecoder {
	return NewDMLMessageDecoderWithDataFactory(decoder,
		func(decoder codeccommon.Decoder, key, value []byte) *codeccommon.DMLMessageData {
			return NewDMLMessageData(decoder, key, value)
		})
}

// NewDMLMessageDecoderWithDataFactory is for decoders such as CSV whose
// restore decoder must be constructed from the input value.
func NewDMLMessageDecoderWithDataFactory(
	decoder codeccommon.Decoder, factory DMLMessageDataFactory,
) *DMLMessageDecoder {
	return &DMLMessageDecoder{Decoder: decoder, factory: factory}
}

// SetDMLMessageRestorer sets the per-input restore wrapper before AddKeyValue.
func (d *DMLMessageDecoder) SetDMLMessageRestorer(
	restore func(*codeccommon.DMLMessage) *codeccommon.DMLMessage,
) {
	d.restore = restore
}

// AddKeyValue implements codeccommon.Decoder.
func (d *DMLMessageDecoder) AddKeyValue(key, value []byte) {
	d.Decoder.AddKeyValue(key, value)
	d.SetRawMessage(key, value)
}

// SetRawMessage records input that was supplied while constructing a decoder,
// such as a CSV decoder. It does not pass the input to the wrapped decoder.
func (d *DMLMessageDecoder) SetRawMessage(key, value []byte) {
	d.key = key
	d.value = value
	d.data = nil
}

// NextDMLMessage implements codeccommon.Decoder.
func (d *DMLMessageDecoder) NextDMLMessage() *codeccommon.DMLMessage {
	message := d.Decoder.NextDMLMessage()
	if message != nil {
		d.attachDMLMessage(message)
	}
	return message
}

func (d *DMLMessageDecoder) attachDMLMessage(message *codeccommon.DMLMessage) {
	if d.data == nil {
		d.data = d.wrapRestore(d.factory(d.Decoder, d.key, d.value))
	}
	d.data.AttachDMLMessage(message)
}

// AttachCachedDMLMessage attaches data to a materialized DML message from
// Simple's DDL cache. It has no raw row payload to restore.
func (d *DMLMessageDecoder) AttachCachedDMLMessage(message *codeccommon.DMLMessage) {
	data := codeccommon.NewDMLMessageData(nil, nil,
		func([]byte, uint64) (*codeccommon.DMLMessage, error) { return message, nil })
	d.wrapRestore(data).AttachDMLMessage(message)
}

func (d *DMLMessageDecoder) wrapRestore(data *codeccommon.DMLMessageData) *codeccommon.DMLMessageData {
	if d.restore == nil {
		return data
	}
	restore := data.Restore
	data.Restore = func(data []byte, dmlIndex uint64) (*codeccommon.DMLMessage, error) {
		message, err := restore(data, dmlIndex)
		if err != nil {
			return nil, err
		}
		return d.restore(message), nil
	}
	return data
}

// Unwrap returns the decoder that produces protocol messages.
func (d *DMLMessageDecoder) Unwrap() codeccommon.Decoder {
	return d.Decoder
}
