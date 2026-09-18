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
	"sync"

	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// DMLMessageDataFactory creates data shared by DML messages decoded from one
// input. It is called lazily when the decoder first returns a DML message.
type DMLMessageDataFactory func(codecCommon.Decoder, []byte, []byte) *codecCommon.DMLMessageData

// DMLMessageDecoder attaches spill data to DML messages as they are decoded.
// It keeps raw input only until EventsGroup has written it to the spill file.
type DMLMessageDecoder struct {
	codecCommon.Decoder

	key, value []byte
	data       *codecCommon.DMLMessageData
	factory    DMLMessageDataFactory
	restorer   *codecCommon.DMLMessageRestorer
	share      bool
	position   int64

	// restoreDecoderFactory builds the decoder of the restore path, and
	// restoreDecoder is the instance it built. A codec decoder holds the cursor
	// of the input it is decoding: the read loop writes an input into Decoder and
	// reads it back, so a spilled payload must not be restored with Decoder when
	// the resolve pipeline restores it on another goroutine. The restore decoder
	// is built on first use, and restoreMu serializes the restores of the resolve
	// pipeline and of a DDL flush.
	restoreDecoderFactory func() (codecCommon.Decoder, error)
	restoreMu             sync.Mutex
	restoreDecoder        codecCommon.Decoder
}

// NewDMLMessageDecoder wraps a decoder with the standard raw-message restorer.
func NewDMLMessageDecoder(decoder codecCommon.Decoder) *DMLMessageDecoder {
	d := NewDMLMessageDecoderWithDataFactory(decoder,
		func(decoder codecCommon.Decoder, key, value []byte) *codecCommon.DMLMessageData {
			return NewDMLMessageData(decoder, key, value)
		})
	d.share = true
	return d
}

// NewDMLMessageDecoderWithDataFactory is for decoders such as CSV whose
// restore decoder must be constructed from the input value.
func NewDMLMessageDecoderWithDataFactory(
	decoder codecCommon.Decoder, factory DMLMessageDataFactory,
) *DMLMessageDecoder {
	return &DMLMessageDecoder{Decoder: decoder, factory: factory}
}

// NewDMLMessageDecoderWithRestoreFactory wraps a decoder whose spilled payloads
// are restored with a decoder of their own, built by the factory on the first
// restore. Master restored payloads in the read loop, so the restore could reuse
// the decoder that decoded the input; the resolve pipeline restores on its own
// goroutine while the read loop keeps decoding, and one codec decoder keeps the
// cursor of the input it decodes, so the restore path needs its own.
func NewDMLMessageDecoderWithRestoreFactory(
	decoder codecCommon.Decoder, restoreDecoderFactory func() (codecCommon.Decoder, error),
) *DMLMessageDecoder {
	d := NewDMLMessageDecoderWithDataFactory(decoder, nil)
	d.share = true
	d.restoreDecoderFactory = restoreDecoderFactory
	return d
}

// SetSourcePosition records broker-specific source metadata, such as a Kafka
// offset. The position is persisted in every event descriptor and can be used
// by an EventsGroup post-restore hook without retaining a closure per input.
func (d *DMLMessageDecoder) SetSourcePosition(position int64) {
	d.position = position
}

// AddKeyValue implements codecCommon.Decoder.
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

// NextDMLMessage implements codecCommon.Decoder.
func (d *DMLMessageDecoder) NextDMLMessage() *codecCommon.DMLMessage {
	message := d.Decoder.NextDMLMessage()
	if message != nil {
		d.attachDMLMessage(message)
	}
	return message
}

func (d *DMLMessageDecoder) attachDMLMessage(message *codecCommon.DMLMessage) {
	if d.data == nil {
		d.data = d.newDMLMessageData()
		if d.share {
			if d.restorer == nil {
				d.restorer = d.data.Restorer
			} else {
				d.data.Restorer = d.restorer
			}
		}
		d.data.SourcePosition = d.position
	}
	d.data.AttachDMLMessage(message)
}

// newDMLMessageData builds the spill data of the input that is being decoded.
// The restore closure it installs is called wherever the payload is resolved,
// which is not necessarily the goroutine that decoded the input.
func (d *DMLMessageDecoder) newDMLMessageData() *codecCommon.DMLMessageData {
	if d.restoreDecoderFactory == nil {
		return d.factory(d.Decoder, d.key, d.value)
	}
	return codecCommon.NewDMLMessageData(d.key, d.value, d.restorePayload)
}

// restorePayload decodes one spilled payload with the decoder of the restore
// path, built on first use and kept for the next payload.
func (d *DMLMessageDecoder) restorePayload(data []byte) ([]*codecCommon.DMLMessage, error) {
	key, value, err := unmarshalDMLMessageData(data)
	if err != nil {
		return nil, err
	}

	d.restoreMu.Lock()
	defer d.restoreMu.Unlock()
	if d.restoreDecoder == nil {
		decoder, err := d.restoreDecoderFactory()
		if err != nil {
			return nil, errors.WrapError(errors.ErrSpillFileOp, err, "create DML restore decoder")
		}
		d.restoreDecoder = decoder
	}
	return restoreDMLMessages(d.restoreDecoder, key, value)
}

// AttachCachedDMLMessage attaches data to a materialized DML message from
// Simple's DDL cache. It has no raw row payload to restore.
func (d *DMLMessageDecoder) AttachCachedDMLMessage(message *codecCommon.DMLMessage) {
	data := codecCommon.NewDMLMessageData(nil, nil,
		func([]byte) ([]*codecCommon.DMLMessage, error) {
			return []*codecCommon.DMLMessage{message}, nil
		})
	data.SourcePosition = d.position
	data.AttachDMLMessage(message)
}

// Unwrap returns the decoder that produces protocol messages.
func (d *DMLMessageDecoder) Unwrap() codecCommon.Decoder {
	return d.Decoder
}
