// Copyright 2026 PingCAP, Inc.
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

package spool

import (
	"encoding/binary"
	"io"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

const (
	// A serialized batch starts with one uint32 storing message count.
	serializedMessageCountBytes = 4
	// Each serialized message writes three uint32 fields before payload:
	// key length, value length, and rows count.
	serializedMessageHeaderBytes = 12
)

type serializedMessageReader struct {
	data      []byte
	offset    int
	remaining uint32
}

func serializedMessagesSize(msgs []*common.Message) int {
	size := serializedMessageCountBytes
	for _, msg := range msgs {
		size += serializedMessageHeaderBytes
		size += len(msg.Key)
		size += len(msg.Value)
	}
	return size
}

func serializeMessages(msgs []*common.Message) []byte {
	data := make([]byte, serializedMessagesSize(msgs))
	offset := 0
	binary.LittleEndian.PutUint32(data[offset:], uint32(len(msgs)))
	offset += serializedMessageCountBytes

	for _, msg := range msgs {
		keyLen := uint32(len(msg.Key))
		valueLen := uint32(len(msg.Value))
		rows := uint32(msg.GetRowsCount())

		binary.LittleEndian.PutUint32(data[offset:], keyLen)
		offset += 4
		binary.LittleEndian.PutUint32(data[offset:], valueLen)
		offset += 4
		binary.LittleEndian.PutUint32(data[offset:], rows)
		offset += 4

		offset += copy(data[offset:], msg.Key)
		offset += copy(data[offset:], msg.Value)
	}
	return data
}

func newSerializedMessageReader(data []byte) (*serializedMessageReader, error) {
	if len(data) < serializedMessageCountBytes {
		return nil, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
	}

	offset := 0
	count := binary.LittleEndian.Uint32(data[offset:])
	offset += serializedMessageCountBytes
	// Every serialized message must consume at least one fixed-size header.
	// If count is already larger than the remaining payload could possibly hold,
	// treat the blob as corrupted and stop before make(..., count) allocates an
	// unreasonable amount of memory.
	maxCount := (len(data) - serializedMessageCountBytes) / serializedMessageHeaderBytes
	if uint64(count) > uint64(maxCount) {
		return nil, errors.ErrDecodeFailed.GenWithStack(
			"message count %d exceeds maximum %d for %d-byte payload",
			count,
			maxCount,
			len(data),
		)
	}
	return &serializedMessageReader{
		data:      data,
		offset:    offset,
		remaining: count,
	}, nil
}

func (r *serializedMessageReader) next() (key, value []byte, rowCount int, ok bool, err error) {
	if r.remaining == 0 {
		return nil, nil, 0, false, nil
	}
	if len(r.data[r.offset:]) < serializedMessageHeaderBytes {
		return nil, nil, 0, false, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
	}

	keyLen := int(binary.LittleEndian.Uint32(r.data[r.offset:]))
	r.offset += 4
	valueLen := int(binary.LittleEndian.Uint32(r.data[r.offset:]))
	r.offset += 4
	rowCount = int(binary.LittleEndian.Uint32(r.data[r.offset:]))
	r.offset += 4

	totalLen := keyLen + valueLen
	if len(r.data[r.offset:]) < totalLen {
		return nil, nil, 0, false, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
	}

	key = r.data[r.offset : r.offset+keyLen]
	r.offset += keyLen
	value = r.data[r.offset : r.offset+valueLen]
	r.offset += valueLen
	r.remaining--

	if keyLen == 0 {
		key = nil
	}
	return key, value, rowCount, true, nil
}
