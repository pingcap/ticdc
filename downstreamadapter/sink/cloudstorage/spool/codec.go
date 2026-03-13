// Copyright 2025 PingCAP, Inc.
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

func deserializeMessages(data []byte) ([]*common.Message, error) {
	if len(data) < serializedMessageCountBytes {
		return nil, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
	}

	offset := 0
	count := binary.LittleEndian.Uint32(data[offset:])
	offset += serializedMessageCountBytes

	var (
		result = make([]*common.Message, 0, count)
	)
	for i := uint32(0); i < count; i++ {
		if len(data[offset:]) < serializedMessageHeaderBytes {
			return nil, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
		}

		keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		valueLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		rowCount := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		totalLen := keyLen + valueLen
		if len(data[offset:]) < totalLen {
			return nil, errors.WrapError(errors.ErrDecodeFailed, io.ErrUnexpectedEOF)
		}

		key := data[offset : offset+keyLen]
		offset += keyLen
		value := data[offset : offset+valueLen]
		offset += valueLen

		if keyLen == 0 {
			key = nil
		}
		msg := &common.Message{
			Key:   key,
			Value: value,
		}
		msg.SetRowsCount(int(rowCount))
		result = append(result, msg)
	}
	return result, nil
}
