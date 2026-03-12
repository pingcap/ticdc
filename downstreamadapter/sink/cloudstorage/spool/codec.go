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
	"bytes"
	"encoding/binary"

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

func serializeMessages(msgs []*common.Message) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, serializedMessagesSize(msgs)))
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(msgs))); err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	for _, msg := range msgs {
		keyLen := uint32(len(msg.Key))
		valueLen := uint32(len(msg.Value))
		rows := uint32(msg.GetRowsCount())
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return nil, errors.WrapError(errors.ErrEncodeFailed, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, valueLen); err != nil {
			return nil, errors.WrapError(errors.ErrEncodeFailed, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, rows); err != nil {
			return nil, errors.WrapError(errors.ErrEncodeFailed, err)
		}
		if _, err := buf.Write(msg.Key); err != nil {
			return nil, errors.WrapError(errors.ErrEncodeFailed, err)
		}
		if _, err := buf.Write(msg.Value); err != nil {
			return nil, errors.WrapError(errors.ErrEncodeFailed, err)
		}
	}
	return buf.Bytes(), nil
}

func deserializeMessages(data []byte) ([]*common.Message, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, errors.WrapError(errors.ErrDecodeFailed, err)
	}

	var (
		keyLen   uint32
		valueLen uint32
		rowCount uint32
		result   = make([]*common.Message, 0, count)
	)
	for i := uint32(0); i < count; i++ {
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, errors.WrapError(errors.ErrDecodeFailed, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return nil, errors.WrapError(errors.ErrDecodeFailed, err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &rowCount); err != nil {
			return nil, errors.WrapError(errors.ErrDecodeFailed, err)
		}

		key := make([]byte, keyLen)
		if _, err := reader.Read(key); err != nil {
			return nil, errors.WrapError(errors.ErrDecodeFailed, err)
		}
		value := make([]byte, valueLen)
		if _, err := reader.Read(value); err != nil {
			return nil, errors.WrapError(errors.ErrDecodeFailed, err)
		}
		msg := common.NewMsg(key, value)
		msg.SetRowsCount(int(rowCount))
		result = append(result, msg)
	}
	return result, nil
}
