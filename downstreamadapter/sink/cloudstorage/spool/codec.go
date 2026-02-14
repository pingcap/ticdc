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
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
)

func serializeMessages(msgs []*codeccommon.Message) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(msgs))); err != nil {
		return nil, errors.Trace(err)
	}
	for _, msg := range msgs {
		keyLen := uint32(len(msg.Key))
		valueLen := uint32(len(msg.Value))
		rows := uint32(msg.GetRowsCount())
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return nil, errors.Trace(err)
		}
		if err := binary.Write(buf, binary.LittleEndian, valueLen); err != nil {
			return nil, errors.Trace(err)
		}
		if err := binary.Write(buf, binary.LittleEndian, rows); err != nil {
			return nil, errors.Trace(err)
		}
		if _, err := buf.Write(msg.Key); err != nil {
			return nil, errors.Trace(err)
		}
		if _, err := buf.Write(msg.Value); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return buf.Bytes(), nil
}

func deserializeMessages(data []byte) ([]*codeccommon.Message, error) {
	reader := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(reader, binary.LittleEndian, &count); err != nil {
		return nil, errors.Trace(err)
	}

	result := make([]*codeccommon.Message, 0, count)
	for i := uint32(0); i < count; i++ {
		var keyLen uint32
		var valueLen uint32
		var rows uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, errors.Trace(err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return nil, errors.Trace(err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &rows); err != nil {
			return nil, errors.Trace(err)
		}

		key := make([]byte, keyLen)
		if _, err := reader.Read(key); err != nil {
			return nil, errors.Trace(err)
		}
		value := make([]byte, valueLen)
		if _, err := reader.Read(value); err != nil {
			return nil, errors.Trace(err)
		}
		msg := codeccommon.NewMsg(key, value)
		msg.SetRowsCount(int(rows))
		result = append(result, msg)
	}
	return result, nil
}
