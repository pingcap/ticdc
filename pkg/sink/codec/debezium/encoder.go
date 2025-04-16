// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"bytes"
	"context"
	"time"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// BatchEncoder encodes message into Debezium format.
type BatchEncoder struct {
	messages []*common.Message

	config *common.Config
	codec  *dbzCodec
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if !d.config.EnableTiDBExtension {
		return nil, nil
	}
	keyMap := bytes.Buffer{}
	valueBuf := bytes.Buffer{}
	err := d.codec.EncodeCheckpointEvent(ts, &keyMap, &valueBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	key, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		keyMap.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	value, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	result := common.NewMsg(key, value)
	return result, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *commonEvent.RowEvent,
) error {
	var key []byte
	var value []byte
	var err error
	if key, err = d.encodeKey(e); err != nil {
		return errors.Trace(err)
	}
	if value, err = d.encodeValue(e); err != nil {
		return errors.Trace(err)
	}
	m := &common.Message{
		Key:      key,
		Value:    value,
		Callback: e.Callback,
	}
	m.IncRowsCount()

	d.messages = append(d.messages, m)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(e *commonEvent.DDLEvent) (*common.Message, error) {
	valueBuf := bytes.Buffer{}
	keyMap := bytes.Buffer{}
	err := d.codec.EncodeDDLEvent(e, &keyMap, &valueBuf)
	if err != nil {
		if errors.ErrDDLUnsupportType.Equal(err) {
			log.Warn("encode ddl event failed, just ignored", zap.Error(err))
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	key, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		keyMap.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	value, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	if err != nil {
		return nil, err
	}
	result := common.NewMsg(key, value)

	return result, nil
}

func (d *BatchEncoder) encodeKey(e *commonEvent.RowEvent) ([]byte, error) {
	keyBuf := bytes.Buffer{}
	err := d.codec.EncodeKey(e, &keyBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	key, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		keyBuf.Bytes(),
	)
	return key, err
}

func (d *BatchEncoder) encodeValue(e *commonEvent.RowEvent) ([]byte, error) {
	valueBuf := bytes.Buffer{}
	err := d.codec.EncodeValue(e, &valueBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	value, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	return value, err
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() []*common.Message {
	if len(d.messages) == 0 {
		return nil
	}

	result := d.messages
	d.messages = nil
	return result
}

func (d *BatchEncoder) Clean() {}

// newBatchEncoder creates a new Debezium BatchEncoder.
func NewBatchEncoder(c *common.Config, clusterID string) common.EventEncoder {
	batch := &BatchEncoder{
		messages: nil,
		config:   c,
		codec: &dbzCodec{
			config:    c,
			clusterID: clusterID,
			nowFunc:   time.Now,
		},
	}
	return batch
}
