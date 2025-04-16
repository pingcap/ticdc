// Copyright 2025 PingCAP, Inc.
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

package avro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// BatchEncoder converts the events to binary Avro data
type BatchEncoder struct {
	namespace string
	schemaM   SchemaManager
	result    []*common.Message

	config *common.Config
}

// NewAvroEncoder return a avro encoder.
func NewAvroEncoder(ctx context.Context, config *common.Config) (common.EventEncoder, error) {
	var schemaM SchemaManager
	var err error

	schemaRegistryType := config.SchemaRegistryType()
	switch schemaRegistryType {
	case common.SchemaRegistryTypeConfluent:
		schemaM, err = NewConfluentSchemaManager(ctx, config.AvroConfluentSchemaRegistry, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case common.SchemaRegistryTypeGlue:
		schemaM, err = NewGlueSchemaManager(ctx, config.AvroGlueSchemaRegistry)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs(schemaRegistryType)
	}
	return &BatchEncoder{
		namespace: config.ChangefeedID.Namespace(),
		schemaM:   schemaM,
		result:    make([]*common.Message, 0, 1),
		config:    config,
	}, nil
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *commonEvent.RowEvent,
) error {
	topic = sanitizeTopic(topic)

	key, err := a.encodeKey(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding key failed", zap.Error(err), zap.Any("event", e))
		return errors.Trace(err)
	}

	value, err := a.encodeValue(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding value failed", zap.Error(err), zap.Any("event", e))
		return errors.Trace(err)
	}

	message := common.NewMsg(key, value)
	message.Callback = e.Callback
	message.IncRowsCount()

	if message.Length() > a.config.MaxMessageBytes {
		log.Warn("Single message is too large for avro",
			zap.Int("maxMessageBytes", a.config.MaxMessageBytes),
			zap.Int("length", message.Length()),
			zap.Any("table", e.TableInfo.TableName))
		return errors.ErrMessageTooLarge.GenWithStackByArgs(message.Length())
	}

	a.result = append(a.result, message)
	return nil
}

// EncodeCheckpointEvent only encode checkpoint event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		data := []interface{}{checkpointByte, ts}
		for _, v := range data {
			err := binary.Write(buf, binary.BigEndian, v)
			if err != nil {
				return nil, errors.WrapError(errors.ErrAvroToEnvelopeError, err)
			}
		}

		value := buf.Bytes()
		return common.NewMsg(nil, value), nil
	}
	return nil, nil
}

// EncodeDDLEvent only encode DDL event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeDDLEvent(e *commonEvent.DDLEvent) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.BigEndian, ddlByte)

		event := &ddlEvent{
			Query:    e.Query,
			Type:     e.GetDDLType(),
			Schema:   e.GetSchemaName(),
			Table:    e.GetTableName(),
			CommitTs: e.GetCommitTs(),
		}
		data, err := json.Marshal(event)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroToEnvelopeError, err)
		}
		buf.Write(data)

		value := buf.Bytes()
		return common.NewMsg(nil, value), nil
	}

	return nil, nil
}

// Build Messages
func (a *BatchEncoder) Build() (messages []*common.Message) {
	result := a.result
	a.result = nil
	return result
}
