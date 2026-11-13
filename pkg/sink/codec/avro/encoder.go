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
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"go.uber.org/zap"
)

// BatchEncoder converts the events to binary Avro data
type BatchEncoder struct {
	keyspace   string
	schemaM    schemamanager.SchemaManager
	codecCache *CodecCache
	result     []*common.Message

	config *common.Config
}

// NewAvroEncoder returns an Avro encoder using the given schema manager.
func NewAvroEncoder(config *common.Config, schemaM schemamanager.SchemaManager) (common.EventEncoder, error) {
	if schemaM == nil {
		return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs("schema manager is nil")
	}
	return &BatchEncoder{
		keyspace:   config.ChangefeedID.Keyspace(),
		schemaM:    schemaM,
		codecCache: NewCodecCache(schemaM),
		result:     make([]*common.Message, 0, 1),
		config:     config,
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
		log.Error("avro encoding key failed",
			zap.String("protocol", "avro"),
			zap.String("part", "key"),
			zap.Stringer("changefeedID", a.config.ChangefeedID),
			zap.String("topic", topic),
			zap.String("schemaSubject", topicName2SchemaSubjects(topic, keySchemaSuffix)),
			zap.String("schema", e.TableInfo.TableName.Schema),
			zap.String("table", e.TableInfo.TableName.Table),
			zap.String("targetSchema", e.TableInfo.GetTargetSchemaName()),
			zap.String("targetTable", e.TableInfo.GetTargetTableName()),
			zap.Int64("tableID", e.PhysicalTableID),
			zap.Uint64("schemaVersion", e.TableInfo.GetUpdateTS()),
			zap.Uint64("startTs", e.StartTs),
			zap.Uint64("commitTs", e.CommitTs),
			zap.Error(err))
		return err
	}

	value, err := a.encodeValue(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding value failed",
			zap.String("protocol", "avro"),
			zap.String("part", "value"),
			zap.Stringer("changefeedID", a.config.ChangefeedID),
			zap.String("topic", topic),
			zap.String("schemaSubject", topicName2SchemaSubjects(topic, valueSchemaSuffix)),
			zap.String("schema", e.TableInfo.TableName.Schema),
			zap.String("table", e.TableInfo.TableName.Table),
			zap.String("targetSchema", e.TableInfo.GetTargetSchemaName()),
			zap.String("targetTable", e.TableInfo.GetTargetTableName()),
			zap.Int64("tableID", e.PhysicalTableID),
			zap.Uint64("schemaVersion", e.TableInfo.GetUpdateTS()),
			zap.Uint64("startTs", e.StartTs),
			zap.Uint64("commitTs", e.CommitTs),
			zap.Error(err))
		return err
	}

	message := common.NewMsg(key, value)
	message.Callback = e.Callback
	message.IncRowsCount()

	length := a.config.MessageLength(message)
	if length > a.config.MaxMessageBytes {
		log.Warn("single message is too large for avro",
			zap.String("protocol", "avro"),
			zap.Int("maxMessageBytes", a.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Stringer("table", e.TableInfo.TableName))
		return errors.ErrMessageTooLarge.GenWithStackByArgs(e.TableInfo.GetTargetTableName(), length, a.config.MaxMessageBytes)
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
			Schema:   e.GetTargetSchemaName(),
			Table:    e.GetTargetTableName(),
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
