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

package outbox

import (
	"context"
	"fmt"
	"sort"

	commonpkg "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
)

const outboxIDHeaderKey = "Id"

// Encoder encodes one insert row into one outbox message.
type Encoder struct {
	config         *codecCommon.Config
	headerBindings []headerBinding
	messages       []*codecCommon.Message
}

type headerBinding struct {
	header string
	column string
}

// NewEncoder creates an outbox-json encoder that emits one message per insert
// row.
func NewEncoder(_ context.Context, config *codecCommon.Config) (codecCommon.EventEncoder, error) {
	headerBindings := make([]headerBinding, 0, len(config.OutboxHeaderColumns))
	headerNames := make([]string, 0, len(config.OutboxHeaderColumns))
	for header := range config.OutboxHeaderColumns {
		headerNames = append(headerNames, header)
	}
	sort.Strings(headerNames)
	for _, header := range headerNames {
		headerBindings = append(headerBindings, headerBinding{
			header: header,
			column: config.OutboxHeaderColumns[header],
		})
	}

	return &Encoder{
		config:         config,
		headerBindings: headerBindings,
		messages:       make([]*codecCommon.Message, 0, 1),
	}, nil
}

// AppendRowChangedEvent encodes a single insert row into one outbox message.
func (e *Encoder) AppendRowChangedEvent(
	_ context.Context, _ string, event *commonEvent.RowEvent,
) error {
	// The sink pipeline already filters non-insert rows for outbox-json.
	// Keep this guard to avoid generating unexpected messages.
	if !event.IsInsert() {
		return nil
	}

	idValue, err := getRequiredColumnValue(event, e.config.OutboxIDColumn)
	if err != nil {
		return err
	}
	keyValue, err := getRequiredColumnValue(event, e.config.OutboxKeyColumn)
	if err != nil {
		return err
	}
	payloadValue, err := getRequiredColumnValue(event, e.config.OutboxValueColumn)
	if err != nil {
		return err
	}

	msg := codecCommon.NewMsg(keyValue, payloadValue)
	msg.Callback = event.Callback
	msg.IncRowsCount()
	msg.Headers = make([]codecCommon.MessageHeader, 0, 1+len(e.headerBindings))
	msg.Headers = append(msg.Headers, codecCommon.MessageHeader{
		Key:   outboxIDHeaderKey,
		Value: idValue,
	})
	for _, binding := range e.headerBindings {
		value, err := getRequiredColumnValue(event, binding.column)
		if err != nil {
			return err
		}
		msg.Headers = append(msg.Headers, codecCommon.MessageHeader{
			Key:   binding.header,
			Value: value,
		})
	}

	if msg.Length() > e.config.MaxMessageBytes {
		return errors.ErrMessageTooLarge.GenWithStackByArgs(
			event.TableInfo.GetTableName(), msg.Length(), e.config.MaxMessageBytes)
	}
	e.messages = append(e.messages, msg)
	return nil
}

// Build returns the buffered outbox messages and clears the encoder buffer.
func (e *Encoder) Build() []*codecCommon.Message {
	if len(e.messages) == 0 {
		return nil
	}
	ret := e.messages
	e.messages = nil
	return ret
}

// EncodeCheckpointEvent returns nil: outbox-json topics carry only INSERT
// payloads, so watermark messages are not emitted.
func (e *Encoder) EncodeCheckpointEvent(_ uint64) (*codecCommon.Message, error) {
	return nil, nil
}

// EncodeDDLEvent returns nil: outbox-json topics carry only INSERT payloads,
// so DDL events are not emitted.
func (e *Encoder) EncodeDDLEvent(_ *commonEvent.DDLEvent) (*codecCommon.Message, error) {
	return nil, nil
}

// Clean resets the encoder's buffered messages.
func (e *Encoder) Clean() {}

// getRequiredColumnValue returns the encoded value for a required outbox column
// and rejects missing, null, or empty values.
func getRequiredColumnValue(event *commonEvent.RowEvent, columnName string) ([]byte, error) {
	colOffset, ok := event.TableInfo.GetColumnOffsetByName(columnName)
	if !ok {
		return nil, errors.ErrCodecInvalidConfig.GenWithStack(
			"outbox required column not found, table: %s, column: %s",
			event.TableInfo.TableName.String(), columnName)
	}
	columnInfo := event.TableInfo.GetColumns()[colOffset]
	value := commonpkg.ExtractColVal(event.GetRows(), columnInfo, colOffset)
	if value == nil {
		return nil, errors.ErrCodecInvalidConfig.GenWithStack(
			"outbox required column is null, table: %s, column: %s",
			event.TableInfo.TableName.String(), columnName)
	}

	encoded := normalizeColumnValue(value)
	if len(encoded) == 0 {
		return nil, errors.ErrCodecInvalidConfig.GenWithStack(
			"outbox required column is empty, table: %s, column: %s",
			event.TableInfo.TableName.String(), columnName)
	}
	return encoded, nil
}

// normalizeColumnValue converts a column value into the byte form used by the
// outbox key, value, and headers.
func normalizeColumnValue(value interface{}) []byte {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(v))
	}
}
