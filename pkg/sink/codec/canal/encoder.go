// Copyright 2022 PingCAP, Inc.
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

package canal

import (
	"context"
	"time"

	"github.com/goccy/go-json"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/internal"
	"github.com/pingcap/ticdc/pkg/sink/kafka/claimcheck"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

var bytesDecoder = charmap.ISO8859_1.NewDecoder()

// TODO: we need to reorg this code later, including use util.jsonWriter and other unreasonable code
func fillColumns(
	valueMap map[int64]string,
	tableInfo *commonType.TableInfo,
	onlyHandleKeyColumn bool,
	out *jwriter.Writer,
	columnSelector columnselector.Selector,
) error {
	if len(tableInfo.GetColumns()) == 0 {
		out.RawString("null")
		return nil
	}
	out.RawByte('[')
	out.RawByte('{')
	isFirst := true
	for _, col := range tableInfo.GetColumns() {
		if !columnSelector.Select(col) {
			continue
		}
		if col != nil {
			colID := col.ID
			if onlyHandleKeyColumn && !tableInfo.GetColumnFlags()[colID].IsHandleKey() {
				continue
			}
			if isFirst {
				isFirst = false
			} else {
				out.RawByte(',')
			}
			out.String(col.Name.O)
			out.RawByte(':')
			if valueMap[colID] == "null" {
				out.RawString("null")
			} else {
				out.String(valueMap[colID])
			}
		}
	}
	out.RawByte('}')
	out.RawByte(']')
	return nil
}

func fillUpdateColumns(
	newValueMap map[int64]string,
	oldValueMap map[int64]string,
	tableInfo *commonType.TableInfo,
	onlyHandleKeyColumn bool,
	onlyOutputUpdatedColumn bool,
	out *jwriter.Writer,
) error {
	if len(tableInfo.GetColumns()) == 0 {
		out.RawString("null")
		return nil
	}
	out.RawByte('[')
	out.RawByte('{')
	isFirst := true
	for _, col := range tableInfo.GetColumns() {
		if col != nil {
			colID := col.ID
			// column equal, do not output it
			if onlyOutputUpdatedColumn && newValueMap[colID] == oldValueMap[colID] {
				continue
			}
			if onlyHandleKeyColumn && !tableInfo.GetColumnFlags()[colID].IsHandleKey() {
				continue
			}
			if isFirst {
				isFirst = false
			} else {
				out.RawByte(',')
			}
			out.String(col.Name.O)
			out.RawByte(':')
			if oldValueMap[colID] == "null" {
				out.RawString("null")
			} else {
				out.String(oldValueMap[colID])
			}
		}
	}
	out.RawByte('}')
	out.RawByte(']')
	return nil
}

func newJSONMessageForDML(
	e *commonEvent.RowEvent,
	config *common.Config,
	messageTooLarge bool,
	claimCheckFileName string,
) ([]byte, error) {
	isDelete := e.IsDelete()

	onlyHandleKey := messageTooLarge
	if isDelete && config.DeleteOnlyHandleKeyColumns {
		onlyHandleKey = true
	}

	columnLen := 0
	for _, col := range e.TableInfo.GetColumns() {
		if e.ColumnSelector.Select(col) {
			columnLen += 1
		}
	}
	if columnLen == 0 {
		return nil, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found invlaid columns for the event")
	}

	mysqlTypeMap := make(map[string]string, columnLen)
	// TODO: use util.JsonWriter
	out := &jwriter.Writer{}
	out.RawByte('{')
	{
		const prefix string = ",\"id\":"
		out.RawString(prefix[1:])
		out.Int64(0) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"database\":"
		out.RawString(prefix)
		out.String(e.TableInfo.GetSchemaName())
	}
	{
		const prefix string = ",\"table\":"
		out.RawString(prefix)
		out.String(e.TableInfo.GetTableName())
	}
	{
		const prefix string = ",\"pkNames\":"
		out.RawString(prefix)
		pkNames := e.PrimaryKeyColumnNames()
		if pkNames == nil {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v25, v26 := range pkNames {
				if v25 > 0 {
					out.RawByte(',')
				}
				out.String(v26)
			}
			out.RawByte(']')
		}
	}
	{
		const prefix string = ",\"isDdl\":"
		out.RawString(prefix)
		out.Bool(false)
	}
	{
		const prefix string = ",\"type\":"
		out.RawString(prefix)
		out.String(eventTypeString(e))
	}
	{
		const prefix string = ",\"es\":"
		out.RawString(prefix)
		out.Int64(convertToCanalTs(e.CommitTs))
	}
	{
		const prefix string = ",\"ts\":"
		out.RawString(prefix)
		out.Int64(time.Now().UnixMilli()) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"sql\":"
		out.RawString(prefix)
		out.String("")
	}

	valueMap := make(map[int64]string, 0)                  // colId -> value
	javaTypeMap := make(map[int64]internal.JavaSQLType, 0) // colId -> javaType

	row := e.GetRows()
	if e.IsDelete() {
		row = e.GetPreRows()
	}
	for idx, col := range e.TableInfo.GetColumns() {
		if !e.ColumnSelector.Select(col) {
			continue
		}
		flag := e.TableInfo.GetColumnFlags()[col.ID]
		value, javaType := formatColumnValue(row, idx, col, flag)
		valueMap[col.ID] = value
		javaTypeMap[col.ID] = javaType
	}
	{
		const prefix string = ",\"sqlType\":"
		out.RawString(prefix)
		emptyColumn := true
		tableInfo := e.TableInfo
		columnInfos := tableInfo.GetColumns()
		for _, col := range columnInfos {
			if col != nil && e.ColumnSelector.Select(col) {
				colID := col.ID
				colFlag := tableInfo.GetColumnFlags()[colID]
				colName := col.Name.O
				if onlyHandleKey && !colFlag.IsHandleKey() {
					continue
				}
				if emptyColumn {
					out.RawByte('{')
					emptyColumn = false
				} else {
					out.RawByte(',')
				}

				out.String(colName)
				out.RawByte(':')
				out.Int32(int32(javaTypeMap[colID]))
				mysqlTypeMap[colName] = utils.GetMySQLType(col, config.ContentCompatible)
			}
		}
		if emptyColumn {
			out.RawString(`null`)
		} else {
			out.RawByte('}')
		}
	}
	{
		const prefix string = ",\"mysqlType\":"
		out.RawString(prefix)
		if mysqlTypeMap == nil {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			isFirst := true
			for typeKey, typeValue := range mysqlTypeMap {
				if isFirst {
					isFirst = false
				} else {
					out.RawByte(',')
				}
				out.String(typeKey)
				out.RawByte(':')
				out.String(typeValue)
			}
			out.RawByte('}')
		}
	}

	if e.IsDelete() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(valueMap, e.TableInfo, onlyHandleKey, out, e.ColumnSelector); err != nil {
			return nil, err
		}
	} else if e.IsInsert() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(valueMap, e.TableInfo, onlyHandleKey, out, e.ColumnSelector); err != nil {
			return nil, err
		}
	} else if e.IsUpdate() {
		out.RawString(",\"old\":")

		oldValueMap := make(map[int64]string, 0) // colId -> value
		preRow := e.GetPreRows()
		for idx, col := range e.TableInfo.GetColumns() {
			if !e.ColumnSelector.Select(col) {
				continue
			}
			flag := e.TableInfo.GetColumnFlags()[col.ID]
			value, _ := formatColumnValue(preRow, idx, col, flag)
			oldValueMap[col.ID] = value
		}

		if err := fillUpdateColumns(valueMap, oldValueMap, e.TableInfo, onlyHandleKey,
			config.OnlyOutputUpdatedColumns, out); err != nil {
			return nil, err
		}
		out.RawString(",\"data\":")
		if err := fillColumns(valueMap, e.TableInfo, onlyHandleKey, out, e.ColumnSelector); err != nil {
			return nil, err
		}
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if config.EnableTiDBExtension {
		const prefix string = ",\"_tidb\":"
		out.RawString(prefix)
		out.RawByte('{')
		out.RawString("\"commitTs\":")
		out.Uint64(e.CommitTs)

		// only send handle key may happen in 2 cases:
		// 1. delete event, and set only handle key config. no need to encode `onlyHandleKey` field
		// 2. event larger than the max message size, and enable large message handle to the `handleKeyOnly`, encode `onlyHandleKey` field
		if messageTooLarge {
			if config.LargeMessageHandle.HandleKeyOnly() {
				out.RawByte(',')
				out.RawString("\"onlyHandleKey\":true")
			}
			if config.LargeMessageHandle.EnableClaimCheck() {
				out.RawByte(',')
				out.RawString("\"claimCheckLocation\":")
				out.String(claimCheckFileName)
			}
		}
		out.RawByte('}')
	}
	out.RawByte('}')

	value, err := out.BuildBytes()
	if err != nil {
		return nil, errors.WrapError(errors.ErrCanalEncodeFailed, err)
	}
	return value, nil
}

func eventTypeString(e *commonEvent.RowEvent) string {
	if e.IsDelete() {
		return "DELETE"
	}
	if e.IsInsert() {
		return "INSERT"
	}
	return "UPDATE"
}

// JSONRowEventEncoder encodes row event in JSON format
type JSONRowEventEncoder struct {
	messages     []*common.Message
	bytesDecoder *encoding.Decoder

	claimCheck *claimcheck.ClaimCheck

	config *common.Config
}

// NewJSONRowEventEncoder creates a new JSONRowEventEncoder
func NewJSONRowEventEncoder(ctx context.Context, config *common.Config) (common.EventEncoder, error) {
	claimCheck, err := claimcheck.New(ctx, config.LargeMessageHandle, config.ChangefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &JSONRowEventEncoder{
		messages:     make([]*common.Message, 0, 1),
		bytesDecoder: charmap.ISO8859_1.NewDecoder(),
		config:       config,
		claimCheck:   claimCheck,
	}, nil
}

func (c *JSONRowEventEncoder) newJSONMessageForDDL(e *commonEvent.DDLEvent) canalJSONMessageInterface {
	msg := &JSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        e.GetSchemaName(),
		Table:         e.GetTableName(),
		IsDDL:         true,
		EventType:     convertDdlEventType(e.Type).String(),
		ExecutionTime: convertToCanalTs(e.GetCommitTs()),
		BuildTime:     time.Now().UnixMilli(), // timestamp
		Query:         e.Query,
	}

	if !c.config.EnableTiDBExtension {
		return msg
	}

	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: msg,
		Extensions:  &tidbExtension{CommitTs: e.GetCommitTs()},
	}
}

func (c *JSONRowEventEncoder) newJSONMessage4CheckpointEvent(
	ts uint64,
) *canalJSONMessageWithTiDBExtension {
	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			ID:            0,
			IsDDL:         false,
			EventType:     tidbWaterMarkType,
			ExecutionTime: convertToCanalTs(ts),
			BuildTime:     time.Now().UnixNano() / int64(time.Millisecond), // converts to milliseconds
		},
		Extensions: &tidbExtension{WatermarkTs: ts},
	}
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if !c.config.EnableTiDBExtension {
		return nil, nil
	}

	msg := c.newJSONMessage4CheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCanalEncodeFailed, err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return common.NewMsg(nil, value), nil
}

// AppendRowChangedEvent implements the interface EventJSONBatchEncoder
func (c *JSONRowEventEncoder) AppendRowChangedEvent(
	ctx context.Context,
	_ string,
	e *commonEvent.RowEvent,
) error {
	value, err := newJSONMessageForDML(e, c.config, false, "")
	if err != nil {
		return errors.Trace(err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return errors.Trace(err)
	}

	m := common.NewMsg(nil, value)
	m.Callback = e.Callback
	m.IncRowsCount()

	originLength := m.Length()
	if m.Length() > c.config.MaxMessageBytes {
		// for single message that is longer than max-message-bytes, do not send it.
		if c.config.LargeMessageHandle.Disabled() {
			log.Error("Single message is too large for canal-json",
				zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
				zap.Int("length", originLength),
				zap.Any("table", e.TableInfo.TableName))
			return errors.ErrMessageTooLarge.GenWithStackByArgs()
		}

		if c.config.LargeMessageHandle.HandleKeyOnly() {
			value, err = newJSONMessageForDML(e, c.config, true, "")
			if err != nil {
				return errors.ErrMessageTooLarge.GenWithStackByArgs()
			}
			value, err = common.Compress(
				c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
			)
			if err != nil {
				return errors.Trace(err)
			}

			m.Value = value
			length := m.Length()
			if length > c.config.MaxMessageBytes {
				log.Error("Single message is still too large for canal-json only encode handle-key columns",
					zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
					zap.Int("originLength", originLength),
					zap.Int("length", length),
					zap.Any("table", e.TableInfo.TableName))
				return errors.ErrMessageTooLarge.GenWithStackByArgs()
			}
			log.Warn("Single message is too large for canal-json, only encode handle-key columns",
				zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
				zap.Int("originLength", originLength),
				zap.Int("length", length),
				zap.Any("table", e.TableInfo.TableName))
		}

		if c.config.LargeMessageHandle.EnableClaimCheck() {
			claimCheckFileName := claimcheck.NewFileName()
			if err := c.claimCheck.WriteMessage(ctx, m.Key, m.Value, claimCheckFileName); err != nil {
				return errors.Trace(err)
			}

			m, err = c.newClaimCheckLocationMessage(e, claimCheckFileName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	c.messages = append(c.messages, m)
	return nil
}

func (c *JSONRowEventEncoder) newClaimCheckLocationMessage(
	event *commonEvent.RowEvent, fileName string,
) (*common.Message, error) {
	claimCheckLocation := c.claimCheck.FileNameWithPrefix(fileName)
	value, err := newJSONMessageForDML(event, c.config, true, claimCheckLocation)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCanalEncodeFailed, err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := common.NewMsg(nil, value)
	result.Callback = event.Callback
	result.IncRowsCount()

	length := result.Length()
	if length > c.config.MaxMessageBytes {
		log.Warn("Single message is too large for canal-json, when create the claim check location message",
			zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", event.TableInfo.TableName))
		return nil, errors.ErrMessageTooLarge.GenWithStackByArgs(length)
	}
	return result, nil
}

// Build implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) Build() []*common.Message {
	if len(c.messages) == 0 {
		return nil
	}

	result := c.messages
	c.messages = nil
	return result
}

// EncodeDDLEvent encodes DDL events
func (c *JSONRowEventEncoder) EncodeDDLEvent(e *commonEvent.DDLEvent) (*common.Message, error) {
	message := c.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCanalEncodeFailed, err)
	}
	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return common.NewMsg(nil, value), nil
}

func (b *JSONRowEventEncoder) Clean() {
	if b.claimCheck != nil {
		b.claimCheck.CleanMetrics()
	}
}
