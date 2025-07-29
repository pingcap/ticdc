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

package canal

import (
	"bytes"
	"encoding/json"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

type txnDecoder struct {
	data []byte

	config *common.Config
	msg    canalJSONMessageInterface
}

// NewTxnDecoder return a new CanalJSONTxnEventDecoder.
func NewTxnDecoder(
	codecConfig *common.Config,
) *txnDecoder {
	return &txnDecoder{
		config: codecConfig,
	}
}

// AddKeyValue set the key value to the decoder
func (d *txnDecoder) AddKeyValue(_, value []byte) {
	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress data failed",
			zap.String("compression", d.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", value),
			zap.Error(err))
	}
	d.data = value
}

// HasNext return true if there is any event can be returned.
func (d *txnDecoder) HasNext() (common.MessageType, bool) {
	if d.data == nil {
		return common.MessageTypeUnknown, false
	}
	var (
		msg         canalJSONMessageInterface = &JSONMessage{}
		encodedData []byte
	)

	if d.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}

	idx := bytes.IndexAny(d.data, d.config.Terminator)
	if idx >= 0 {
		encodedData = d.data[:idx]
		d.data = d.data[idx+len(d.config.Terminator):]
	} else {
		encodedData = d.data
		d.data = nil
	}

	if len(encodedData) == 0 {
		return common.MessageTypeUnknown, false
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Panic("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		return common.MessageTypeUnknown, false
	}
	d.msg = msg
	return d.msg.messageType(), true
}

func (d *txnDecoder) NextDMLEvent() *commonEvent.DMLEvent {
	if d.msg == nil || d.msg.messageType() != common.MessageTypeRow {
		log.Panic("message type is not row changed",
			zap.Any("messageType", d.msg.messageType()), zap.Any("msg", d.msg))
	}
	result := d.canalJSONMessage2RowChange()
	d.msg = nil
	return result
}

func (d *txnDecoder) canalJSONMessage2RowChange() *commonEvent.DMLEvent {
	msg := d.msg

	tableInfo := newTableInfo(msg)
	result := new(commonEvent.DMLEvent)
	result.Length++                    // todo: set this field correctly
	result.StartTs = msg.getCommitTs() // todo: how to set this correctly?
	result.TableInfo = tableInfo
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	result.CommitTs = msg.getCommitTs()

	columns := tableInfo.GetColumns()
	switch msg.eventType() {
	case canal.EventType_DELETE:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
	case canal.EventType_INSERT:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
	case canal.EventType_UPDATE:
		previous := formatAllColumnsValue(msg.getOld(), columns)
		data := formatAllColumnsValue(msg.getData(), columns)
		for k, v := range data {
			if _, ok := previous[k]; !ok {
				previous[k] = v
			}
		}
		common.AppendRow2Chunk(previous, columns, chk)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
	default:
		log.Panic("unknown event type for the DML event", zap.Any("eventType", msg.eventType()))
	}
	result.Rows = chk
	// todo: may fix this later.
	result.PhysicalTableID = result.TableInfo.TableName.TableID
	return result
}

// NextResolvedEvent implements the Decoder interface
func (d *txnDecoder) NextResolvedEvent() uint64 {
	return 0
}

// NextDDLEvent implements the Decoder interface
func (d *txnDecoder) NextDDLEvent() *commonEvent.DDLEvent {
	return nil
}
