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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
)

type canalJSONTxnEventDecoder struct {
	data []byte

	config *common.Config
	msg    canalJSONMessageInterface
}

// NewCanalJSONTxnEventDecoder return a new CanalJSONTxnEventDecoder.
func NewCanalJSONTxnEventDecoder(
	codecConfig *common.Config,
) *canalJSONTxnEventDecoder {
	return &canalJSONTxnEventDecoder{
		config: codecConfig,
	}
}

// AddKeyValue set the key value to the decoder
func (d *canalJSONTxnEventDecoder) AddKeyValue(_, value []byte) error {
	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", d.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Error(err))

		return errors.Trace(err)
	}
	d.data = value
	return nil
}

// HasNext return true if there is any event can be returned.
func (d *canalJSONTxnEventDecoder) HasNext() (common.MessageType, bool, error) {
	if d.data == nil {
		return common.MessageTypeUnknown, false, nil
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
		return common.MessageTypeUnknown, false, nil
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		return common.MessageTypeUnknown, false, err
	}
	d.msg = msg
	return d.msg.messageType(), true, nil
}

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (d *canalJSONTxnEventDecoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if d.msg == nil || d.msg.messageType() != common.MessageTypeRow {
		return nil, errors.ErrCanalEncodeFailed.
			GenWithStack("not found row changed event message")
	}
	result := d.canalJSONMessage2RowChange()
	d.msg = nil
	return result, nil
}

func (d *canalJSONTxnEventDecoder) canalJSONMessage2RowChange() *commonEvent.DMLEvent {
	msg := d.msg

	tableInfo := newTableInfo(msg, 0)
	result := new(commonEvent.DMLEvent)
	result.Length++                    // todo: set this field correctly
	result.StartTs = msg.getCommitTs() // todo: how to set this correctly?
	result.ApproximateSize = 0
	result.TableInfo = tableInfo
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	result.CommitTs = msg.getCommitTs()

	columns := tableInfo.GetColumns()
	switch msg.eventType() {
	case canal.EventType_DELETE:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
		result.Length += 1
	case canal.EventType_INSERT:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
		result.Length += 1
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
		result.Length += 1
	default:
		log.Panic("unknown event type for the DML event", zap.Any("eventType", msg.eventType()))
	}
	result.Rows = chk
	// todo: may fix this later.
	result.PhysicalTableID = result.TableInfo.TableName.TableID
	return result
}

// NextResolvedEvent implements the RowEventDecoder interface
func (d *canalJSONTxnEventDecoder) NextResolvedEvent() (uint64, error) {
	return 0, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (d *canalJSONTxnEventDecoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	return nil, nil
}
