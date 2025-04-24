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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// JSONTxnEventEncoder encodes txn event in JSON format
type JSONTxnEventEncoder struct {
	config *common.Config

	// the symbol separating two lines
	terminator []byte
	valueBuf   *bytes.Buffer
	batchSize  int
	callback   func()

	// Store some fields of the txn event.
	txnCommitTs uint64
	txnSchema   *string
	txnTable    *string

	columnSelector columnselector.Selector
}

// NewJSONTxnEventEncoder creates a new JSONTxnEventEncoder
func NewJSONTxnEventEncoder(config *common.Config) common.TxnEventEncoder {
	return &JSONTxnEventEncoder{
		valueBuf:       &bytes.Buffer{},
		terminator:     []byte(config.Terminator),
		columnSelector: columnselector.NewDefaultColumnSelector(),
		config:         config,
	}
}

// AppendTxnEvent appends a txn event to the encoder.
func (j *JSONTxnEventEncoder) AppendTxnEvent(event *commonEvent.DMLEvent) error {
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}
		value, err := newJSONMessageForDML(&commonEvent.RowEvent{
			TableInfo:      event.TableInfo,
			CommitTs:       event.CommitTs,
			Event:          row,
			ColumnSelector: j.columnSelector,
		}, j.config, false, "")
		if err != nil {
			return err
		}
		length := len(value) + common.MaxRecordOverhead
		// For single message that is longer than max-message-bytes, do not send it.
		if length > j.config.MaxMessageBytes {
			log.Warn("Single message is too large for canal-json",
				zap.Int("maxMessageBytes", j.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", event.TableInfo.TableName))
			return errors.ErrMessageTooLarge.GenWithStackByArgs()
		}
		j.valueBuf.Write(value)
		j.valueBuf.Write(j.terminator)
		j.batchSize++
	}
	j.callback = event.PostFlush
	j.txnCommitTs = event.CommitTs
	j.txnSchema = event.TableInfo.GetSchemaNamePtr()
	j.txnTable = event.TableInfo.GetTableNamePtr()
	return nil
}

// Build builds a message from the encoder and resets the encoder.
func (j *JSONTxnEventEncoder) Build() []*common.Message {
	if j.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(nil, j.valueBuf.Bytes())
	ret.SetRowsCount(j.batchSize)
	ret.Callback = j.callback
	if j.valueBuf.Cap() > common.MemBufShrinkThreshold {
		j.valueBuf = &bytes.Buffer{}
	} else {
		j.valueBuf.Reset()
	}
	j.callback = nil
	j.batchSize = 0
	j.txnCommitTs = 0
	j.txnSchema = nil
	j.txnTable = nil

	return []*common.Message{ret}
}
