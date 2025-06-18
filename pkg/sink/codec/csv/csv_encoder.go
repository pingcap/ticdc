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

package csv

import (
	"bytes"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// batchEncoder encodes the events into the byte of a batch into.
type batchEncoder struct {
	header    []byte
	valueBuf  *bytes.Buffer
	callback  func()
	batchSize int
	config    *common.Config
}

// NewTxnEventEncoder creates a new csv BatchEncoder.
func NewTxnEventEncoder(config *common.Config) common.TxnEventEncoder {
	return &batchEncoder{
		config:   config,
		valueBuf: &bytes.Buffer{},
	}
}

// AppendTxnEvent implements the TxnEventEncoder interface
func (b *batchEncoder) AppendTxnEvent(event *commonEvent.DMLEvent) error {
	if b.config.CSVOutputFieldHeader && b.batchSize == 0 {
		b.setHeader(event.TableInfo)
	}
	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}
		msg, err := rowChangedEvent2CSVMsg(b.config, &commonEvent.RowEvent{
			TableInfo: event.TableInfo,
			CommitTs:  event.CommitTs,
			Event:     row,
		})
		if err != nil {
			return err
		}
		b.valueBuf.Write(msg.encode())
		b.batchSize++
	}
	b.callback = event.PostFlush
	return nil
}

// Build implements the RowEventEncoder interface
func (b *batchEncoder) Build() (messages []*common.Message) {
	if b.batchSize == 0 {
		return nil
	}
	ret := common.NewMsg(b.header, b.valueBuf.Bytes())
	ret.SetRowsCount(b.batchSize)
	ret.Callback = b.callback
	if b.valueBuf.Cap() > common.MemBufShrinkThreshold {
		b.valueBuf = &bytes.Buffer{}
	} else {
		b.valueBuf.Reset()
	}
	b.callback = nil
	b.batchSize = 0
	b.header = nil

	return []*common.Message{ret}
}

func (b *batchEncoder) setHeader(tableInfo *commonType.TableInfo) {
	buf := &bytes.Buffer{}
	colNames := make([]string, 0, len(tableInfo.GetColumns()))
	for _, col := range tableInfo.GetColumns() {
		colNames = append(colNames, col.Name.O)
	}
	buf.Write(encodeHeader(b.config, colNames))
	b.header = buf.Bytes()
}
