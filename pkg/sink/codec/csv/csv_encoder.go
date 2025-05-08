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

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// batchEncoder encodes the events into the byte of a batch into.
type batchEncoder struct {
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
	ret := common.NewMsg(nil, b.valueBuf.Bytes())
	ret.SetRowsCount(b.batchSize)
	ret.Callback = b.callback
	if b.valueBuf.Cap() > common.MemBufShrinkThreshold {
		b.valueBuf = &bytes.Buffer{}
	} else {
		b.valueBuf.Reset()
	}
	b.callback = nil
	b.batchSize = 0

	return []*common.Message{ret}
}
