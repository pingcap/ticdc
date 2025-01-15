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

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/errors"
)

// BatchEncoder encodes message into Debezium format.
type BatchEncoder struct {
	messages []*common.Message

	config *common.Config
	codec  *dbzCodec
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	// Currently ignored. Debezium MySQL Connector does not emit such event.
	return nil, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *commonEvent.RowChangedEvent,
	callback func(),
) error {
	valueBuf := bytes.Buffer{}
	err := d.codec.EncodeRowChangedEvent(e, &valueBuf)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	value, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	if err != nil {
		return errors.Trace(err)
	}
	m := &common.Message{
		Key:      nil,
		Value:    value,
		Callback: callback,
	}
	m.IncRowsCount()

	d.messages = append(d.messages, m)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(e *commonEvent.DDLEvent) (*common.Message, error) {
	// Schema Change Events are currently not supported.
	return nil, nil
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
