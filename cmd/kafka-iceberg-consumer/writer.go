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

package main

import (
	"context"
	"fmt"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"go.uber.org/zap"
)

type writer struct {
	icebergSink sink.Sink
	topic       string
	codecConfig *codecCommon.Config

	decoderMu sync.Mutex
	decoders  map[int32]codecCommon.Decoder
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		topic:       o.topic,
		codecConfig: o.codecConfig,
		decoders:    make(map[int32]codecCommon.Decoder),
	}
	if w.codecConfig == nil {
		panicWithLog("codec config is not initialized")
	}

	icebergSinkURI := fmt.Sprintf("iceberg://?warehouse=%s", neturl.QueryEscape(o.icebergWarehouse))
	if strings.HasPrefix(o.icebergWarehouse, "s3://") || strings.HasPrefix(o.icebergWarehouse, "file://") {
		icebergSinkURI = fmt.Sprintf("iceberg://%s", o.icebergWarehouse)
	}

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-iceberg-consumer", commonType.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      icebergSinkURI,
		SinkConfig:   o.sinkConfig,
	}

	if err := sink.Verify(ctx, cfg, changefeedID); err != nil {
		panicWithLog("iceberg sink config is invalid", zap.Error(err))
	}

	icebergSink, err := sink.New(ctx, cfg, changefeedID)
	if err != nil {
		panicWithLog("cannot create the iceberg sink", zap.Error(err))
	}
	w.icebergSink = icebergSink
	log.Info("iceberg sink created", zap.String("sinkURI", icebergSinkURI))
	return w
}

func (w *writer) run(ctx context.Context) error {
	return w.icebergSink.Run(ctx)
}

func (w *writer) getDecoder(ctx context.Context, partition int32) codecCommon.Decoder {
	w.decoderMu.Lock()
	defer w.decoderMu.Unlock()

	if dec, ok := w.decoders[partition]; ok {
		return dec
	}

	dec, err := codec.NewEventDecoder(ctx, int(partition), w.codecConfig, w.topic, nil)
	if err != nil {
		panicWithLog("create decoder failed",
			zap.Int32("partition", partition),
			zap.String("topic", w.topic),
			zap.Error(err))
	}
	w.decoders[partition] = dec
	return dec
}

func (w *writer) flushDMLEvents(ctx context.Context, topic string, partition int32, offset kafka.Offset, rows []*commonEvent.DMLEvent) bool {
	validRows := make([]*commonEvent.DMLEvent, 0, len(rows))
	for _, row := range rows {
		if row != nil {
			validRows = append(validRows, row)
		}
	}
	if len(validRows) == 0 {
		return true
	}

	total := int64(len(validRows))
	var flushed int64
	done := make(chan struct{}, 1)
	for _, row := range validRows {
		row.AddPostFlushFunc(func() {
			if atomic.AddInt64(&flushed, 1) == total {
				close(done)
			}
		})
		w.icebergSink.AddDMLEvent(row)
	}

	select {
	case <-done:
		return true
	case <-ctx.Done():
		log.Warn("dml events not flushed before context cancellation",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Any("offset", offset),
			zap.Int64("pending", total-flushed),
			zap.Error(context.Cause(ctx)))
		return false
	}
}

func (w *writer) WriteMessage(ctx context.Context, message *kafka.Message) bool {
	topic := w.topic
	if message.TopicPartition.Topic != nil {
		topic = *message.TopicPartition.Topic
	}
	partition := message.TopicPartition.Partition
	offset := message.TopicPartition.Offset
	decoder := w.getDecoder(ctx, partition)

	decoder.AddKeyValue(message.Key, message.Value)
	messageType, hasNext := decoder.HasNext()
	if !hasNext {
		panicWithLog("next event is missing",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Any("offset", offset))
	}

	switch messageType {
	case codecCommon.MessageTypeResolved:
		_ = decoder.NextResolvedEvent()
		return true
	case codecCommon.MessageTypeDDL:
		ddl := decoder.NextDDLEvent()
		if simpleDecoder, ok := decoder.(*simple.Decoder); ok {
			if !w.flushDMLEvents(ctx, topic, partition, offset, simpleDecoder.GetCachedEvents()) {
				return false
			}
		}

		// Bootstrap/table-info messages in simple protocol may have empty query.
		if ddl == nil || ddl.Query == "" {
			return true
		}
		if err := w.icebergSink.WriteBlockEvent(ddl); err != nil {
			log.Warn("write block event failed",
				zap.String("topic", topic),
				zap.Int32("partition", partition),
				zap.Any("offset", offset),
				zap.String("schema", ddl.GetSchemaName()),
				zap.String("table", ddl.GetTableName()),
				zap.Uint64("commitTs", ddl.GetCommitTs()),
				zap.Error(err))
			return false
		}
		return true
	case codecCommon.MessageTypeRow:
		rows := make([]*commonEvent.DMLEvent, 0, 1)
		row := decoder.NextDMLEvent()
		if row != nil {
			rows = append(rows, row)
		}
		for {
			_, hasNext = decoder.HasNext()
			if !hasNext {
				break
			}
			nextRow := decoder.NextDMLEvent()
			if nextRow != nil {
				rows = append(rows, nextRow)
			}
		}
		return w.flushDMLEvents(ctx, topic, partition, offset, rows)
	default:
		panicWithLog("unknown message type",
			zap.String("topic", topic),
			zap.Int32("partition", partition),
			zap.Any("offset", offset),
			zap.Any("messageType", messageType))
	}
	return false
}
