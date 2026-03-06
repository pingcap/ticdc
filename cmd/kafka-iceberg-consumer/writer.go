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
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

type writer struct {
	icebergSink sink.Sink
	topic       string
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		topic: o.topic,
	}

	icebergSinkURI := fmt.Sprintf("iceberg://%s", o.icebergWarehouse)
	if !strings.HasPrefix(o.icebergWarehouse, "s3://") && !strings.HasPrefix(o.icebergWarehouse, "file://") {
		icebergSinkURI = fmt.Sprintf("iceberg://%s?warehouse=%s", o.icebergWarehouse, o.icebergWarehouse)
	}

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-iceberg-consumer", commonType.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      icebergSinkURI,
		SinkConfig:   o.sinkConfig,
	}

	icebergSink, err := sink.New(ctx, cfg, changefeedID)
	if err != nil {
		log.Panic("cannot create the iceberg sink", zap.Error(err))
	}
	w.icebergSink = icebergSink
	log.Info("iceberg sink created", zap.String("sinkURI", icebergSinkURI))
	return w
}

func (w *writer) run(ctx context.Context) error {
	return w.icebergSink.Run(ctx)
}

func (w *writer) WriteMessage(ctx context.Context, message *kafka.Message) bool {
	log.Debug("received message from kafka",
		zap.String("topic", *message.TopicPartition.Topic),
		zap.Int32("partition", message.TopicPartition.Partition),
		zap.Int64("offset", int64(message.TopicPartition.Offset)),
		zap.ByteString("key", message.Key),
		zap.ByteString("value", message.Value))

	return true
}
