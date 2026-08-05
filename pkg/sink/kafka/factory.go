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

package kafka

import (
	"context"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// Factory is used to produce all Kafka components.
type Factory interface {
	// Admin returns a Kafka admin.
	Admin(ctx context.Context) (Admin, error)
	// SyncProducer creates a sync producer to write messages to Kafka.
	SyncProducer(ctx context.Context) (SyncProducer, error)
	// AsyncProducer creates an async producer to write messages to Kafka.
	AsyncProducer(ctx context.Context) (AsyncProducer, error)
}

type factory struct {
	changefeedID common.ChangeFeedID
	options      options
}

// NewFactory constructs a Factory.
func NewFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	admin, err := newAdmin(ctx, changefeedID, o, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	defer admin.Close()

	if err := adjustOptions(changefeedID, admin, o, o.Topic); err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	if compression == "" {
		compression = "none"
	}
	log.Info("kafka sink configuration resolved",
		zap.String("namespace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
		zap.String("topic", o.Topic),
		zap.Int32("partitionNum", o.PartitionNum),
		zap.Int("maxMessageBytes", o.MaxMessageBytes),
		zap.Int("maxBatchedBytes", o.MaxBatchedBytes),
		zap.String("compression", compression),
		zap.Int16("requiredAcks", int16(o.RequiredAcks)),
		zap.Int("maxRetry", o.MaxRetry),
		zap.Duration("dialTimeout", o.DialTimeout),
		zap.Duration("readTimeout", o.ReadTimeout),
		zap.Duration("writeTimeout", o.WriteTimeout))

	return &factory{
		changefeedID: changefeedID,
		options:      *o,
	}, nil
}

func (f *factory) Admin(ctx context.Context) (Admin, error) {
	admin, err := newAdmin(ctx, f.changefeedID, &f.options, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return admin, nil
}

func (f *factory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	hook := newKafkaMetricsHook(f.changefeedID)
	producer, err := newSyncProducer(ctx, f.changefeedID, &f.options, hook)
	if err != nil {
		CleanupMetrics(f.changefeedID)
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return producer, nil
}

func (f *factory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	hook := newKafkaMetricsHook(f.changefeedID)
	producer, err := newAsyncProducer(ctx, f.changefeedID, &f.options, hook)
	if err != nil {
		CleanupMetrics(f.changefeedID)
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return producer, nil
}
