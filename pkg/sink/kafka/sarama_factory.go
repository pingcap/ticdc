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
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type saramaFactory struct {
	changefeedID   common.ChangeFeedID
	option         *options
	metricRegistry metrics.Registry
	client         sarama.Client
}

// NewSaramaFactory constructs a Factory with sarama implementation.
func NewSaramaFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	start := time.Now()
	config, err := newSaramaConfig(ctx, o)
	duration := time.Since(start)
	if duration > 2*time.Second {
		log.Warn("kafka configuration initialization is slow",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", duration))
	}
	if err != nil {
		return nil, err
	}

	admin, err := newAdminClient(changefeedID, o.BrokerEndpoints, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		admin.Close()
	}()

	if err = adjustOptions(changefeedID, admin, o, o.Topic); err != nil {
		return nil, err
	}
	log.Info("kafka sink configuration resolved",
		zap.String("namespace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
		zap.String("topic", o.Topic),
		zap.Int32("partitionNum", o.PartitionNum),
		zap.Int("maxMessageBytes", o.MaxMessageBytes),
		zap.Int("maxBatchedBytes", o.MaxBatchedBytes),
		zap.String("compression", config.Producer.Compression.String()),
		zap.Int16("requiredAcks", int16(o.RequiredAcks)),
		zap.Int("maxRetry", o.MaxRetry),
		zap.Duration("dialTimeout", o.DialTimeout),
		zap.Duration("readTimeout", o.ReadTimeout),
		zap.Duration("writeTimeout", o.WriteTimeout))

	return &saramaFactory{
		changefeedID:   changefeedID,
		option:         o,
		metricRegistry: metrics.NewRegistry(),
	}, nil
}

func newAdminClient(changefeedID common.ChangeFeedID, endpoints []string, config *sarama.Config) (*saramaAdminClient, error) {
	start := time.Now()
	client, err := sarama.NewClient(endpoints, config)
	duration := time.Since(start)
	if duration > 2*time.Second {
		log.Warn("kafka client initialization is slow",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", duration))
	}
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	start = time.Now()
	admin, err := sarama.NewClusterAdminFromClient(client)
	duration = time.Since(start)
	if duration > 2*time.Second {
		log.Warn("kafka admin client initialization is slow",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", duration))
	}
	if err != nil {
		// No admin exists to close the client when construction fails.
		_ = client.Close()
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return &saramaAdminClient{
		client:     client,
		admin:      admin,
		changefeed: changefeedID,
	}, nil
}

func (f *saramaFactory) AdminClient(ctx context.Context) (AdminClient, error) {
	config, err := newSaramaConfig(ctx, f.option)
	if err != nil {
		return nil, err
	}
	config.MetricRegistry = f.metricRegistry

	admin, err := newAdminClient(f.changefeedID, f.option.BrokerEndpoints, config)
	if err != nil {
		return nil, err
	}
	f.client = admin.client
	return admin, nil
}

// SyncProducer returns a Sync SyncProducer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) SyncProducer(context.Context) (SyncProducer, error) {
	p, err := sarama.NewSyncProducerFromClient(f.client)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	return &saramaSyncProducer{
		id:       f.changefeedID,
		producer: p,
		closed:   atomic.NewBool(false),
	}, nil
}

// AsyncProducer return an Async SyncProducer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) AsyncProducer(context.Context) (AsyncProducer, error) {
	p, err := sarama.NewAsyncProducerFromClient(f.client)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return &saramaAsyncProducer{
		producer:     p,
		changefeedID: f.changefeedID,
		closed:       atomic.NewBool(false),
	}, nil
}

func (f *saramaFactory) MetricsCollector(
	adminClient AdminClient,
) MetricsCollector {
	return &saramaMetricsCollector{
		changefeedID: f.changefeedID,
		adminClient:  adminClient,
		brokers:      make(map[int32]struct{}),
		registry:     f.metricRegistry,
	}
}
