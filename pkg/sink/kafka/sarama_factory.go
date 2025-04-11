// Copyright 2023 PingCAP, Inc.
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
	changefeedID common.ChangeFeedID
	config       *sarama.Config
	endpoints    []string
}

// NewSaramaFactory constructs a Factory with sarama implementation.
func NewSaramaFactory(
	ctx context.Context,
	o *Options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	start := time.Now()
	saramaConfig, err := NewSaramaConfig(ctx, o)
	duration := time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama config cost too much time",
			zap.Any("duration", duration), zap.Stringer("changefeedID", changefeedID))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	saramaConfig.MetricRegistry = metrics.NewRegistry()

	return &saramaFactory{
		changefeedID: changefeedID,
		endpoints:    o.BrokerEndpoints,
		config:       saramaConfig,
	}, nil
}

func (f *saramaFactory) AdminClient() (ClusterAdminClient, error) {
	start := time.Now()
	client, err := sarama.NewClient(f.endpoints, f.config)
	duration := time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama client cost too much time",
			zap.Any("duration", duration), zap.Stringer("changefeedID", f.changefeedID))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	start = time.Now()
	admin, err := sarama.NewClusterAdminFromClient(client)
	duration = time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama cluster admin cost too much time",
			zap.Any("duration", duration), zap.Stringer("changefeedID", f.changefeedID))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &saramaAdminClient{
		client:     client,
		admin:      admin,
		changefeed: f.changefeedID,
	}, nil
}

// SyncProducer returns a Sync SyncProducer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) SyncProducer() (SyncProducer, error) {
	p, err := sarama.NewSyncProducer(f.endpoints, f.config)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	return &saramaSyncProducer{
		id:       f.changefeedID,
		producer: p,
		closed:   false,
	}, nil
}

// AsyncProducer return an Async SyncProducer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) AsyncProducer() (AsyncProducer, error) {
	client, err := sarama.NewClient(f.endpoints, f.config)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	p, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return &saramaAsyncProducer{
		client:       client,
		producer:     p,
		changefeedID: f.changefeedID,
		closed:       atomic.NewBool(false),
		failpointCh:  make(chan error, 1),
	}, nil
}

func (f *saramaFactory) MetricsCollector(
	adminClient ClusterAdminClient,
) MetricsCollector {
	return &saramaMetricsCollector{
		changefeedID: f.changefeedID,
		adminClient:  adminClient,
		brokers:      make(map[int32]struct{}),
		registry:     f.config.MetricRegistry,
	}
}
