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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
)

type factory struct {
	changefeedID common.ChangeFeedID
	clientOption *clientOptions

	metricsHook *metricsHook
}

type kafkaMetricsCollector struct {
	changefeedID common.ChangeFeedID
	hook         *metricsHook
}

func (c *kafkaMetricsCollector) Run(ctx context.Context) {
	<-ctx.Done()
	if c.hook != nil {
		c.hook.cleanupMetrics()
	}
	cleanupAdminMetrics(c.changefeedID.Keyspace(), c.changefeedID.Name())
}

func newKafkaMetricsHook(changefeedID common.ChangeFeedID) *metricsHook {
	return newMetricsHook(
		changefeedID.Keyspace(),
		changefeedID.Name(),
		metricVectors{
			RequestsInFlight:  requestsInFlightGauge,
			OutgoingByteRate:  OutgoingByteRateGauge,
			RequestRate:       RequestRateGauge,
			RequestLatency:    RequestLatencyGauge,
			ResponseRate:      responseRateGauge,
			CompressionRatio:  compressionRatioGauge,
			RecordsPerRequest: recordsPerRequestGauge,
		},
	)
}

// NewFactory constructs a Factory.
func NewFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	admin, err := newAdminClient(ctx, changefeedID, newClientOption(o), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer admin.Close()

	if err := adjustOptions(ctx, admin, o, o.Topic); err != nil {
		return nil, errors.Trace(err)
	}

	return &factory{
		changefeedID: changefeedID,
		clientOption: newClientOption(o),
		metricsHook:  newKafkaMetricsHook(changefeedID),
	}, nil
}

func (f *factory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	admin, err := newAdminClient(ctx, f.changefeedID, f.clientOption, f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return admin, nil
}

func (f *factory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	producer, err := newSyncProducer(ctx, f.changefeedID, f.clientOption, f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *factory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	producer, err := newAsyncProducer(ctx, f.changefeedID, f.clientOption, f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *factory) MetricsCollector() MetricsCollector {
	return &kafkaMetricsCollector{changefeedID: f.changefeedID, hook: f.metricsHook}
}

func newClientOption(o *options) *clientOptions {
	return &clientOptions{
		BrokerEndpoints: o.BrokerEndpoints,
		ClientID:        o.ClientID,

		Version:           o.Version,
		IsAssignedVersion: o.IsAssignedVersion,

		MaxMessageBytes:       o.MaxMessageBytes,
		ProducerBatchMaxBytes: o.ProducerBatchMaxBytes,
		MaxRetry:              o.MaxRetry,
		Compression:           o.Compression,
		RequiredAcks:          int16(o.RequiredAcks),

		EnableTLS:          o.EnableTLS,
		Credential:         o.Credential,
		InsecureSkipVerify: o.InsecureSkipVerify,
		sasl:               o.sasl,

		DialTimeout:  o.DialTimeout,
		WriteTimeout: o.WriteTimeout,
		ReadTimeout:  o.ReadTimeout,
	}
}
