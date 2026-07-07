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

const (
	clientTypeAsyncProducer = "async_producer"
	clientTypeSyncProducer  = "sync_producer"
	clientTypeAdminClient   = "admin_client"
)

type factory struct {
	changefeedID common.ChangeFeedID
	option       *options

	asyncMetricsHook *metricsHook
	syncMetricsHook  *metricsHook
	adminMetricsHook *metricsHook
}

type kafkaMetricsCollector struct {
	changefeedID common.ChangeFeedID
	hooks        []*metricsHook
}

func (c *kafkaMetricsCollector) Run(ctx context.Context) {
	<-ctx.Done()
	for _, hook := range c.hooks {
		if hook != nil {
			hook.cleanupMetrics()
		}
	}
	cleanupAdminMetrics(c.changefeedID.Keyspace(), c.changefeedID.Name())
}

func newKafkaMetricsHook(changefeedID common.ChangeFeedID, clientType string) *metricsHook {
	return newMetricsHook(
		changefeedID.Keyspace(),
		changefeedID.Name(),
		clientType,
		metricVectors{
			RequestsInFlight:  kafkaClientRequestsInFlightGauge,
			OutgoingByteRate:  kafkaClientOutgoingByteTotalGauge,
			RequestRate:       kafkaClientRequestTotalGauge,
			RequestLatency:    kafkaClientRequestLatencyHistogram,
			ResponseRate:      kafkaClientResponseTotalGauge,
			CompressionRatio:  kafkaClientCompressionRatioHistogram,
			RecordsPerRequest: kafkaClientRecordsPerRequestHistogram,

			LegacyRequestsInFlight:  requestsInFlightGauge,
			LegacyOutgoingByteRate:  OutgoingByteRateGauge,
			LegacyRequestRate:       RequestRateGauge,
			LegacyRequestLatency:    RequestLatencyGauge,
			LegacyResponseRate:      responseRateGauge,
			LegacyCompressionRatio:  compressionRatioGauge,
			LegacyRecordsPerRequest: recordsPerRequestGauge,
		},
	)
}

// NewFactory constructs a Factory.
func NewFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	admin, err := newAdminClient(ctx, changefeedID, newKafkaOptions(o), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer admin.Close()

	if err := adjustOptions(ctx, admin, o, o.Topic); err != nil {
		return nil, errors.Trace(err)
	}

	return &factory{
		changefeedID:     changefeedID,
		option:           o,
		asyncMetricsHook: newKafkaMetricsHook(changefeedID, clientTypeAsyncProducer),
		syncMetricsHook:  newKafkaMetricsHook(changefeedID, clientTypeSyncProducer),
		adminMetricsHook: newKafkaMetricsHook(changefeedID, clientTypeAdminClient),
	}, nil
}

func (f *factory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	admin, err := newAdminClient(ctx, f.changefeedID, newKafkaOptions(f.option), f.adminMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return admin, nil
}

func (f *factory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	producer, err := newSyncProducer(ctx, f.changefeedID, newKafkaOptions(f.option), f.syncMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *factory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	producer, err := newAsyncProducer(ctx, f.changefeedID, newKafkaOptions(f.option), f.asyncMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *factory) MetricsCollector() MetricsCollector {
	return &kafkaMetricsCollector{changefeedID: f.changefeedID, hooks: []*metricsHook{
		f.asyncMetricsHook,
		f.syncMetricsHook,
		f.adminMetricsHook,
	}}
}

func newKafkaOptions(o *options) *clientOptions {
	if o == nil {
		return &clientOptions{
			RequiredAcks: int16(WaitForAll),
		}
	}
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
		SASL:               o.SASL,

		DialTimeout:  o.DialTimeout,
		WriteTimeout: o.WriteTimeout,
		ReadTimeout:  o.ReadTimeout,
	}
}
