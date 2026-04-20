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
	"github.com/pingcap/ticdc/pkg/sink/kafka/franz"
)

const (
	clientTypeAsyncProducer = "async_producer"
	clientTypeSyncProducer  = "sync_producer"
	clientTypeAdminClient   = "admin_client"
)

type franzFactory struct {
	changefeedID common.ChangeFeedID
	option       *options

	asyncMetricsHook *franz.MetricsHook
	syncMetricsHook  *franz.MetricsHook
	adminMetricsHook *franz.MetricsHook
}

type franzMetricsCollector struct {
	changefeedID common.ChangeFeedID
	hooks        []*franz.MetricsHook
}

func (c *franzMetricsCollector) Run(ctx context.Context) {
	<-ctx.Done()
	for _, hook := range c.hooks {
		if hook != nil {
			hook.CleanupPrometheusMetrics()
		}
	}
	franz.CleanupAdminMetrics(c.changefeedID.Keyspace(), c.changefeedID.Name())
}

func newFranzMetricsHook(changefeedID common.ChangeFeedID, clientType string) *franz.MetricsHook {
	hook := franz.NewMetricsHook(clientType)
	hook.BindPrometheusMetrics(
		changefeedID.Keyspace(),
		changefeedID.Name(),
		franz.PrometheusMetrics{
			RequestsInFlight:  franzRequestsInFlightByClientGauge,
			OutgoingByteRate:  franzOutgoingByteTotalByClientGauge,
			RequestRate:       franzRequestTotalByClientGauge,
			RequestLatency:    franzRequestLatencyHistogram,
			ResponseRate:      franzResponseTotalByClientGauge,
			CompressionRatio:  franzCompressionRatioHistogram,
			RecordsPerRequest: franzRecordsPerRequestHistogram,
		},
	)
	return hook
}

// NewFranzFactory constructs a Factory with franz-go implementation.
//
// NOTE: The franz-go specific implementation details live in `pkg/sink/kafka/franz`.
// This function keeps the public API stable and adapts to the existing kafka package interfaces.
func NewFranzFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	adminInner, err := franz.NewAdminClient(ctx, changefeedID, newFranzOptions(o), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	admin := &franzAdminClientAdapter{inner: adminInner}
	defer admin.Close()

	if err := adjustOptions(ctx, admin, o, o.Topic); err != nil {
		return nil, errors.Trace(err)
	}

	return &franzFactory{
		changefeedID:     changefeedID,
		option:           o,
		asyncMetricsHook: newFranzMetricsHook(changefeedID, clientTypeAsyncProducer),
		syncMetricsHook:  newFranzMetricsHook(changefeedID, clientTypeSyncProducer),
		adminMetricsHook: newFranzMetricsHook(changefeedID, clientTypeAdminClient),
	}, nil
}

func (f *franzFactory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	adminInner, err := franz.NewAdminClient(ctx, f.changefeedID, newFranzOptions(f.option), f.adminMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return &franzAdminClientAdapter{inner: adminInner}, nil
}

func (f *franzFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	producer, err := franz.NewSyncProducer(ctx, f.changefeedID, newFranzOptions(f.option), f.syncMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	producer, err := franz.NewAsyncProducer(ctx, f.changefeedID, newFranzOptions(f.option), f.asyncMetricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) MetricsCollector(_ ClusterAdminClient) MetricsCollector {
	return &franzMetricsCollector{changefeedID: f.changefeedID, hooks: []*franz.MetricsHook{
		f.asyncMetricsHook,
		f.syncMetricsHook,
		f.adminMetricsHook,
	}}
}

func newFranzOptions(o *options) *franz.Options {
	if o == nil {
		return &franz.Options{
			RequiredAcks: int16(WaitForAll),
		}
	}
	return &franz.Options{
		BrokerEndpoints: o.BrokerEndpoints,
		ClientID:        o.ClientID,

		Version:           o.Version,
		IsAssignedVersion: o.IsAssignedVersion,

		MaxMessageBytes: o.MaxMessageBytes,
		Compression:     o.Compression,
		RequiredAcks:    int16(o.RequiredAcks),

		EnableTLS:          o.EnableTLS,
		Credential:         o.Credential,
		InsecureSkipVerify: o.InsecureSkipVerify,
		SASL:               o.SASL,

		DialTimeout:  o.DialTimeout,
		WriteTimeout: o.WriteTimeout,
		ReadTimeout:  o.ReadTimeout,
	}
}
