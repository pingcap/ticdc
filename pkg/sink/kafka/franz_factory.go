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
	kafkafranz "github.com/pingcap/ticdc/pkg/sink/kafka/franz"
)

type franzFactory struct {
	changefeedID common.ChangeFeedID
	option       *options
	metricsHook  *kafkafranz.MetricsHook
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
	adminInner, err := kafkafranz.NewAdminClient(ctx, changefeedID, newFranzOptions(o), nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	admin := &franzAdminClientAdapter{inner: adminInner}
	defer admin.Close()

	if err := adjustOptions(ctx, admin, o, o.Topic); err != nil {
		return nil, errors.Trace(err)
	}

	return &franzFactory{
		changefeedID: changefeedID,
		option:       o,
		metricsHook:  kafkafranz.NewMetricsHook(),
	}, nil
}

func (f *franzFactory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	adminInner, err := kafkafranz.NewAdminClient(ctx, f.changefeedID, newFranzOptions(f.option), f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return &franzAdminClientAdapter{inner: adminInner}, nil
}

func (f *franzFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	producer, err := kafkafranz.NewSyncProducer(ctx, f.changefeedID, newFranzOptions(f.option), f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	producer, err := kafkafranz.NewAsyncProducer(ctx, f.changefeedID, newFranzOptions(f.option), f.metricsHook)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) MetricsCollector(_ ClusterAdminClient) MetricsCollector {
	return &franzMetricsCollector{
		changefeedID: f.changefeedID,
		hook:         f.metricsHook,
	}
}

func newFranzOptions(o *options) *kafkafranz.Options {
	if o == nil {
		return &kafkafranz.Options{}
	}
	return &kafkafranz.Options{
		BrokerEndpoints: o.BrokerEndpoints,
		ClientID:        o.ClientID,

		Version:           o.Version,
		IsAssignedVersion: o.IsAssignedVersion,

		MaxMessageBytes: o.MaxMessageBytes,
		Compression:     o.Compression,
		RequiredAcks:    kafkafranz.RequiredAcks(o.RequiredAcks),

		EnableTLS:          o.EnableTLS,
		Credential:         o.Credential,
		InsecureSkipVerify: o.InsecureSkipVerify,
		SASL:               o.SASL,

		DialTimeout:  o.DialTimeout,
		WriteTimeout: o.WriteTimeout,
		ReadTimeout:  o.ReadTimeout,
	}
}
