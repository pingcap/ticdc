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
	"net/url"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/kafka/claimcheck"
	"github.com/pingcap/tidb/br/pkg/utils"
)

type components struct {
	encoderGroup   codec.EncoderGroup
	encoder        common.EventEncoder
	columnSelector *columnselector.ColumnSelectors
	eventRouter    *eventrouter.EventRouter
	topicManager   topicmanager.TopicManager
	adminClient    kafka.ClusterAdminClient
	factory        kafka.Factory
	claimCheck     *claimcheck.ClaimCheck
}

func (c components) close() {
	if c.adminClient != nil {
		c.adminClient.Close()
	}
	if c.topicManager != nil {
		c.topicManager.Close()
	}
	if c.claimCheck != nil {
		c.claimCheck.Close()
	}
}

func newKafkaSinkComponent(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (components, config.Protocol, error) {
	var (
		comp components
		err  error
	)
	// must release resources when error occurs.
	defer func() {
		if err != nil {
			comp.close()
		}
	}()
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return comp, config.ProtocolUnknown, errors.Trace(err)
	}

	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err = options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return comp, protocol, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	options.Topic = topic

	comp.factory, err = kafka.NewSaramaFactory(ctx, options, changefeedID)
	if err != nil {
		return comp, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	isAvroLike := protocol == config.ProtocolAvro || protocol == config.ProtocolDebeziumAvro
	comp.eventRouter, err = eventrouter.NewEventRouter(
		sinkConfig, topic, false, isAvroLike)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	comp.columnSelector, err = columnselector.New(sinkConfig)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	encoderConfig, err := helper.GetEncoderConfig(
		changefeedID, sinkURI, protocol, sinkConfig,
		options.MaxMessageBytes, options.MaxBatchedBytes,
	)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	comp.claimCheck, err = claimcheck.New(ctx, encoderConfig.LargeMessageHandle, changefeedID)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	comp.encoderGroup, err = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, comp.claimCheck, changefeedID)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	comp.encoder, err = codec.NewEventEncoder(ctx, encoderConfig, comp.claimCheck)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}

	comp.adminClient, err = comp.factory.AdminClient(ctx)
	if err != nil {
		return comp, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	comp.topicManager, err = topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		comp.adminClient,
	)
	if err != nil {
		return comp, protocol, errors.Trace(err)
	}
	return comp, protocol, nil
}
