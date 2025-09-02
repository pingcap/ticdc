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
}

func (c components) close() {
	if c.adminClient != nil {
		c.adminClient.Close()
	}
	if c.topicManager != nil {
		c.topicManager.Close()
	}
}

func newKafkaSinkComponentWithFactory(ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	factoryCreator kafka.FactoryCreator,
) (components, config.Protocol, error) {
	kafkaComponent := components{}
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return kafkaComponent, config.ProtocolUnknown, errors.Trace(err)
	}

	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err = options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	options.Topic = topic

	kafkaComponent.factory, err = factoryCreator(ctx, options, changefeedID)
	if err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	kafkaComponent.eventRouter, err = eventrouter.NewEventRouter(
		sinkConfig, topic, false, protocol == config.ProtocolAvro)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.columnSelector, err = columnselector.New(sinkConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	encoderConfig, err := helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.encoderGroup, err = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.encoder, err = codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.adminClient, err = kafkaComponent.factory.AdminClient()
	if err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.adminClient != nil {
			kafkaComponent.adminClient.Close()
		}
	}()

	kafkaComponent.topicManager, err = topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		kafkaComponent.adminClient,
	)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}
	return kafkaComponent, protocol, nil
}

func newKafkaSinkComponent(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (components, config.Protocol, error) {
	return newKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, kafka.NewSaramaFactory)
}

func newKafkaSinkComponentForTest(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (components, config.Protocol, error) {
	return newKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, kafka.NewMockFactory)
}
