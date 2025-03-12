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

package worker

import (
	"context"
	"net/url"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	v2 "github.com/pingcap/ticdc/pkg/sink/kafka/v2"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tiflow/pkg/sink"
)

type KafkaComponent struct {
	EncoderGroup   codec.EncoderGroup
	Encoder        common.EventEncoder
	ColumnSelector *columnselector.ColumnSelectors
	EventRouter    *eventrouter.EventRouter
	TopicManager   topicmanager.TopicManager
	AdminClient    kafka.ClusterAdminClient
	Factory        kafka.Factory
}

func getKafkaSinkComponentWithFactory(ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	factoryCreator kafka.FactoryCreator,
) (KafkaComponent, config.Protocol, error) {
	kafkaComponent := KafkaComponent{}
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return kafkaComponent, config.ProtocolUnknown, errors.Trace(err)
	}

	options := kafka.NewOptions()
	if err = options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	kafkaComponent.Factory, err = factoryCreator(ctx, options, changefeedID)
	if err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	kafkaComponent.AdminClient, err = kafkaComponent.Factory.AdminClient()
	if err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.AdminClient != nil {
			kafkaComponent.AdminClient.Close()
		}
	}()

	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}
	// adjust the option configuration before creating the kafka client
	if err = kafka.AdjustOptions(ctx, kafkaComponent.AdminClient, options, topic); err != nil {
		return kafkaComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	kafkaComponent.TopicManager, err = topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		kafkaComponent.AdminClient,
	)

	scheme := sink.GetScheme(sinkURI)
	kafkaComponent.EventRouter, err = eventrouter.NewEventRouter(sinkConfig, protocol, topic, scheme)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.ColumnSelector, err = columnselector.NewColumnSelectors(sinkConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, options.MaxMessageBytes)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.EncoderGroup, err = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}

	kafkaComponent.Encoder, err = codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return kafkaComponent, protocol, errors.Trace(err)
	}
	return kafkaComponent, protocol, nil
}

func GetKafkaSinkComponent(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (KafkaComponent, config.Protocol, error) {
	factoryCreator := kafka.NewSaramaFactory
	if utils.GetOrZero(sinkConfig.EnableKafkaSinkV2) {
		factoryCreator = v2.NewFactory
	}
	return getKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, factoryCreator)
}

func GetKafkaSinkComponentForTest(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (KafkaComponent, config.Protocol, error) {
	return getKafkaSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, kafka.NewMockFactory)
}

type PulsarComponent struct {
	Config         *config.PulsarConfig
	EncoderGroup   codec.EncoderGroup
	Encoder        common.EventEncoder
	ColumnSelector *columnselector.ColumnSelectors
	EventRouter    *eventrouter.EventRouter
	TopicManager   topicmanager.TopicManager
	Factory        pulsarClient.Client
}

func getPulsarSinkComponentWithFactory(ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	factoryCreator pulsar.FactoryCreator,
) (PulsarComponent, config.Protocol, error) {
	pulsarComponent := PulsarComponent{}
	protocol, err := helper.GetProtocol(utils.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return pulsarComponent, config.ProtocolUnknown, errors.Trace(err)
	}

	pulsarComponent.Config, err = pulsar.NewPulsarConfig(sinkURI, sinkConfig.PulsarConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.Factory, err = factoryCreator(pulsarComponent.Config, changefeedID, sinkConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.TopicManager, err = topicmanager.GetPulsarTopicManagerAndTryCreateTopic(ctx, pulsarComponent.Config, topic, pulsarComponent.Factory)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	scheme := sink.GetScheme(sinkURI)
	pulsarComponent.EventRouter, err = eventrouter.NewEventRouter(sinkConfig, protocol, topic, scheme)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.ColumnSelector, err = columnselector.NewColumnSelectors(sinkConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, config.DefaultMaxMessageBytes)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.EncoderGroup, err = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.Encoder, err = codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}
	return pulsarComponent, protocol, nil
}

func GetPulsarSinkComponent(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (PulsarComponent, config.Protocol, error) {
	return getPulsarSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, pulsar.NewCreatorFactory)
}

func GetPulsarSinkComponentForTest(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (PulsarComponent, config.Protocol, error) {
	return getPulsarSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, pulsar.NewMockCreatorFactory)
}
