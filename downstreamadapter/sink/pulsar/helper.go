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

package pulsar

import (
	"context"
	"net/url"

	pulsarClient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	putil "github.com/pingcap/ticdc/pkg/util"
)

type component struct {
	config         *config.PulsarConfig
	encoderGroup   codec.EncoderGroup
	encoder        common.EventEncoder
	columnSelector *columnselector.ColumnSelectors
	eventRouter    *eventrouter.EventRouter
	topicManager   topicmanager.TopicManager
	client         pulsarClient.Client
}

func (c component) close() {
	if c.topicManager != nil {
		c.topicManager.Close()
	}
	if c.client != nil {
		go c.client.Close()
	}
}

func newPulsarSinkComponent(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (component, config.Protocol, error) {
	return newPulsarSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, pulsar.NewCreatorFactory)
}

func newPulsarSinkComponentForTest(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (component, config.Protocol, error) {
	return newPulsarSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, pulsar.NewMockCreatorFactory)
}

func newPulsarSinkComponentWithFactory(ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	factoryCreator pulsar.FactoryCreator,
) (component, config.Protocol, error) {
	pulsarComponent := component{}
	protocol, err := helper.GetProtocol(putil.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return pulsarComponent, config.ProtocolUnknown, errors.Trace(err)
	}

	pulsarComponent.config, err = pulsar.NewPulsarConfig(sinkURI, sinkConfig.PulsarConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.client, err = factoryCreator(pulsarComponent.config, changefeedID, sinkConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}

	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.topicManager, err = topicmanager.GetPulsarTopicManagerAndTryCreateTopic(ctx, pulsarComponent.config, topic, pulsarComponent.client)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	// pulsar only support canal-json, so we don't need to check the protocol
	pulsarComponent.eventRouter, err = eventrouter.NewEventRouter(sinkConfig, topic, true, false)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.columnSelector, err = columnselector.New(sinkConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	encoderConfig, err := helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, config.DefaultMaxMessageBytes)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.encoderGroup, err = codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, changefeedID)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}

	pulsarComponent.encoder, err = codec.NewEventEncoder(ctx, encoderConfig)
	if err != nil {
		return pulsarComponent, protocol, errors.Trace(err)
	}
	return pulsarComponent, protocol, nil
}
