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
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tiflow/pkg/sink"
)

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
