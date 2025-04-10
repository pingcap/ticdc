// Copyright 2024 PingCAP, Inc.
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
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type PulsarSink struct {
	changefeedID common.ChangeFeedID

	protocol config.Protocol

	dmlProducer *pulsarDMLProducer
	ddlProducer *pulsarDDLProducers

	topicManager topicmanager.TopicManager
	statistics   *metrics.Statistics

	// isNormal means the sink does not meet error.
	// if sink is normal, isNormal is 1, otherwise is 0
	isNormal uint32
	ctx      context.Context
}

func GetPulsarSinkComponent(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) (PulsarComponent, config.Protocol, error) {
	return getPulsarSinkComponentWithFactory(ctx, changefeedID, sinkURI, sinkConfig, pulsar.NewCreatorFactory)
}

func (s *PulsarSink) SinkType() common.SinkType {
	return common.PulsarSinkType
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	components, _, err := GetPulsarSinkComponent(ctx, changefeedID, uri, sinkConfig)
	if components.TopicManager != nil {
		components.TopicManager.Close()
	}
	return err
}

func New(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*PulsarSink, error) {
	pulsarComponent, protocol, err := GetPulsarSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	statistics := metrics.NewStatistics(changefeedID, "PulsarSink")

	failpointCh := make(chan error, 1)
	dmlProducer, err := NewPulsarDMLProducer(changefeedID, pulsarComponent.Factory, sinkConfig, failpointCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//dmlWorker := worker.NewMQDMLWorker(
	//	changefeedID,
	//	protocol,
	//	dmlProducer,
	//	pulsarComponent.EncoderGroup,
	//	pulsarComponent.ColumnSelector,
	//	pulsarComponent.EventRouter,
	//	pulsarComponent.TopicManager,
	//	statistics,
	//)

	ddlProducer, err := NewPulsarDDLProducer(changefeedID, pulsarComponent.Config, pulsarComponent.Factory, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	//ddlWorker := worker.NewMQDDLWorker(
	//	changefeedID,
	//	protocol,
	//	ddlProducer,
	//	pulsarComponent.Encoder,
	//	pulsarComponent.EventRouter,
	//	pulsarComponent.TopicManager,
	//	statistics,
	//)

	sink := &PulsarSink{
		changefeedID: changefeedID,
		dmlProducer:  dmlProducer,
		ddlProducer:  ddlProducer,
		topicManager: pulsarComponent.TopicManager,
		statistics:   statistics,
		protocol:     protocol,
		ctx:          ctx,
	}
	return sink, nil
}

func (s *PulsarSink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.dmlProducer.Run(ctx)
	})
	g.Go(func() error {
		return s.ddlProducer.Run(ctx)
	})
	err := g.Wait()
	atomic.StoreUint32(&s.isNormal, 0)
	return errors.Trace(err)
}

func (s *PulsarSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *PulsarSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlProducer.AddDMLEvent(event)
}

func (s *PulsarSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *PulsarSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch v := event.(type) {
	case *commonEvent.DDLEvent:
		if v.TiDBOnly {
			// run callback directly and return
			v.PostFlush()
			return nil
		}
		err := s.ddlProducer.WriteBlockEvent(s.ctx, v)
		if err != nil {
			atomic.StoreUint32(&s.isNormal, 0)
			return errors.Trace(err)
		}
	case *commonEvent.SyncPointEvent:
		log.Error("PulsarSink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("PulsarSink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *PulsarSink) AddCheckpointTs(ts uint64) {
	s.ddlProducer.AddCheckpoint(ts)
}

func (s *PulsarSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlProducer.SetTableSchemaStore(tableSchemaStore)
}

func (s *PulsarSink) GetStartTsList(_ []int64, startTsList []int64, _ bool) ([]int64, []bool, error) {
	return startTsList, make([]bool, len(startTsList)), nil
}

func (s *PulsarSink) Close(_ bool) {
	s.ddlProducer.Close()
	s.dmlProducer.Close()
	s.topicManager.Close()
	s.statistics.Close()
}
