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

package sink

import (
	"context"
	"net/url"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type PulsarSink struct {
	changefeedID common.ChangeFeedID

	dmlWorker *worker.PulsarDMLWorker
	ddlWorker *worker.PulsarDDLWorker

	topicManager topicmanager.TopicManager
	statistics   *metrics.Statistics

	// isNormal means the sink does not meet error.
	// if sink is normal, isNormal is 1, otherwise is 0
	isNormal uint32
	ctx      context.Context
}

func (s *PulsarSink) SinkType() common.SinkType {
	return common.PulsarSinkType
}

func verifyPulsarSink(ctx context.Context, changefeedID common.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	components, _, err := worker.GetPulsarSinkComponent(ctx, changefeedID, uri, sinkConfig)
	if components.TopicManager != nil {
		components.TopicManager.Close()
	}
	return err
}

func newPulsarSink(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*PulsarSink, error) {
	pulsarComponent, protocol, err := worker.GetPulsarSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")

	failpointCh := make(chan error, 1)
	dmlProducer, err := producer.NewPulsarDMLProducer(ctx, changefeedID, pulsarComponent.Factory, sinkConfig, failpointCh)
	dmlWorker := worker.NewPulsarDMLWorker(
		changefeedID,
		protocol,
		dmlProducer,
		pulsarComponent.EncoderGroup,
		pulsarComponent.ColumnSelector,
		pulsarComponent.EventRouter,
		pulsarComponent.TopicManager,
		statistics,
	)

	ddlProducer, err := producer.NewPulsarDDLProducer(ctx, changefeedID, pulsarComponent.Config, pulsarComponent.Factory, sinkConfig)
	ddlWorker := worker.NewPulsarDDLWorker(
		changefeedID,
		protocol,
		pulsarComponent.Config,
		ddlProducer,
		pulsarComponent.Encoder,
		pulsarComponent.EventRouter,
		pulsarComponent.TopicManager,
		statistics,
	)

	sink := &PulsarSink{
		changefeedID: changefeedID,
		dmlWorker:    dmlWorker,
		ddlWorker:    ddlWorker,
		topicManager: pulsarComponent.TopicManager,
		ctx:          ctx,
	}
	return sink, nil
}

func (s *PulsarSink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.dmlWorker.Run(ctx)
	})
	g.Go(func() error {
		return s.ddlWorker.Run(ctx)
	})
	err := g.Wait()
	atomic.StoreUint32(&s.isNormal, 0)
	return errors.Trace(err)
}

func (s *PulsarSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *PulsarSink) AddDMLEvent(event *commonEvent.DMLEvent) error {
	s.dmlWorker.AddDMLEvent(event)
	return nil
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
		err := s.ddlWorker.WriteBlockEvent(s.ctx, v)
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
	s.ddlWorker.AddCheckpoint(ts)
}

func (s *PulsarSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *PulsarSink) Close(_ bool) {
	s.ddlWorker.Close()
	s.dmlWorker.Close()
	s.topicManager.Close()
	s.statistics.Close()
}
