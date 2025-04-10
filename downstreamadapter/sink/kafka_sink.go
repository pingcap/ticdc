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
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type KafkaSink struct {
	changefeedID common.ChangeFeedID

	dmlWorker *worker.MQDMLWorker
	ddlWorker *worker.MQDDLWorker

	// the module used by dmlWorker and ddlWorker
	// KafkaSink need to close it when Close() is called
	adminClient      kafka.ClusterAdminClient
	topicManager     topicmanager.TopicManager
	statistics       *metrics.Statistics
	metricsCollector kafka.MetricsCollector

	// isNormal means the sink does not meet error.
	// if sink is normal, isNormal is 1, otherwise is 0
	isNormal uint32
	ctx      context.Context
}

func (s *KafkaSink) SinkType() common.SinkType {
	return common.KafkaSinkType
}

func verifyKafkaSink(ctx context.Context, changefeedID common.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	components, _, err := worker.GetKafkaSinkComponent(ctx, changefeedID, uri, sinkConfig)
	if components.AdminClient != nil {
		components.AdminClient.Close()
	}
	if components.TopicManager != nil {
		components.TopicManager.Close()
	}
	return err
}

func newKafkaSink(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*KafkaSink, error) {
	kafkaComponent, protocol, err := worker.GetKafkaSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.AdminClient != nil {
			kafkaComponent.AdminClient.Close()
		}
	}()

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	asyncProducer, err := kafkaComponent.Factory.AsyncProducer(ctx)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	dmlProducer := producer.NewKafkaDMLProducer(changefeedID, asyncProducer)
	dmlWorker := worker.NewMQDMLWorker(
		changefeedID,
		protocol,
		dmlProducer,
		kafkaComponent.EncoderGroup,
		kafkaComponent.ColumnSelector,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics)

	syncProducer, err := kafkaComponent.Factory.SyncProducer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlProducer := producer.NewKafkaDDLProducer(ctx, changefeedID, syncProducer)
	ddlWorker := worker.NewMQDDLWorker(
		changefeedID,
		protocol,
		ddlProducer,
		kafkaComponent.Encoder,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics)

	sink := &KafkaSink{
		changefeedID:     changefeedID,
		dmlWorker:        dmlWorker,
		ddlWorker:        ddlWorker,
		adminClient:      kafkaComponent.AdminClient,
		topicManager:     kafkaComponent.TopicManager,
		statistics:       statistics,
		ctx:              ctx,
		metricsCollector: kafkaComponent.Factory.MetricsCollector(kafkaComponent.AdminClient),
	}
	return sink, nil
}

func (s *KafkaSink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.dmlWorker.Run(ctx)
	})
	g.Go(func() error {
		return s.ddlWorker.Run(ctx)
	})
	g.Go(func() error {
		s.metricsCollector.Run(ctx)
		return nil
	})
	err := g.Wait()
	atomic.StoreUint32(&s.isNormal, 0)
	return errors.Trace(err)
}

func (s *KafkaSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *KafkaSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWorker.AddDMLEvent(event)
}

func (s *KafkaSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *KafkaSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
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
		log.Error("KafkaSink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("KafkaSink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *KafkaSink) AddCheckpointTs(ts uint64) {
	s.ddlWorker.AddCheckpoint(ts)
}

func (s *KafkaSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *KafkaSink) GetStartTsList(_ []int64, startTsList []int64, _ bool) ([]int64, []bool, error) {
	return startTsList, make([]bool, len(startTsList)), nil
}

func (s *KafkaSink) Close(_ bool) {
	s.ddlWorker.Close()
	s.dmlWorker.Close()
	s.adminClient.Close()
	s.topicManager.Close()
	s.statistics.Close()
}
