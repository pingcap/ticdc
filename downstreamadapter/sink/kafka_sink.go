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
	"fmt"
	"net/url"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/topicmanager"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type KafkaSink struct {
	changefeedID common.ChangeFeedID

	dmlWorker *worker.KafkaDMLWorker
	ddlWorker *worker.KafkaDDLWorker

	// the module used by dmlWorker and ddlWorker
	// KafkaSink need to close it when Close() is called
	adminClient      kafka.ClusterAdminClient
	topicManager     topicmanager.TopicManager
	statistics       *metrics.Statistics
	metricsCollector kafka.MetricsCollector

	errgroup *errgroup.Group
	ctx      context.Context
	cancel   context.CancelFunc

	errCh    chan error
	isNormal uint32 // if sink is normal, isNormal is 1, otherwise is 0
}

func (s *KafkaSink) SinkType() common.SinkType {
	return common.KafkaSinkType
}

func NewKafkaSink(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	errCh chan error,
) (*KafkaSink, error) {
	var err error
	errGroup, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			cancel()
		}
	}()
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
	dmlAsyncProducer, err := kafkaComponent.Factory.AsyncProducer(ctx)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewProducer, err)
	}
	dmlProducer := producer.NewKafkaDMLProducer(changefeedID, dmlAsyncProducer)
	dmlWorker := worker.NewKafkaDMLWorker(
		changefeedID,
		protocol,
		dmlProducer,
		kafkaComponent.EncoderGroup,
		kafkaComponent.ColumnSelector,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics,
		errGroup)

	ddlSyncProducer, err := kafkaComponent.Factory.SyncProducer()
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlProducer := producer.NewKafkaDDLProducer(ctx, changefeedID, ddlSyncProducer)
	ddlWorker := worker.NewKafkaDDLWorker(ctx,
		changefeedID,
		protocol,
		ddlProducer,
		kafkaComponent.Encoder,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics,
		errGroup)

	sink := &KafkaSink{
		changefeedID:     changefeedID,
		dmlWorker:        dmlWorker,
		ddlWorker:        ddlWorker,
		adminClient:      kafkaComponent.AdminClient,
		topicManager:     kafkaComponent.TopicManager,
		statistics:       statistics,
		metricsCollector: kafkaComponent.Factory.MetricsCollector(kafkaComponent.AdminClient),
		errgroup:         errGroup,
		ctx:              ctx,
		cancel:           cancel,
		errCh:            errCh,
	}
	go sink.run(ctx)
	return sink, nil
}

func (s *KafkaSink) run(ctx context.Context) {
	s.dmlWorker.Run(ctx)
	s.ddlWorker.Run(ctx)
	s.errgroup.Go(func() error {
		s.metricsCollector.Run(ctx)
		return nil
	})
	err := s.errgroup.Wait()
	if errors.Cause(err) != context.Canceled {
		atomic.StoreUint32(&s.isNormal, 0)
		select {
		case s.errCh <- err:
		default:
			log.Error("error channel is full, discard error",
				zap.Any("changefeedID", s.changefeedID.String()),
				zap.Error(err))
		}
	}
}

func (s *KafkaSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *KafkaSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWorker.GetEventChan() <- event
}

func (s *KafkaSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *KafkaSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event := event.(type) {
	case *commonEvent.DDLEvent:
		if event.TiDBOnly {
			// run callback directly and return
			event.PostFlush()
			return nil
		}
		err := s.ddlWorker.WriteBlockEvent(s.ctx, event)
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
	s.ddlWorker.GetCheckpointTsChan() <- ts
}

func (s *KafkaSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *KafkaSink) Close(_ bool) error {
	if s.cancel != nil {
		s.cancel()
	}
	err := s.ddlWorker.Close()
	if err != nil {
		return errors.Trace(err)
	}

	err = s.dmlWorker.Close()
	if err != nil {
		return errors.Trace(err)
	}

	s.adminClient.Close()
	s.topicManager.Close()
	s.statistics.Close()

	return nil
}

func newKafkaSinkForTest() (*KafkaSink, producer.DMLProducer, producer.DDLProducer, error) {
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := "open-protocol"
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	errGroup, ctx := errgroup.WithContext(ctx)
	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	kafkaComponent, protocol, err := worker.GetKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.AdminClient != nil {
			kafkaComponent.AdminClient.Close()
		}
	}()

	dmlMockProducer := producer.NewMockDMLProducer()

	dmlWorker := worker.NewKafkaDMLWorker(
		changefeedID,
		protocol,
		dmlMockProducer,
		kafkaComponent.EncoderGroup,
		kafkaComponent.ColumnSelector,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics,
		errGroup)

	ddlMockProducer := producer.NewMockDDLProducer()
	ddlWorker := worker.NewKafkaDDLWorker(ctx,
		changefeedID,
		protocol,
		ddlMockProducer,
		kafkaComponent.Encoder,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics,
		errGroup)

	sink := &KafkaSink{
		changefeedID:     changefeedID,
		dmlWorker:        dmlWorker,
		ddlWorker:        ddlWorker,
		adminClient:      kafkaComponent.AdminClient,
		topicManager:     kafkaComponent.TopicManager,
		statistics:       statistics,
		metricsCollector: kafkaComponent.Factory.MetricsCollector(kafkaComponent.AdminClient),
		errgroup:         errGroup,
		errCh:            make(chan error, 1),
	}
	go sink.run(ctx)
	return sink, dmlMockProducer, ddlMockProducer, nil
}
