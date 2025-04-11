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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type sink struct {
	changefeedID common.ChangeFeedID

	dmlProducer *dmlProducers
	ddlProducer *ddlProducers

	comp       component
	statistics *metrics.Statistics

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	ctx      context.Context
}

func (s *sink) SinkType() common.SinkType {
	return common.PulsarSinkType
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, uri *url.URL, sinkConfig *config.SinkConfig) error {
	comp, _, err := newPulsarSinkComponent(ctx, changefeedID, uri, sinkConfig)
	comp.close()
	return err
}

func New(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
) (*sink, error) {
	comp, protocol, err := newPulsarSinkComponent(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err != nil {
			comp.close()
		}
	}()

	failpointCh := make(chan error, 1)
	statistics := metrics.NewStatistics(changefeedID, "sink")
	dmlProducer, err := newDMLProducers(changefeedID, comp, statistics, sinkConfig, failpointCh)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ddlProducer, err := newDDLProducers(changefeedID, comp, statistics, protocol, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &sink{
		changefeedID: changefeedID,
		dmlProducer:  dmlProducer,
		ddlProducer:  ddlProducer,
		statistics:   statistics,
		isNormal:     atomic.NewBool(true),
		ctx:          ctx,
	}, nil
}

func (s *sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.dmlProducer.Run(ctx)
	})
	g.Go(func() error {
		return s.ddlProducer.Run(ctx)
	})
	err := g.Wait()
	s.isNormal.Store(false)
	return errors.Trace(err)
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlProducer.AddDMLEvent(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch v := event.(type) {
	case *commonEvent.DDLEvent:
		if v.TiDBOnly {
			// run callback directly and return
			v.PostFlush()
			return nil
		}
		err := s.ddlProducer.WriteBlockEvent(s.ctx, v)
		if err != nil {
			s.isNormal.Store(false)
			return errors.Trace(err)
		}
	case *commonEvent.SyncPointEvent:
		log.Error("sink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	s.ddlProducer.AddCheckpoint(ts)
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlProducer.SetTableSchemaStore(tableSchemaStore)
}

func (s *sink) GetStartTsList(_ []int64, startTsList []int64, _ bool) ([]int64, []bool, error) {
	return startTsList, make([]bool, len(startTsList)), nil
}

func (s *sink) Close(_ bool) {
	s.ddlProducer.Close()
	s.dmlProducer.Close()
	s.comp.close()
	s.statistics.Close()
}
