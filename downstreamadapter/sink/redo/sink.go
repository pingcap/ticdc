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

package redo

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/factory"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Sink manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements Sink interface.
type Sink struct {
	ctx          context.Context
	changefeedID common.ChangeFeedID
	ddlWriter    writer.RedoLogWriter
	dmlWriter    writer.RedoLogWriter

	logBuffer *chann.UnlimitedChannel[writer.RedoEvent, any]

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	isClosed *atomic.Bool

	metricCollector *metricCollector
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, cfg *config.ConsistentConfig) error {
	if cfg == nil || !redo.IsConsistentEnabled(util.GetOrZero(cfg.Level)) {
		return nil
	}
	return nil
}

// New creates a new redo sink.
func New(ctx context.Context, changefeedID common.ChangeFeedID,
	cfg *config.ConsistentConfig,
) (*Sink, error) {
	config, err := writer.NewConfig(changefeedID, cfg)
	if err != nil {
		return nil, err
	}
	s := &Sink{
		ctx:          ctx,
		changefeedID: changefeedID,
		logBuffer:    chann.NewUnlimitedChannelDefault[writer.RedoEvent](),
		isNormal:     atomic.NewBool(true),
		isClosed:     atomic.NewBool(false),
	}
	start := time.Now()
	ddlWriter, err := factory.NewRedoLogWriter(ctx, config, redo.RedoDDLLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, err
	}
	dmlWriter, err := factory.NewRedoLogWriter(ctx, config, redo.RedoRowLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, err
	}
	s.ddlWriter = ddlWriter
	s.dmlWriter = dmlWriter
	s.metricCollector = newMetricCollector(changefeedID)
	return s, nil
}

func (s *Sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer s.logBuffer.Close()
		return s.dmlWriter.Run(ctx)
	})
	g.Go(func() error {
		return s.sendMessages(ctx)
	})
	err := g.Wait()
	s.isNormal.Store(false)
	return err
}

func (s *Sink) FlushDMLBeforeBlock(_ commonEvent.BlockEvent) error {
	return nil
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		start := time.Now()
		err := s.ddlWriter.WriteEvents(s.ctx, e)
		if err != nil {
			s.isNormal.Store(false)
			return err
		}
		s.metricCollector.observeDDLWrite(time.Since(start))
	}
	return nil
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	rowsCount := event.Len()
	events := make([]writer.RedoEvent, 0, rowsCount)
	rowCallback := helper.NewTxnPostFlushRowCallback(event, uint64(rowsCount))

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		events = append(events, &commonEvent.RedoRowEvent{
			StartTs:         event.StartTs,
			CommitTs:        event.CommitTs,
			Event:           row,
			PhysicalTableID: event.PhysicalTableID,
			TableInfo:       event.TableInfo,
			Callback:        rowCallback,
		})
	}
	s.logBuffer.Push(events...)
}

func (s *Sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *Sink) SinkType() common.SinkType {
	return common.RedoSinkType
}

func (s *Sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	s.ddlWriter.SetTableSchemaStore(tableSchemaStore)
}

func (s *Sink) Close(_ bool) {
	if !s.isClosed.CompareAndSwap(false, true) {
		return
	}
	start := time.Now()
	s.logBuffer.Close()
	if s.ddlWriter != nil {
		if err := s.ddlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close ddl writer",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Error(err))
		}
	}

	if s.dmlWriter != nil {
		if err := s.dmlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close dml writer",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Error(err))
		}
	}
	if s.metricCollector != nil {
		s.metricCollector.close()
	}
	log.Info("redo sink closed",
		zap.String("keyspace", s.changefeedID.Keyspace()),
		zap.String("changefeed", s.changefeedID.Name()),
		zap.Duration("duration", time.Since(start)))
}

func (s *Sink) sendMessages(ctx context.Context) error {
	buffer := make([]writer.RedoEvent, 0, redo.DefaultFlushBatchSize)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		default:
		}
		events, ok := s.logBuffer.GetMultipleNoGroup(buffer)
		if !ok {
			return nil
		}
		if len(events) == 0 {
			continue
		}
		buffer = events[:0]

		start := time.Now()
		err := s.dmlWriter.WriteEvents(ctx, events...)
		if err != nil {
			return err
		}
		s.metricCollector.observeRowWrite(len(events), time.Since(start))
	}
}

func (s *Sink) AddCheckpointTs(_ uint64) {}
