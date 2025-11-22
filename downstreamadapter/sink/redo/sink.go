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
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/factory"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Sink manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements Sink interface.
type Sink struct {
	ctx       context.Context
	cfg       *writer.LogWriterConfig
	ddlWriter writer.RedoLogWriter
	dmlWriter writer.RedoLogWriter

	logBuffer *chann.UnlimitedChannel[writer.RedoEvent, any]

	// isNormal indicate whether the sink is in the normal state.
	isNormal   *atomic.Bool
	isClosed   *atomic.Bool
	statistics *metrics.Statistics
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, cfg *config.ConsistentConfig) error {
	if cfg == nil || !redo.IsConsistentEnabled(cfg.Level) {
		return nil
	}
	return nil
}

// New creates a new redo sink.
func New(ctx context.Context, changefeedID common.ChangeFeedID,
	startTs common.Ts,
	cfg *config.ConsistentConfig,
) *Sink {
	s := &Sink{
		ctx: ctx,
		cfg: &writer.LogWriterConfig{
			ConsistentConfig:  *cfg,
			CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
			ChangeFeedID:      changefeedID,
			MaxLogSizeInBytes: cfg.MaxLogSize * redo.Megabyte,
		},
		logBuffer:  chann.NewUnlimitedChannelDefault[writer.RedoEvent](),
		isNormal:   atomic.NewBool(true),
		isClosed:   atomic.NewBool(false),
		statistics: metrics.NewStatistics(changefeedID, "redo"),
	}
	start := time.Now()
	ddlWriter, err := factory.NewRedoLogWriter(s.ctx, s.cfg, redo.RedoDDLLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
			zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil
	}
	dmlWriter, err := factory.NewRedoLogWriter(s.ctx, s.cfg, redo.RedoRowLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
			zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil
	}
	s.ddlWriter = ddlWriter
	s.dmlWriter = dmlWriter
	return s
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

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		err := s.statistics.RecordDDLExecution(func() error {
			return s.ddlWriter.WriteEvents(s.ctx, e)
		})
		if err != nil {
			s.isNormal.Store(false)
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	_ = s.statistics.RecordBatchExecution(func() (int, int64, error) {
		for {
			row, ok := event.GetNextRow()
			if !ok {
				event.Rewind()
				break
			}
			s.logBuffer.Push(&commonEvent.RedoRowEvent{
				StartTs:         event.StartTs,
				CommitTs:        event.CommitTs,
				Event:           row,
				PhysicalTableID: event.PhysicalTableID,
				TableInfo:       event.TableInfo,
				Callback:        event.PostFlush,
			})
		}
		return int(event.Len()), event.GetSize(), nil
	})
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
	s.logBuffer.Close()
	if s.ddlWriter != nil {
		if err := s.ddlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close ddl writer",
				zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
				zap.Error(err))
		}
	}
	if s.dmlWriter != nil {
		if err := s.dmlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close dml writer",
				zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
				zap.Error(err))
		}
	}
	if s.statistics != nil {
		s.statistics.Close()
	}
	log.Info("redo sink closed",
		zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
		zap.String("changefeed", s.cfg.ChangeFeedID.Name()))
}

func (s *Sink) sendMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if e, ok := s.logBuffer.Get(); ok {
				err := s.dmlWriter.WriteEvents(ctx, e)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (s *Sink) AddCheckpointTs(_ uint64) {}
