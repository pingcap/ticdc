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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/redo/writer"
	"github.com/pingcap/ticdc/redo/writer/factory"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Sink manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements Sink interface.
type Sink struct {
	ctx       context.Context
	enabled   bool
	cfg       *writer.LogWriterConfig
	ddlWriter writer.RedoLogWriter
	dmlWriter writer.RedoLogWriter

	rwlock sync.RWMutex
	// TODO: remove logBuffer and use writer directly after file logWriter is deprecated.
	logBuffer chan writer.RedoEvent
	closed    int32

	// metricWriteLogDuration    prometheus.Observer
	// metricFlushLogDuration    prometheus.Observer
	// metricTotalRowsCount      prometheus.Counter
	// metricRedoWorkerBusyRatio prometheus.Counter
}

// New creates a new redo sink.
func New(ctx context.Context, changefeedID common.ChangeFeedID,
	startTs common.Ts,
	cfg *config.ConsistentConfig,
) *Sink {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || !redo.IsConsistentEnabled(cfg.Level) {
		return &Sink{enabled: false}
	}

	s := &Sink{
		ctx:     ctx,
		enabled: true,
		cfg: &writer.LogWriterConfig{
			ConsistentConfig:  *cfg,
			CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
			ChangeFeedID:      changefeedID,
			MaxLogSizeInBytes: cfg.MaxLogSize * redo.Megabyte,
		},
		logBuffer: make(chan writer.RedoEvent, 32),
		// metricWriteLogDuration: misc.RedoWriteLogDurationHistogram.
		// 	WithLabelValues(changefeedID.Namespace(), changefeedID.Name(), "event"),
		// metricFlushLogDuration: misc.RedoFlushLogDurationHistogram.
		// 	WithLabelValues(changefeedID.Namespace(), changefeedID.Name(), "event"),
		// metricTotalRowsCount: misc.RedoTotalRowsCountGauge.
		// 	WithLabelValues(changefeedID.Namespace(), changefeedID.Name(), "event"),
		// metricRedoWorkerBusyRatio: misc.RedoWorkerBusyRatio.
		// 	WithLabelValues(changefeedID.Namespace(), changefeedID.Name(), "event"),
	}
	start := time.Now()
	ddlWriter, err := factory.NewRedoLogWriter(s.ctx, s.cfg, redo.RedoDDLLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("namespace", s.cfg.ChangeFeedID.Namespace()),
			zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil
	}
	dmlWriter, err := factory.NewRedoLogWriter(s.ctx, s.cfg, redo.RedoRowLogFileType)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("namespace", s.cfg.ChangeFeedID.Namespace()),
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
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.ddlWriter.Run(ctx)
	})
	eg.Go(func() error {
		return s.dmlWriter.Run(ctx)
	})
	return eg.Wait()
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		return s.ddlWriter.WriteEvents(s.ctx, e)
	}
	return nil
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWriter.AsyncWriteEvents(s.ctx, event)
}

func (s *Sink) IsNormal() bool {
	return true
}

func (s *Sink) SinkType() common.SinkType {
	return common.RedoSinkType
}

func (s *Sink) Enabled() bool {
	return s.enabled
}

func (s *Sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
}

func (s *Sink) Close(_ bool) {
	atomic.StoreInt32(&s.closed, 1)

	close(s.logBuffer)
	if s.ddlWriter != nil {
		if err := s.ddlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo manager fails to close ddl writer",
				zap.String("namespace", s.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
				zap.Error(err))
		}
	}
	if s.dmlWriter != nil {
		if err := s.dmlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo manager fails to close dml writer",
				zap.String("namespace", s.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", s.cfg.ChangeFeedID.Name()),
				zap.Error(err))
		}
	}
	log.Info("redo manager closed",
		zap.String("namespace", s.cfg.ChangeFeedID.Namespace()),
		zap.String("changefeed", s.cfg.ChangeFeedID.Name()))
}
