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

	"github.com/pingcap/failpoint"
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
)

// Sink manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements Sink interface.
type Sink struct {
	ctx     context.Context
	enabled bool
	cfg     *writer.LogWriterConfig
	writer  writer.RedoLogWriter

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

	r := &Sink{
		ctx:     ctx,
		enabled: true,
		cfg: &writer.LogWriterConfig{
			ConsistentConfig:  *cfg,
			CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
			ChangeFeedID:      changefeedID,
			MaxLogSizeInBytes: cfg.MaxLogSize * redo.Megabyte,
			// FIXME
			LogType: redo.RedoDDLLogFileType,
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
	return r
}

// Run implements pkg/util.Runnable.
func (m *Sink) Run(ctx context.Context) error {
	failpoint.Inject("ChangefeedNewRedoManagerError", func() {
		failpoint.Return(errors.New("changefeed new redo manager injected error"))
	})
	if !m.Enabled() {
		return nil
	}

	start := time.Now()
	w, err := factory.NewRedoLogWriter(m.ctx, m.cfg)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
			zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return err
	}
	m.writer = w
	return m.bgUpdateLog()
}

func (m *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		return m.emitRedoEvents(e)
	}
	return nil
}

func (m *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	m.emitRedoEvents(event)
}

func (m *Sink) Close(_ bool) {
	m.close()
}

func (m *Sink) IsNormal() bool {
	return true
}

func (m *Sink) SinkType() common.SinkType {
	return common.RedoSinkType
}

func (m *Sink) Enabled() bool {
	return m.enabled
}

func (s *Sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
}

func (m *Sink) getFlushDuration() time.Duration {
	flushIntervalInMs := m.cfg.FlushIntervalInMs
	defaultFlushIntervalInMs := redo.DefaultFlushIntervalInMs
	// if m.cfg.LogType == redo.RedoMetaFileType {
	// 	flushIntervalInMs = m.cfg.MetaFlushIntervalInMs
	// 	defaultFlushIntervalInMs = redo.DefaultMetaFlushIntervalInMs
	// }
	if flushIntervalInMs < redo.MinFlushIntervalInMs {
		log.Warn("redo flush interval is too small, use default value",
			zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
			zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
			zap.Int("default", defaultFlushIntervalInMs),
			zap.String("logType", m.cfg.LogType),
			zap.Int64("interval", flushIntervalInMs))
		flushIntervalInMs = int64(defaultFlushIntervalInMs)
	}
	return time.Duration(flushIntervalInMs) * time.Millisecond
}

// emitRedoEvents sends row changed events to a log buffer, the log buffer
// will be consumed by a background goroutine, which converts row changed events
// to redo logs and sends to log writer.
func (m *Sink) emitRedoEvents(
	event writer.RedoEvent,
) error {
	return m.withLock(func(m *Sink) error {
		select {
		case <-m.ctx.Done():
			return errors.Trace(m.ctx.Err())
		case m.logBuffer <- event:
		}
		return nil
	})
}

func (m *Sink) handleEvent(
	ctx context.Context, e writer.RedoEvent, workTimeSlice *time.Duration,
) error {
	startHandleEvent := time.Now()
	defer func() {
		*workTimeSlice += time.Since(startHandleEvent)
	}()

	start := time.Now()
	err := m.writer.WriteEvents(ctx, e)
	if err != nil {
		return errors.Trace(err)
	}
	writeLogElapse := time.Since(start)
	log.Debug("redo sink writes rows",
		zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
		zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
		zap.Duration("writeLogElapse", writeLogElapse))
	// m.metricTotalRowsCount.Add(float64(len(e.events)))
	// m.metricWriteLogDuration.Observe(writeLogElapse.Seconds())
	return nil
}

func (m *Sink) bgUpdateLog() error {
	flushDuration := m.getFlushDuration()
	ticker := time.NewTicker(flushDuration)
	defer ticker.Stop()
	log.Info("redo manager bgUpdateLog is running",
		zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
		zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
		zap.Duration("flushIntervalInMs", flushDuration),
		zap.Int64("maxLogSize", m.cfg.MaxLogSize),
		zap.Int("encoderWorkerNum", m.cfg.EncodingWorkerNum),
		zap.Int("flushWorkerNum", m.cfg.FlushWorkerNum))

	var err error
	// logErrCh is used to retrieve errors from log flushing goroutines.
	// if the channel is full, it's better to block subsequent flushing goroutines.
	logErrCh := make(chan error, 1)
	// handleErr := func(err error) { logErrCh <- err }

	overseerDuration := time.Second * 5
	overseerTicker := time.NewTicker(overseerDuration)
	defer overseerTicker.Stop()
	var workTimeSlice time.Duration
	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		case event, ok := <-m.logBuffer:
			if !ok {
				return nil // channel closed
			}
			// TODO: flushlog when meets too many events
			err = m.handleEvent(m.ctx, event, &workTimeSlice)
		case <-overseerTicker.C:
			// m.metricRedoWorkerBusyRatio.Add(workTimeSlice.Seconds())
			workTimeSlice = 0
		case err = <-logErrCh:
		}
		if err != nil {
			log.Warn("redo manager writer meets write or flush fail",
				zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
				zap.Error(err))
			return err
		}
	}
}

func (m *Sink) withLock(action func(m *Sink) error) error {
	m.rwlock.RLock()
	defer m.rwlock.RUnlock()
	if atomic.LoadInt32(&m.closed) != 0 {
		return errors.ErrRedoWriterStopped.GenWithStack("redo manager is closed")
	}
	return action(m)
}

func (m *Sink) close() {
	m.rwlock.Lock()
	defer m.rwlock.Unlock()
	atomic.StoreInt32(&m.closed, 1)

	close(m.logBuffer)
	if m.writer != nil {
		if err := m.writer.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo manager fails to close writer",
				zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", m.cfg.ChangeFeedID.Name()),
				zap.Error(err))
		}
	}
	log.Info("redo manager closed",
		zap.String("namespace", m.cfg.ChangeFeedID.Namespace()),
		zap.String("changefeed", m.cfg.ChangeFeedID.Name()))
}
