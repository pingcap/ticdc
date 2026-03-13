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
	ctx       context.Context
	cfg       *writer.LogWriterConfig
	ddlWriter writer.RedoLogWriter
	dmlWriter writer.RedoLogWriter

	logBuffer *chann.UnlimitedChannel[*redoLogTask, any]
	inflight  *inflightRowTracker

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	isClosed *atomic.Bool

	metric *redoSinkMetrics
}

type redoLogTask struct {
	event   writer.RedoEvent
	barrier *flushBarrier
}

func newRedoEventTask(event writer.RedoEvent) *redoLogTask {
	return &redoLogTask{event: event}
}

func newFlushBarrierTask() *redoLogTask {
	return &redoLogTask{barrier: newFlushBarrier()}
}

func (t *redoLogTask) isBarrier() bool {
	return t != nil && t.barrier != nil
}

type flushBarrier struct {
	done chan struct{}
}

type inflightRowTracker struct {
	mu      sync.Mutex
	rows    int
	drained chan struct{}
}

func newFlushBarrier() *flushBarrier {
	return &flushBarrier{
		done: make(chan struct{}),
	}
}

func newInflightRowTracker() *inflightRowTracker {
	drained := make(chan struct{})
	close(drained)
	return &inflightRowTracker{
		drained: drained,
	}
}

func (b *flushBarrier) finish() {
	close(b.done)
}

func (b *flushBarrier) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case <-b.done:
		return nil
	}
}

func (t *inflightRowTracker) add(count int) {
	if count <= 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.rows == 0 {
		t.drained = make(chan struct{})
	}
	t.rows += count
}

func (t *inflightRowTracker) done() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rows--
	if t.rows > 0 {
		return
	}
	if t.rows < 0 {
		t.rows = 0
		log.Warn("redo sink inflight rows regressed")
	}
	close(t.drained)
}

func (t *inflightRowTracker) waitZero(ctx context.Context) error {
	for {
		t.mu.Lock()
		if t.rows == 0 {
			t.mu.Unlock()
			return nil
		}
		drained := t.drained
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-drained:
		}
	}
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
) *Sink {
	s := &Sink{
		ctx: ctx,
		cfg: &writer.LogWriterConfig{
			ConsistentConfig:  *cfg,
			CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
			ChangeFeedID:      changefeedID,
			MaxLogSizeInBytes: util.GetOrZero(cfg.MaxLogSize) * redo.Megabyte,
		},
		logBuffer: chann.NewUnlimitedChannelDefault[*redoLogTask](),
		inflight:  newInflightRowTracker(),
		isNormal:  atomic.NewBool(true),
		isClosed:  atomic.NewBool(false),
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
	s.metric = newRedoSinkMetrics(changefeedID)
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

func (s *Sink) FlushDMLBeforeBlock(event commonEvent.BlockEvent) error {
	if event == nil {
		return nil
	}
	barrierTask := newFlushBarrierTask()
	s.logBuffer.Push(barrierTask)
	return barrierTask.barrier.wait(s.ctx)
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		start := time.Now()
		err := s.ddlWriter.WriteEvents(s.ctx, e)
		if err != nil {
			s.isNormal.Store(false)
			return errors.Trace(err)
		}
		if s.metric != nil {
			s.metric.observeDDLWrite(time.Since(start))
		}
	}
	return nil
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	rowsCount := event.Len()
	if rowsCount == 0 {
		event.PostEnqueue()
		event.PostFlush()
		return
	}

	tasks := make([]*redoLogTask, 0, rowsCount)
	rowCallback := helper.NewTxnPostFlushRowCallback(event, uint64(rowsCount))

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		tasks = append(tasks, newRedoEventTask(&commonEvent.RedoRowEvent{
			StartTs:         event.StartTs,
			CommitTs:        event.CommitTs,
			Event:           row,
			PhysicalTableID: event.PhysicalTableID,
			TableInfo:       event.TableInfo,
			Callback: func() {
				s.inflight.done()
				rowCallback()
			},
		}))
	}
	s.logBuffer.Push(tasks...)
	event.PostEnqueue()
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
	if s.metric != nil {
		s.metric.close()
	}
	log.Info("redo sink closed",
		zap.String("keyspace", s.cfg.ChangeFeedID.Keyspace()),
		zap.String("changefeed", s.cfg.ChangeFeedID.Name()))
}

func (s *Sink) sendMessages(ctx context.Context) error {
	taskBuffer := make([]*redoLogTask, 0, redo.DefaultFlushBatchSize)
	pendingTasks := make([]*redoLogTask, 0, redo.DefaultFlushBatchSize)
	eventBuffer := make([]writer.RedoEvent, 0, redo.DefaultFlushBatchSize)
	flushBufferedEvents := func() error {
		if len(eventBuffer) == 0 {
			return nil
		}

		s.inflight.add(len(eventBuffer))
		start := time.Now()
		err := s.dmlWriter.WriteEvents(ctx, eventBuffer...)
		if err != nil {
			return err
		}

		if s.metric != nil {
			s.metric.observeRowWrite(len(eventBuffer), time.Since(start))
		}
		eventBuffer = eventBuffer[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}
		var (
			tasks []*redoLogTask
			ok    bool
		)
		if len(pendingTasks) > 0 {
			tasks = pendingTasks
			pendingTasks = pendingTasks[:0]
			ok = true
		} else {
			tasks, ok = s.logBuffer.GetMultipleNoGroup(taskBuffer)
		}
		if !ok {
			return flushBufferedEvents()
		}
		if len(tasks) == 0 {
			continue
		}

		barrierIndex := -1
		for i, task := range tasks {
			if task.isBarrier() {
				barrierIndex = i
				break
			}
			eventBuffer = append(eventBuffer, task.event)
		}
		taskBuffer = tasks[:0]

		if barrierIndex >= 0 {
			if barrierIndex+1 < len(tasks) {
				pendingTasks = append(pendingTasks, tasks[barrierIndex+1:]...)
			}
			if err := flushBufferedEvents(); err != nil {
				return err
			}
			if err := s.inflight.waitZero(s.ctx); err != nil {
				return err
			}
			tasks[barrierIndex].barrier.finish()
			continue
		}

		if err := flushBufferedEvents(); err != nil {
			return err
		}
	}
}

func (s *Sink) AddCheckpointTs(_ uint64) {}
