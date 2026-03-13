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

	tasks *chann.UnlimitedChannel[*sinkTask, any]
	flush *flushTracker

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	isClosed *atomic.Bool

	metric *redoSinkMetrics
}

type sinkTask struct {
	dispatcherID common.DispatcherID
	seq          uint64
	event        writer.RedoEvent
	barrier      *barrierTask
}

func newRowTask(dispatcherID common.DispatcherID, seq uint64, event writer.RedoEvent) *sinkTask {
	return &sinkTask{
		dispatcherID: dispatcherID,
		seq:          seq,
		event:        event,
	}
}

func newBarrierTask(dispatcherID common.DispatcherID) *sinkTask {
	return &sinkTask{
		dispatcherID: dispatcherID,
		barrier:      newBarrier(dispatcherID),
	}
}

func (t *sinkTask) isBarrier() bool {
	return t != nil && t.barrier != nil
}

type barrierTask struct {
	dispatcherID common.DispatcherID
	target       uint64
	done         chan struct{}
}

type flushTracker struct {
	mu     sync.Mutex
	states map[common.DispatcherID]*dispatcherState
	err    error
	errCh  chan struct{}
}

type dispatcherState struct {
	mu         sync.Mutex
	nextSeq    uint64
	flushedSeq uint64
	pending    map[uint64]struct{}
	changed    chan struct{}
}

func newBarrier(dispatcherID common.DispatcherID) *barrierTask {
	return &barrierTask{
		dispatcherID: dispatcherID,
		done:         make(chan struct{}),
	}
}

func newFlushTracker() *flushTracker {
	return &flushTracker{
		states: make(map[common.DispatcherID]*dispatcherState),
		errCh:  make(chan struct{}),
	}
}

func newDispatcherState() *dispatcherState {
	return &dispatcherState{
		changed: make(chan struct{}),
	}
}

func (t *flushTracker) state(dispatcherID common.DispatcherID) *dispatcherState {
	t.mu.Lock()
	defer t.mu.Unlock()

	if state, ok := t.states[dispatcherID]; ok {
		return state
	}

	state := newDispatcherState()
	t.states[dispatcherID] = state
	return state
}

func (s *dispatcherState) nextRow() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextSeq++
	return s.nextSeq
}

func (s *dispatcherState) markFlushed(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seq <= s.flushedSeq {
		return
	}
	if seq == s.flushedSeq+1 {
		s.flushedSeq = seq
		for {
			nextSeq := s.flushedSeq + 1
			if _, ok := s.pending[nextSeq]; !ok {
				break
			}
			delete(s.pending, nextSeq)
			s.flushedSeq = nextSeq
		}
		close(s.changed)
		s.changed = make(chan struct{})
		return
	}
	if s.pending == nil {
		s.pending = make(map[uint64]struct{})
	}
	s.pending[seq] = struct{}{}
}

func (s *dispatcherState) wait(
	ctx context.Context,
	errCh <-chan struct{},
	loadErr func() error,
	target uint64,
) error {
	if target == 0 {
		return nil
	}

	for {
		s.mu.Lock()
		if s.flushedSeq >= target {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-errCh:
			return errors.Trace(loadErr())
		case <-changed:
		}
	}
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, cfg *config.ConsistentConfig) error {
	if cfg == nil || !redo.IsConsistentEnabled(util.GetOrZero(cfg.Level)) {
		return nil
	}
	return nil
}

func (b *barrierTask) finish(target uint64) {
	b.target = target
	close(b.done)
}

func (t *flushTracker) fail(err error) {
	if err == nil {
		err = context.Canceled
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.err != nil {
		return
	}
	t.err = err
	close(t.errCh)
}

func (t *flushTracker) loadErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.err == nil {
		return context.Canceled
	}
	return t.err
}

func (t *flushTracker) waitBarrier(
	ctx context.Context,
	state *dispatcherState,
	barrier *barrierTask,
) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case <-t.errCh:
		return errors.Trace(t.loadErr())
	case <-barrier.done:
	}

	return state.wait(ctx, t.errCh, t.loadErr, barrier.target)
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
		tasks:    chann.NewUnlimitedChannelDefault[*sinkTask](),
		flush:    newFlushTracker(),
		isNormal: atomic.NewBool(true),
		isClosed: atomic.NewBool(false),
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
		defer s.tasks.Close()
		err := s.dmlWriter.Run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled && errors.Cause(err) != context.DeadlineExceeded {
			s.flush.fail(err)
		}
		return err
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
	dispatcherID := event.GetDispatcherID()
	state := s.flush.state(dispatcherID)
	task := newBarrierTask(dispatcherID)
	s.tasks.Push(task)
	return s.flush.waitBarrier(s.ctx, state, task.barrier)
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

	dispatcherID := event.GetDispatcherID()
	state := s.flush.state(dispatcherID)
	tasks := make([]*sinkTask, 0, rowsCount)
	rowCallback := helper.NewTxnPostFlushRowCallback(event, uint64(rowsCount))

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		rowSeq := state.nextRow()
		tasks = append(tasks, newRowTask(dispatcherID, rowSeq, &commonEvent.RedoRowEvent{
			StartTs:         event.StartTs,
			CommitTs:        event.CommitTs,
			Event:           row,
			PhysicalTableID: event.PhysicalTableID,
			TableInfo:       event.TableInfo,
			Callback: func() {
				state.markFlushed(rowSeq)
				rowCallback()
			},
		}))
	}
	s.tasks.Push(tasks...)
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
	s.flush.fail(context.Canceled)
	s.tasks.Close()
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
	taskBuffer := make([]*sinkTask, 0, redo.DefaultFlushBatchSize)
	pendingTasks := make([]*sinkTask, 0, redo.DefaultFlushBatchSize)
	eventBuffer := make([]writer.RedoEvent, 0, redo.DefaultFlushBatchSize)
	deferredTasks := make([]*sinkTask, 0, redo.DefaultFlushBatchSize)
	lastSeenSeq := make(map[common.DispatcherID]uint64)
	writeRows := func(events []writer.RedoEvent) error {
		if len(events) == 0 {
			return nil
		}

		start := time.Now()
		err := s.dmlWriter.WriteEvents(ctx, events...)
		if err != nil {
			if errors.Cause(err) != context.Canceled && errors.Cause(err) != context.DeadlineExceeded {
				s.flush.fail(err)
			}
			return err
		}

		if s.metric != nil {
			s.metric.observeRowWrite(len(events), time.Since(start))
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}
		var (
			tasks []*sinkTask
			ok    bool
		)
		if len(pendingTasks) > 0 {
			tasks = pendingTasks
			pendingTasks = pendingTasks[:0]
			ok = true
		} else {
			tasks, ok = s.tasks.GetMultipleNoGroup(taskBuffer)
		}
		if !ok {
			return nil
		}
		if len(tasks) == 0 {
			continue
		}

		barrierIndex := -1
		barrierDispatcherID := common.DispatcherID{}
		eventBuffer = eventBuffer[:0]
		deferredTasks = deferredTasks[:0]
		for i, task := range tasks {
			if task.isBarrier() {
				barrierIndex = i
				barrierDispatcherID = task.dispatcherID
				break
			}
			lastSeenSeq[task.dispatcherID] = task.seq
			eventBuffer = append(eventBuffer, task.event)
		}
		taskBuffer = tasks[:0]

		if barrierIndex >= 0 {
			eventBuffer = eventBuffer[:0]
			for _, task := range tasks[:barrierIndex] {
				if task.dispatcherID == barrierDispatcherID {
					eventBuffer = append(eventBuffer, task.event)
					continue
				}
				deferredTasks = append(deferredTasks, task)
			}
			if barrierIndex+1 < len(tasks) {
				deferredTasks = append(deferredTasks, tasks[barrierIndex+1:]...)
			}

			if err := writeRows(eventBuffer); err != nil {
				return err
			}
			pendingTasks = append(pendingTasks[:0], deferredTasks...)
			tasks[barrierIndex].barrier.finish(lastSeenSeq[barrierDispatcherID])
			continue
		}

		if err := writeRows(eventBuffer); err != nil {
			return err
		}
	}
}

func (s *Sink) AddCheckpointTs(_ uint64) {}
