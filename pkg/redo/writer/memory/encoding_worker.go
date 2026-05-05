// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package memory

import (
	"context"
	"encoding/binary"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/codec"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// polymorphicRedoEvent wraps RedoLog and callback for file worker.
type polymorphicRedoEvent struct {
	commitTs common.Ts
	data     []byte
	callback func()
}

func (e *polymorphicRedoEvent) PostFlush() {
	if e.callback != nil {
		e.callback()
	}
}

func toPolymorphicDMLEvent(
	event *commonEvent.RedoRowEvent,
) (*polymorphicRedoEvent, error) {
	rl := event.ToRedoLog()

	rawData, err := codec.MarshalRedoLog(rl, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}
	lenField, padBytes := writer.EncodeFrameSize(len(rawData))
	data := make([]byte, 8+len(rawData)+padBytes)
	binary.LittleEndian.PutUint64(data[:8], lenField)
	copy(data[8:], rawData)
	return &polymorphicRedoEvent{
		commitTs: rl.GetCommitTs(),
		callback: event.PostFlush,
		data:     data,
	}, nil
}

type encodingWorkerGroup struct {
	changefeed common.ChangeFeedID

	outputCh  chan *polymorphicRedoEvent
	inputChs  []chan *commonEvent.RedoRowEvent
	workerNum int

	nextWorker atomic.Uint64
	closed     chan error
}

func newEncodingWorkerGroup(cfg *writer.Config) *encodingWorkerGroup {
	workerNum := cfg.EncodingWorkerNum()
	if workerNum <= 0 {
		workerNum = redo.DefaultEncodingWorkerNum
	}
	inputChs := make([]chan *commonEvent.RedoRowEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		inputChs[i] = make(chan *commonEvent.RedoRowEvent, redo.DefaultEncodingInputChanSize)
	}
	return &encodingWorkerGroup{
		changefeed: cfg.ChangeFeedID(),
		inputChs:   inputChs,
		outputCh:   make(chan *polymorphicRedoEvent, redo.DefaultEncodingOutputChanSize),
		workerNum:  workerNum,
		closed:     make(chan error, 1),
	}
}

func (e *encodingWorkerGroup) Run(ctx context.Context) (err error) {
	defer func() {
		log.Warn("redo encoding workers closed",
			zap.String("keyspace", e.changefeed.Keyspace()),
			zap.String("changefeed", e.changefeed.Name()),
			zap.Error(err))
		if err != nil {
			select {
			case e.closed <- err:
			default:
			}
		}
		close(e.closed)
	}()
	g, egCtx := errgroup.WithContext(ctx)
	for i := 0; i < e.workerNum; i++ {
		idx := i
		g.Go(func() error {
			return e.runWorker(egCtx, idx)
		})
	}
	log.Info("redo log encoding workers started",
		zap.String("keyspace", e.changefeed.Keyspace()),
		zap.String("changefeed", e.changefeed.Name()),
		zap.Int("workerNum", e.workerNum))
	return g.Wait()
}

func (e *encodingWorkerGroup) AddEvent(ctx context.Context, event *commonEvent.RedoRowEvent) error {
	idx := int((e.nextWorker.Inc() - 1) % uint64(e.workerNum))
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case err := <-e.closed:
		return errors.ErrRedoWriterStopped.FastGenByArgs(err)
	case e.inputChs[idx] <- event:
	}
	return nil
}

func (e *encodingWorkerGroup) runWorker(ctx context.Context, idx int) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case event := <-e.inputChs[idx]:
			if event == nil {
				log.Warn("received nil event in redo encoding worker",
					zap.String("keyspace", e.changefeed.Keyspace()),
					zap.String("changefeed", e.changefeed.Name()))
				continue
			}
			redoLogEvent, err := toPolymorphicDMLEvent(event)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return errors.Trace(context.Cause(ctx))
			case err := <-e.closed:
				return errors.ErrRedoWriterStopped.FastGenByArgs(err)
			case e.outputCh <- redoLogEvent:
			}
		}
	}
}
