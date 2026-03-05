//  Copyright 2026 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
	"github.com/pingcap/ticdc/pkg/util"
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

func toPolymorphicRedoEvent(
	event writer.RedoEvent,
	tableSchemaStore *commonEvent.TableSchemaStore,
) (*polymorphicRedoEvent, error) {
	rl := event.ToRedoLog()
	if rl == nil {
		return nil, errors.ErrUnexpected.FastGenByArgs("redo event to log conversion failed")
	}
	if rl.Type == commonEvent.RedoLogTypeDDL {
		rl.RedoDDL.SetTableSchemaStore(tableSchemaStore)
	}

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
	inputChs  []chan writer.RedoEvent
	workerNum int

	nextWorker atomic.Uint64
	closed     chan error

	tableSchemaStore *commonEvent.TableSchemaStore
}

func newEncodingWorkerGroup(cfg *writer.LogWriterConfig) *encodingWorkerGroup {
	workerNum := util.GetOrZero(cfg.EncodingWorkerNum)
	if workerNum <= 0 {
		workerNum = redo.DefaultEncodingWorkerNum
	}
	inputChs := make([]chan writer.RedoEvent, workerNum)
	for i := 0; i < workerNum; i++ {
		inputChs[i] = make(chan writer.RedoEvent, redo.DefaultEncodingInputChanSize)
	}
	return &encodingWorkerGroup{
		changefeed: cfg.ChangeFeedID,
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

func (e *encodingWorkerGroup) AddEvent(ctx context.Context, event writer.RedoEvent) error {
	idx := int((e.nextWorker.Inc() - 1) % uint64(e.workerNum))
	return e.input(ctx, idx, event)
}

func (e *encodingWorkerGroup) runWorker(egCtx context.Context, idx int) error {
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case event := <-e.inputChs[idx]:
			if event == nil {
				log.Warn("received nil event in redo encoding worker",
					zap.String("keyspace", e.changefeed.Keyspace()),
					zap.String("changefeed", e.changefeed.Name()))
				continue
			}
			redoLogEvent, err := toPolymorphicRedoEvent(event, e.tableSchemaStore)
			if err != nil {
				return errors.Trace(err)
			}
			if err := e.output(egCtx, redoLogEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (e *encodingWorkerGroup) input(
	ctx context.Context, idx int, event writer.RedoEvent,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-e.closed:
		return errors.ErrRedoWriterStopped.FastGenByArgs(err)
	case e.inputChs[idx] <- event:
		return nil
	}
}

func (e *encodingWorkerGroup) output(
	ctx context.Context, event *polymorphicRedoEvent,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-e.closed:
		return errors.ErrRedoWriterStopped.FastGenByArgs(err)
	case e.outputCh <- event:
		return nil
	}
}
