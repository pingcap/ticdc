//  Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	_ writer.RedoDMLWriter = (*dmlWriter)(nil)
	_ writer.RedoDDLWriter = (*ddlWriter)(nil)
)

type memoryLogWriter struct {
	cfg           *writer.Config
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup
	fileType      string
	tableSchema   *commonEvent.TableSchemaStore

	cancel context.CancelFunc
}

type dmlWriter struct {
	*memoryLogWriter
}

type ddlWriter struct {
	*memoryLogWriter
}

func newLogWriter(
	ctx context.Context, cfg *writer.Config, fileType string, opts ...writer.Option,
) (*memoryLogWriter, error) {
	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI())
	if err != nil {
		return nil, err
	}

	lw := &memoryLogWriter{
		cfg:      cfg,
		fileType: fileType,
	}
	var fileInputCh chan *polymorphicRedoEvent
	if fileType == redo.RedoRowLogFileType {
		lw.encodeWorkers = newEncodingWorkerGroup(cfg)
		fileInputCh = lw.encodeWorkers.outputCh
	}
	lw.fileWorkers = newFileWorkerGroup(
		cfg, cfg.FlushWorkerNum(), fileType, fileInputCh, extStorage, opts...)

	return lw, nil
}

// NewDMLWriter creates a new memory DML writer.
func NewDMLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDMLWriter, error) {
	lw, err := newLogWriter(ctx, cfg, redo.RedoRowLogFileType, opts...)
	if err != nil {
		return nil, err
	}
	return &dmlWriter{memoryLogWriter: lw}, nil
}

// NewDDLWriter creates a new memory DDL writer.
func NewDDLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDDLWriter, error) {
	lw, err := newLogWriter(ctx, cfg, redo.RedoDDLLogFileType, opts...)
	if err != nil {
		return nil, err
	}
	return &ddlWriter{memoryLogWriter: lw}, nil
}

func (l *memoryLogWriter) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	l.tableSchema = tableSchemaStore
	if l.encodeWorkers != nil {
		l.encodeWorkers.tableSchemaStore = tableSchemaStore
	}
}

func (l *memoryLogWriter) Run(ctx context.Context) error {
	newCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	if l.encodeWorkers == nil {
		return l.fileWorkers.Run(newCtx)
	}
	eg, egCtx := errgroup.WithContext(newCtx)
	eg.Go(func() error {
		return l.encodeWorkers.Run(egCtx)
	})
	eg.Go(func() error {
		return l.fileWorkers.Run(egCtx)
	})
	return eg.Wait()
}

func (l *dmlWriter) AppendDMLEvents(ctx context.Context, events ...*commonEvent.RedoRowEvent) error {
	for _, e := range events {
		if e == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID().Name()))
			continue
		}
		if l.encodeWorkers == nil {
			return errors.ErrRedoWriterStopped.GenWithStackByArgs()
		}
		if err := l.encodeWorkers.AddEvent(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (l *ddlWriter) WriteDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	if event == nil {
		log.Warn("writing nil event to redo log, ignore this",
			zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
			zap.String("changefeed", l.cfg.ChangeFeedID().Name()))
		return nil
	}
	redoLogEvent, err := toPolymorphicRedoEvent(event, l.tableSchema)
	if err != nil {
		return err
	}
	// todo: this should be simplified to a single writer.
	return l.fileWorkers.syncWrite(ctx, redoLogEvent)
}

func (l *memoryLogWriter) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	return nil
}
