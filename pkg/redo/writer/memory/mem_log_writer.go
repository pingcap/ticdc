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
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ writer.RedoLogWriter = (*memoryLogWriter)(nil)

type memoryLogWriter struct {
	cfg           *writer.LogWriterConfig
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup
	fileType      string
	tableSchema   *event.TableSchemaStore

	cancel context.CancelFunc
}

// NewLogWriter creates a new memoryLogWriter.
func NewLogWriter(
	ctx context.Context, cfg *writer.LogWriterConfig, fileType string, opts ...writer.Option,
) (*memoryLogWriter, error) {
	if cfg == nil {
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid,
			errors.New("invalid LogWriterConfig"))
	}

	// "nfs" and "local" scheme are converted to "file" scheme
	if !cfg.UseExternalStorage {
		redo.FixLocalScheme(cfg.URI)
		cfg.UseExternalStorage = redo.IsExternalStorage(cfg.URI.Scheme)
	}

	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI)
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
		cfg, util.GetOrZero(cfg.FlushWorkerNum), fileType, fileInputCh, extStorage, opts...)

	return lw, nil
}

func (l *memoryLogWriter) SetTableSchemaStore(tableSchemaStore *event.TableSchemaStore) {
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

func (l *memoryLogWriter) WriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	if l.fileType == redo.RedoDDLLogFileType {
		return l.writeEvents(ctx, events...)
	}
	return l.asyncWriteEvents(ctx, events...)
}

func (l *memoryLogWriter) writeEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, e := range events {
		if e == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		redoLogEvent, err := toPolymorphicRedoEvent(e, l.tableSchema)
		if err != nil {
			return err
		}
		if err := encodeData(redoLogEvent); err != nil {
			return err
		}
		if err := l.fileWorkers.syncWrite(ctx, redoLogEvent); err != nil {
			return err
		}
	}
	return nil
}

func (l *memoryLogWriter) asyncWriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, e := range events {
		if e == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
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

func (l *memoryLogWriter) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	return nil
}
