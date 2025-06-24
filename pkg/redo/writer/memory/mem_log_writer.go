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
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"go.uber.org/zap"
)

var _ writer.RedoLogWriter = (*memoryLogWriter)(nil)

type memoryLogWriter struct {
	cfg         *writer.LogWriterConfig
	fileWorkers *fileWorkerGroup
	fileType    string

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
	lw.fileWorkers = newFileWorkerGroup(cfg, cfg.FlushWorkerNum, fileType, extStorage, opts...)

	return lw, nil
}

func (l *memoryLogWriter) Run(ctx context.Context) error {
	newCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel
	return l.fileWorkers.Run(newCtx)
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
				zap.String("namespace", l.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		if err := l.fileWorkers.syncWrite(ctx, e); err != nil {
			return err
		}
	}
	return nil
}

func (l *memoryLogWriter) asyncWriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, e := range events {
		if e == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("namespace", l.cfg.ChangeFeedID.Namespace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		if err := l.fileWorkers.input(ctx, e); err != nil {
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
