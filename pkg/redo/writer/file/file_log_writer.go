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

package file

import (
	"context"
	"path/filepath"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
)

var _ writer.RedoLogWriter = &logWriter{}

type logWriter struct {
	cfg           *writer.LogWriterConfig
	backendWriter fileWriter
	fileType      string
}

// NewLogWriter create a new logWriter.
func NewLogWriter(
	ctx context.Context, cfg *writer.LogWriterConfig, fileType string, opts ...writer.Option,
) (l *logWriter, err error) {
	if cfg == nil {
		err := errors.New("LogWriterConfig can not be nil")
		return nil, errors.WrapError(errors.ErrRedoConfigInvalid, err)
	}

	if cfg.UseExternalStorage {
		// When an external storage is used, we use redoDir as a temporary dir to store redo logs
		// before we flush them to S3.
		changeFeedID := cfg.ChangeFeedID
		dataDir := config.GetGlobalServerConfig().DataDir
		cfg.Dir = filepath.Join(dataDir, config.DefaultRedoDir,
			changeFeedID.Keyspace(), changeFeedID.Name())
	} else {
		// When local storage or NFS is used, we use redoDir as the final storage path.
		cfg.Dir = cfg.URI.Path
	}

	l = &logWriter{cfg: cfg, fileType: fileType}
	if l.backendWriter, err = NewFileWriter(ctx, cfg, fileType, opts...); err != nil {
		return nil, err
	}
	return
}

func (l *logWriter) SetTableSchemaStore(tableSchemaStore *event.TableSchemaStore) {
	l.backendWriter.SetTableSchemaStore(tableSchemaStore)
}

func (l *logWriter) Run(ctx context.Context) error {
	return l.backendWriter.Run(ctx)
}

func (l *logWriter) WriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	if l.fileType == redo.RedoDDLLogFileType {
		return l.writeEvents(ctx, events...)
	}
	return l.asyncWriteEvents(ctx, events...)
}

func (l *logWriter) writeEvents(ctx context.Context, events ...writer.RedoEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		if err := l.backendWriter.SyncWrite(event); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (l *logWriter) asyncWriteEvents(ctx context.Context, events ...writer.RedoEvent) error {
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID.Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID.Name()),
				zap.String("capture", l.cfg.CaptureID))
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case l.backendWriter.GetInputCh() <- event:
		}
	}
	return nil
}

// Close implements RedoLogWriter.Close.
func (l *logWriter) Close() (err error) {
	return l.backendWriter.Close()
}

func (l *logWriter) isStopped() bool {
	return !l.backendWriter.IsRunning()
}
