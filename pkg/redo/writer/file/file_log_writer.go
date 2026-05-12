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

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
)

var (
	_ writer.RedoDMLWriter = (*dmlWriter)(nil)
	_ writer.RedoDDLWriter = (*ddlWriter)(nil)
)

type logWriter struct {
	cfg           *writer.Config
	backendWriter fileWriter
}

type dmlWriter struct {
	*logWriter
}

type ddlWriter struct {
	*logWriter
}

func newLogWriter(
	ctx context.Context, cfg *writer.Config, fileType string, opts ...writer.Option,
) (l *logWriter, err error) {
	l = &logWriter{cfg: cfg}
	if l.backendWriter, err = NewFileWriter(ctx, cfg, fileType, opts...); err != nil {
		return nil, err
	}
	return
}

// NewDMLWriter creates a new file DML writer.
func NewDMLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDMLWriter, error) {
	l, err := newLogWriter(ctx, cfg, redo.RedoRowLogFileType, opts...)
	if err != nil {
		return nil, err
	}
	return &dmlWriter{logWriter: l}, nil
}

// NewDDLWriter creates a new file DDL writer.
func NewDDLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDDLWriter, error) {
	l, err := newLogWriter(ctx, cfg, redo.RedoDDLLogFileType, opts...)
	if err != nil {
		return nil, err
	}
	return &ddlWriter{logWriter: l}, nil
}

func (l *logWriter) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	l.backendWriter.SetTableSchemaStore(tableSchemaStore)
}

func (l *logWriter) Run(ctx context.Context) error {
	return l.backendWriter.Run(ctx)
}

func (l *dmlWriter) AddDMLEvents(ctx context.Context, events ...*commonEvent.RedoRowEvent) error {
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID().Name()),
				zap.String("capture", l.cfg.CaptureID()))
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

func (l *ddlWriter) WriteDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if l.isStopped() {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}
	if event == nil {
		log.Warn("writing nil event to redo log, ignore this",
			zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
			zap.String("changefeed", l.cfg.ChangeFeedID().Name()),
			zap.String("capture", l.cfg.CaptureID()))
		return nil
	}
	if err := l.backendWriter.SyncWrite(event); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (l *logWriter) Close() (err error) {
	return l.backendWriter.Close()
}

func (l *logWriter) isStopped() bool {
	return !l.backendWriter.IsRunning()
}
