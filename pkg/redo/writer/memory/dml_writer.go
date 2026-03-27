// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ writer.RedoDMLWriter = (*dmlWriter)(nil)

type dmlWriter struct {
	cfg           *writer.Config
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup

	cancel context.CancelFunc
}

// NewDMLWriter creates a new memory DML writer.
func NewDMLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDMLWriter, error) {
	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI())
	if err != nil {
		return nil, err
	}

	encodeWorkers := newEncodingWorkerGroup(cfg)
	fileWorkers := newFileWorkerGroup(
		cfg, encodeWorkers.outputCh, extStorage, opts...)

	return &dmlWriter{
		cfg:           cfg,
		encodeWorkers: encodeWorkers,
		fileWorkers:   fileWorkers,
	}, nil
}

func (l *dmlWriter) Run(ctx context.Context) error {
	newCtx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	eg, egCtx := errgroup.WithContext(newCtx)
	eg.Go(func() error {
		return l.encodeWorkers.Run(egCtx)
	})
	eg.Go(func() error {
		return l.fileWorkers.Run(egCtx)
	})
	return eg.Wait()
}

func (l *dmlWriter) AddDMLEvents(ctx context.Context, events ...*commonEvent.RedoRowEvent) error {
	for _, event := range events {
		if event == nil {
			log.Warn("writing nil event to redo log, ignore this",
				zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
				zap.String("changefeed", l.cfg.ChangeFeedID().Name()))
			continue
		}
		if err := l.encodeWorkers.AddEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (l *dmlWriter) Close() error {
	if l.cancel != nil {
		l.cancel()
	}
	return nil
}
