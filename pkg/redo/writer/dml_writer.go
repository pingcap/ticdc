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

package writer

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ RedoDMLWriter = (*dmlWriter)(nil)

type dmlWriter struct {
	cfg           *Config
	encodeWorkers *encodingWorkerGroup
	fileWorkers   *fileWorkerGroup
	spool         *spool.Spool
	spoolEntries  *chann.UnlimitedChannel[*redoSpoolEntry, any]
	extStorage    storeapi.Storage
	cancel        context.CancelFunc
}

type redoSpoolEntry struct {
	entry            *spool.Entry
	flushImmediately bool
}

const redoSpoolDirectory = "redo-sink-spool"

// NewDMLWriter creates a new redo DML writer.
func NewDMLWriter(
	ctx context.Context, cfg *Config, opts ...Option,
) (RedoDMLWriter, error) {
	uri := cfg.URI()
	if redo.IsBlackholeStorage(uri.Scheme) {
		return newBlackHoleDMLWriter(strings.HasSuffix(uri.Scheme, "invalid")), nil
	}
	return newDMLWriter(ctx, cfg, opts...)
}

func newDMLWriter(
	ctx context.Context, cfg *Config, opts ...Option,
) (RedoDMLWriter, error) {
	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI())
	if err != nil {
		return nil, err
	}

	encodeWorkers := newEncodingWorkerGroup(cfg)
	fileWorkerInput := make(chan *polymorphicRedoEvent, redo.DefaultEncodingOutputChanSize)
	fileWorkers := newFileWorkerGroup(
		cfg, fileWorkerInput, extStorage, opts...)
	spoolBaseDir := cfg.SpoolBaseDir()
	if spoolBaseDir == "" {
		spoolBaseDir = config.GetGlobalServerConfig().DataDir
		if spoolBaseDir == "" {
			spoolBaseDir = os.TempDir()
		}
		spoolBaseDir = filepath.Join(spoolBaseDir, redoSpoolDirectory)
	}
	spoolBuffer, err := spool.New(
		cfg.ChangeFeedID(),
		spool.WithRootDir(spoolBaseDir),
		spool.WithDiskQuotaBytes(cfg.SpoolDiskQuota()),
	)
	if err != nil {
		extStorage.Close()
		return nil, err
	}

	return &dmlWriter{
		cfg:           cfg,
		encodeWorkers: encodeWorkers,
		fileWorkers:   fileWorkers,
		spool:         spoolBuffer,
		spoolEntries:  chann.NewUnlimitedChannelDefault[*redoSpoolEntry](),
		extStorage:    extStorage,
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
		return l.writeEncodedEventsToSpool(egCtx)
	})
	eg.Go(func() error {
		return l.readEncodedEventsFromSpool(egCtx)
	})
	eg.Go(func() error {
		return l.fileWorkers.Run(egCtx)
	})
	return eg.Wait()
}

func (l *dmlWriter) writeEncodedEventsToSpool(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case event := <-l.encodeWorkers.outputCh:
			if event == nil {
				return errors.ErrUnexpected.FastGenByArgs("encoded redo event is nil")
			}
			key := make([]byte, 8)
			binary.LittleEndian.PutUint64(key, event.commitTs)
			msg := common.NewMsg(key, event.data)
			msg.Callback = event.postFlush

			for {
				action, entry, err := l.spool.TryEnqueue(
					[]*common.Message{msg}, event.postEnqueue)
				if err != nil {
					return err
				}
				if action == spool.EnqueueActionWaitDiskQuota {
					if err := l.spool.WaitForDiskQuota(ctx, []*common.Message{msg}); err != nil {
						return err
					}
					continue
				}
				l.spoolEntries.Push(&redoSpoolEntry{
					entry:            entry,
					flushImmediately: action == spool.EnqueueActionAcceptedOversized,
				})
				break
			}
		}
	}
}

func (l *dmlWriter) readEncodedEventsFromSpool(ctx context.Context) error {
	for {
		spooled, ok, err := l.spoolEntries.GetWithContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		entry := spooled.entry
		reader, err := l.spool.NewMessageReader(entry)
		if err != nil {
			return err
		}
		key, data, _, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok || len(key) != 8 || len(data) == 0 {
			return errors.ErrUnexpected.FastGenByArgs("invalid encoded redo spool entry")
		}
		_, _, _, hasMore, err := reader.Next()
		if err != nil {
			return err
		}
		if hasMore {
			return errors.ErrUnexpected.FastGenByArgs("encoded redo spool entry contains multiple messages")
		}
		postFlushCallbacks := reader.PostFlushCallbacks()
		encodedEvent := &polymorphicRedoEvent{
			commitTs:         binary.LittleEndian.Uint64(key),
			data:             data,
			flushImmediately: spooled.flushImmediately,
			postFlush: func() {
				for _, callback := range postFlushCallbacks {
					callback()
				}
				l.spool.Release(entry)
			},
		}
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case l.fileWorkers.inputCh <- encodedEvent:
		}
	}
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
		l.cancel = nil
	}
	if l.extStorage != nil {
		l.extStorage.Close()
		l.extStorage = nil
	}
	if l.spool != nil {
		l.spool.Close()
		l.spool = nil
	}
	return nil
}
