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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/codec"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type fileCache struct {
	data        []byte
	fileSize    int64
	maxCommitTs common.Ts
	// After memoryWriter become stable, this field would be used to
	// avoid traversing log files.
	minCommitTs common.Ts

	filename string
	flushed  chan struct{}
	writer   *dataWriter
}

type dataWriter struct {
	buf    *bytes.Buffer
	writer io.Writer
	closer io.Closer
}

func (w *dataWriter) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

func (w *dataWriter) Close() error {
	if w.closer != nil {
		return w.closer.Close()
	}
	return nil
}

func (f *fileCache) waitFlushed(ctx context.Context) error {
	if f.flushed != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.flushed:
		}
	}
	return nil
}

func (f *fileCache) markFlushed() {
	if f.flushed != nil {
		close(f.flushed)
	}
}

type fileWorkerGroup struct {
	cfg           *writer.LogWriterConfig
	logType       string
	op            *writer.LogWriterOptions
	workerNum     int
	inputCh       chan writer.RedoEvent
	extStorage    storage.ExternalStorage
	uuidGenerator uuid.Generator

	pool    sync.Pool
	files   []*fileCache
	flushCh chan *fileCache

	metricWriteBytes       prometheus.Gauge
	metricFlushAllDuration prometheus.Observer
	tableSchemaStore       *commonEvent.TableSchemaStore
}

// newFileWorkerGroup create a fileWorkerGroup
// fileWorkerGroup received RedoEvents and wrote them to the cache. It also utilizes background goroutines for flushing.
func newFileWorkerGroup(
	cfg *writer.LogWriterConfig, workerNum int,
	logType string,
	extStorage storage.ExternalStorage,
	opts ...writer.Option,
) *fileWorkerGroup {
	if workerNum <= 0 {
		workerNum = redo.DefaultFlushWorkerNum
	}

	op := &writer.LogWriterOptions{}
	for _, opt := range opts {
		opt(op)
	}

	return &fileWorkerGroup{
		cfg:           cfg,
		logType:       logType,
		op:            op,
		workerNum:     workerNum,
		inputCh:       make(chan writer.RedoEvent, redo.DefaultEncodingInputChanSize*workerNum),
		extStorage:    extStorage,
		uuidGenerator: uuid.NewGenerator(),
		pool: sync.Pool{
			New: func() interface{} {
				// Use pointer here to prevent static checkers from reporting errors.
				// Ref: https://github.com/dominikh/go-tools/issues/1336.
				buf := make([]byte, 0, cfg.MaxLogSizeInBytes)
				return &buf
			},
		},
		flushCh: make(chan *fileCache, 32),
		metricWriteBytes: metrics.RedoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID.Keyspace(), cfg.ChangeFeedID.Name(), logType),
		metricFlushAllDuration: metrics.RedoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Keyspace(), cfg.ChangeFeedID.Name(), logType),
	}
}

func (f *fileWorkerGroup) Run(
	ctx context.Context,
) (err error) {
	defer func() {
		f.close()
		log.Warn("redo file workers closed",
			zap.String("keyspace", f.cfg.ChangeFeedID.Keyspace()),
			zap.String("changefeed", f.cfg.ChangeFeedID.Name()),
			zap.Error(err))
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return f.bgWriteLogs(egCtx, f.inputCh)
	})
	for i := 0; i < f.workerNum; i++ {
		eg.Go(func() error {
			return f.bgFlushFileCache(egCtx)
		})
	}
	log.Info("redo file workers started",
		zap.String("keyspace", f.cfg.ChangeFeedID.Keyspace()),
		zap.String("changefeed", f.cfg.ChangeFeedID.Name()),
		zap.Int("workerNum", f.workerNum))
	return eg.Wait()
}

func (f *fileWorkerGroup) close() {
	metrics.RedoFlushAllDurationHistogram.
		DeleteLabelValues(f.cfg.ChangeFeedID.Keyspace(), f.cfg.ChangeFeedID.Name(), f.logType)
	metrics.RedoWriteBytesGauge.
		DeleteLabelValues(f.cfg.ChangeFeedID.Keyspace(), f.cfg.ChangeFeedID.Name(), f.logType)
}

func (f *fileWorkerGroup) input(ctx context.Context, event writer.RedoEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.inputCh <- event:
	}
	return nil
}

func (f *fileWorkerGroup) bgFlushFileCache(egCtx context.Context) error {
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case file := <-f.flushCh:
			err := f.syncWriteFile(egCtx, file)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (f *fileWorkerGroup) multiPartUpload(ctx context.Context, file *fileCache) error {
	multipartWrite, err := f.extStorage.Create(ctx, file.filename, &storage.WriterOption{
		Concurrency: f.cfg.FlushConcurrency,
	})
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = multipartWrite.Write(ctx, file.writer.buf.Bytes()); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(multipartWrite.Close(ctx))
}

func (f *fileWorkerGroup) bgWriteLogs(
	egCtx context.Context, inputCh <-chan writer.RedoEvent,
) (err error) {
	d := time.Duration(f.cfg.FlushIntervalInMs) * time.Millisecond
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	num := 0
	cacheEventPostFlush := make([]func(), 0, redo.DefaultFlushBatchSize)
	flush := func() error {
		err := f.flushAll(egCtx)
		if err != nil {
			return err
		}
		for _, fn := range cacheEventPostFlush {
			fn()
		}
		num = 0
		cacheEventPostFlush = cacheEventPostFlush[:0]
		return nil
	}
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case <-ticker.C:
			err := flush()
			if err != nil {
				return errors.Trace(err)
			}
		case event := <-inputCh:
			if event == nil {
				log.Error("inputCh of redo file worker is closed unexpectedly")
				return errors.ErrUnexpected.FastGenByArgs("inputCh of redo file worker is closed unexpectedly")
			}
			err := f.writeToCache(egCtx, event)
			if err != nil {
				return errors.Trace(err)
			}
			num++
			if num > redo.DefaultFlushBatchSize {
				err := flush()
				if err != nil {
					return errors.Trace(err)
				}
				event.PostFlush()
			} else {
				cacheEventPostFlush = append(cacheEventPostFlush, event.PostFlush)
			}
		}
	}
}

func (f *fileWorkerGroup) syncWrite(egCtx context.Context, event writer.RedoEvent) error {
	rl, data, err := f.encodeData(event)
	if err != nil {
		return err
	}
	file := f.newFileCache(data, rl.GetCommitTs())
	f.syncWriteFile(egCtx, file)
	// flush
	event.PostFlush()
	return nil
}

func (f *fileWorkerGroup) syncWriteFile(egCtx context.Context, file *fileCache) error {
	var err error
	start := time.Now()
	file.filename = f.getLogFileName(file.maxCommitTs)
	if err = file.writer.Close(); err != nil {
		return err
	}
	if f.cfg.FlushConcurrency <= 1 {
		err = f.extStorage.WriteFile(egCtx, file.filename, file.writer.buf.Bytes())
	} else {
		err = f.multiPartUpload(egCtx, file)
	}
	f.metricFlushAllDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	file.markFlushed()

	bufPtr := &file.data
	file.data = nil
	f.pool.Put(bufPtr)
	return nil
}

// newFileCache write event to a new file cache.
func (f *fileWorkerGroup) newFileCache(data []byte, commitTs common.Ts) *fileCache {
	bufPtr := f.pool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]
	var (
		wr     io.Writer
		closer io.Closer
	)
	bufferWriter := bytes.NewBuffer(buf)
	wr = bufferWriter
	if f.cfg.Compression == compression.LZ4 {
		wr = lz4.NewWriter(bufferWriter)
		closer = wr.(io.Closer)
	}
	_, err := wr.Write(data)
	if err != nil {
		log.Error("write to new file failed", zap.Error(err))
		return nil
	}

	dw := &dataWriter{
		buf:    bufferWriter,
		writer: wr,
		closer: closer,
	}
	return &fileCache{
		data:        buf,
		fileSize:    int64(len(data)),
		maxCommitTs: commitTs,
		minCommitTs: commitTs,
		flushed:     make(chan struct{}),
		writer:      dw,
	}
}

func (f *fileWorkerGroup) encodeData(event writer.RedoEvent) (*commonEvent.RedoLog, []byte, error) {
	rl := event.ToRedoLog()
	if rl.Type == commonEvent.RedoLogTypeDDL {
		rl.RedoDDL.SetTableSchemaStore(f.tableSchemaStore)
	}
	rawData, err := codec.MarshalRedoLog(rl, nil)
	if err != nil {
		return nil, nil, err
	}
	uint64buf := make([]byte, 8)
	lenField, padBytes := writer.EncodeFrameSize(len(rawData))
	binary.LittleEndian.PutUint64(uint64buf, lenField)
	data := append(uint64buf, rawData...)
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	return rl, data, nil
}

// encoding format: lenField(8 bytes) + rawData + padding bytes(force 8 bytes alignment)
func (f *fileWorkerGroup) writeToCache(
	egCtx context.Context, event writer.RedoEvent,
) (err error) {
	rl, data, err := f.encodeData(event)
	if err != nil {
		return err
	}
	writeLen := int64(len(data))
	if writeLen > f.cfg.MaxLogSizeInBytes {
		// TODO: maybe we need to deal with the oversized commonEvent.
		return errors.ErrRedoFileSizeExceed.GenWithStackByArgs(writeLen, f.cfg.MaxLogSizeInBytes)
	}
	defer f.metricWriteBytes.Add(float64(writeLen))

	if len(f.files) == 0 {
		file := f.newFileCache(data, rl.GetCommitTs())
		f.files = append(f.files, file)
		return nil
	}

	file := f.files[len(f.files)-1]
	if file.fileSize+writeLen > f.cfg.MaxLogSizeInBytes {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case f.flushCh <- file:
		}
		file := f.newFileCache(data, rl.GetCommitTs())
		f.files = append(f.files, file)
		return nil
	}

	_, err = file.writer.Write(data)
	if err != nil {
		return err
	}

	file.fileSize += writeLen
	commitTs := rl.GetCommitTs()
	if commitTs > file.maxCommitTs {
		file.maxCommitTs = commitTs
	}
	if commitTs < file.minCommitTs {
		file.minCommitTs = commitTs
	}
	return nil
}

func (f *fileWorkerGroup) flushAll(egCtx context.Context) error {
	if len(f.files) == 0 {
		return nil
	}

	file := f.files[len(f.files)-1]
	select {
	case <-egCtx.Done():
		return errors.Trace(egCtx.Err())
	case f.flushCh <- file:
	}

	// wait all files flushed
	for _, file := range f.files {
		err := file.waitFlushed(egCtx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	f.files = f.files[:0]
	return nil
}

func (f *fileWorkerGroup) getLogFileName(maxCommitTS common.Ts) string {
	if f.op != nil && f.op.GetLogFileName != nil {
		return f.op.GetLogFileName()
	}
	uid := f.uuidGenerator.NewString()
	if common.DefaultKeyspaceNamme == f.cfg.ChangeFeedID.Keyspace() {
		return fmt.Sprintf(redo.RedoLogFileFormatV1,
			f.cfg.CaptureID, f.cfg.ChangeFeedID.Name(), f.logType,
			maxCommitTS, uid, redo.LogEXT)
	}
	return fmt.Sprintf(redo.RedoLogFileFormatV2,
		f.cfg.CaptureID, f.cfg.ChangeFeedID.Keyspace(), f.cfg.ChangeFeedID.Name(),
		f.logType, maxCommitTS, uid, redo.LogEXT)
}
