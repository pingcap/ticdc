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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
)

var _ writer.RedoDDLWriter = (*ddlWriter)(nil)

type ddlWriter struct {
	mu sync.Mutex

	cfg        *writer.Config
	op         *writer.LogWriterOptions
	extStorage storage.ExternalStorage
	uuidGen    uuid.Generator

	tableSchema *commonEvent.TableSchemaStore
	closed      bool
	writeMetric prometheus.Gauge
	flushMetric prometheus.Observer
}

// NewDDLWriter creates a new memory DDL writer.
func NewDDLWriter(
	ctx context.Context, cfg *writer.Config, opts ...writer.Option,
) (writer.RedoDDLWriter, error) {
	extStorage, err := redo.InitExternalStorage(ctx, *cfg.URI())
	if err != nil {
		return nil, err
	}

	op := &writer.LogWriterOptions{}
	for _, opt := range opts {
		opt(op)
	}

	uuidGen := uuid.NewGenerator()
	if op.GetUUIDGenerator != nil {
		uuidGen = op.GetUUIDGenerator()
	}

	return &ddlWriter{
		cfg:        cfg,
		op:         op,
		extStorage: extStorage,
		uuidGen:    uuidGen,
		writeMetric: metrics.RedoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID().Keyspace(), cfg.ChangeFeedID().Name(), redo.RedoDDLLogFileType),
		flushMetric: metrics.RedoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID().Keyspace(), cfg.ChangeFeedID().Name(), redo.RedoDDLLogFileType),
	}, nil
}

func (l *ddlWriter) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tableSchema = tableSchemaStore
}

func (l *ddlWriter) WriteDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	if event == nil {
		log.Warn("writing nil event to redo log, ignore this",
			zap.String("keyspace", l.cfg.ChangeFeedID().Keyspace()),
			zap.String("changefeed", l.cfg.ChangeFeedID().Name()))
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return errors.ErrRedoWriterStopped.GenWithStackByArgs()
	}

	redoLogEvent, err := toPolymorphicDDLEvent(event, l.tableSchema)
	if err != nil {
		return err
	}
	return l.write(ctx, redoLogEvent)
}

func (l *ddlWriter) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}
	l.closed = true
	l.closeMetrics()
	return nil
}

func (l *ddlWriter) write(ctx context.Context, event *polymorphicRedoEvent) error {
	if len(event.data) == 0 {
		return errors.ErrUnexpected.FastGenByArgs("encoded redo event data is empty")
	}

	writeLen := int64(len(event.data))
	if writeLen > l.cfg.MaxLogSizeInBytes() {
		return errors.ErrRedoFileSizeExceed.GenWithStackByArgs(writeLen, l.cfg.MaxLogSizeInBytes())
	}
	defer l.writeMetric.Add(float64(writeLen))

	start := time.Now()
	data, err := l.prepareWriteData(event.data)
	if err != nil {
		return err
	}
	fileName := l.getLogFileName(event.commitTs)
	if l.cfg.FlushConcurrency() <= 1 {
		err = l.extStorage.WriteFile(ctx, fileName, data)
	} else {
		err = l.multiPartUpload(ctx, fileName, data)
	}
	l.flushMetric.Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	event.PostFlush()
	return nil
}

func (l *ddlWriter) closeMetrics() {
	metrics.RedoFlushAllDurationHistogram.
		DeleteLabelValues(l.cfg.ChangeFeedID().Keyspace(), l.cfg.ChangeFeedID().Name(), redo.RedoDDLLogFileType)
	metrics.RedoWriteBytesGauge.
		DeleteLabelValues(l.cfg.ChangeFeedID().Keyspace(), l.cfg.ChangeFeedID().Name(), redo.RedoDDLLogFileType)
}

func (l *ddlWriter) multiPartUpload(ctx context.Context, fileName string, data []byte) error {
	multipartWrite, err := l.extStorage.Create(ctx, fileName, &storage.WriterOption{
		Concurrency: l.cfg.FlushConcurrency(),
	})
	if err != nil {
		return errors.Trace(err)
	}
	if _, err = multipartWrite.Write(ctx, data); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(multipartWrite.Close(ctx))
}

func (l *ddlWriter) prepareWriteData(data []byte) ([]byte, error) {
	if l.cfg.Compression() != compression.LZ4 {
		return data, nil
	}

	buf := bytes.NewBuffer(make([]byte, 0, len(data)))
	compressor := lz4.NewWriter(buf)
	if _, err := compressor.Write(data); err != nil {
		log.Error("write to new file failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	if err := compressor.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	return buf.Bytes(), nil
}

func (l *ddlWriter) getLogFileName(maxCommitTS common.Ts) string {
	if l.op != nil && l.op.GetLogFileName != nil {
		return l.op.GetLogFileName()
	}
	uid := l.uuidGen.NewString()
	if common.DefaultKeyspaceName == l.cfg.ChangeFeedID().Keyspace() {
		return fmt.Sprintf(redo.RedoLogFileFormatV1,
			l.cfg.CaptureID(), l.cfg.ChangeFeedID().Name(), redo.RedoDDLLogFileType,
			maxCommitTS, uid, redo.LogEXT)
	}
	return fmt.Sprintf(redo.RedoLogFileFormatV2,
		l.cfg.CaptureID(), l.cfg.ChangeFeedID().Keyspace(), l.cfg.ChangeFeedID().Name(),
		redo.RedoDDLLogFileType, maxCommitTS, uid, redo.LogEXT)
}

func toPolymorphicDDLEvent(
	event *commonEvent.DDLEvent,
	tableSchemaStore *commonEvent.TableSchemaStore,
) (*polymorphicRedoEvent, error) {
	rl := event.ToRedoLog()
	rl.RedoDDL.SetTableSchemaStore(tableSchemaStore)

	rawData, err := codec.MarshalRedoLog(rl, nil)
	if err != nil {
		return nil, err
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
