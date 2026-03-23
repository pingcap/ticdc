// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"bytes"
	"context"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	pmetrics "github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type writer struct {
	shardID       int
	changeFeedID  common.ChangeFeedID
	storage       storage.ExternalStorage
	config        *cloudstorage.Config
	spool         *spool.Spool
	bufferManager *bufferManager

	toBeFlushedCh chan writerTask

	statistics        *pmetrics.Statistics
	filePathGenerator *cloudstorage.FilePathGenerator

	metricWriteBytes       prometheus.Gauge
	metricFileCount        prometheus.Gauge
	metricWriteDuration    prometheus.Observer
	metricFlushDuration    prometheus.Observer
	metricsWorkerBusyRatio prometheus.Counter
	writerLabel            string
}

// writerTask is internal and never crosses component boundary.
// marker task and data batch are mutually exclusive in normal flow.
type writerTask struct {
	batch  batchedTask
	marker *flushMarker
}

func newWriter(
	id int,
	changefeedID common.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	statistics *pmetrics.Statistics,
	spoolBuffer *spool.Spool,
) *writer {
	flushCh := make(chan writerTask, 64)
	d := &writer{
		shardID:       id,
		changeFeedID:  changefeedID,
		storage:       storage,
		config:        config,
		spool:         spoolBuffer,
		toBeFlushedCh: flushCh,
		statistics:    statistics,
		filePathGenerator: cloudstorage.NewFilePathGenerator(
			changefeedID, config, storage, extension,
		),
		metricWriteBytes: metrics.CloudStorageWriteBytesGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()),
		metricFileCount: metrics.CloudStorageFileCountGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()),
		metricWriteDuration: metrics.CloudStorageWriteDurationHistogram.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()),
		metricFlushDuration: metrics.CloudStorageFlushDurationHistogram.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String()),
		metricsWorkerBusyRatio: metrics.CloudStorageWorkerBusyRatio.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), strconv.Itoa(id)),
		writerLabel: strconv.Itoa(id),
	}
	d.bufferManager = newBufferManager(
		d.shardID,
		d.changeFeedID,
		d.config,
		d.spool,
		d.toBeFlushedCh,
	)
	return d
}

func (d *writer) run(ctx context.Context) error {
	defer d.deleteMetrics()
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return d.flushMessages(ctx)
	})
	g.Go(func() error {
		return d.bufferManager.run(ctx)
	})
	return g.Wait()
}

func (d *writer) flushMessages(ctx context.Context) error {
	var flushTimeSlice time.Duration
	overseerDuration := d.config.FlushInterval * 2
	overseerTicker := time.NewTicker(overseerDuration)
	defer overseerTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-overseerTicker.C:
			d.metricsWorkerBusyRatio.Add(flushTimeSlice.Seconds())
			flushTimeSlice = 0
		case task, ok := <-d.toBeFlushedCh:
			if !ok {
				return nil
			}
			if task.marker != nil {
				// Flush marker ack point:
				// marker is emitted only after the pending batch of the same dispatcher
				// is emitted by bufferManager.
				task.marker.finish()
				continue
			}
			if len(task.batch.batch) == 0 {
				continue
			}

			start := time.Now()
			for table, singleTask := range task.batch.batch {
				if len(singleTask.entries) == 0 {
					continue
				}

				hasNewerSchemaVersion, err := d.filePathGenerator.CheckOrWriteSchema(ctx, table, singleTask.tableInfo)
				if err != nil {
					d.recordStageError("schema")
					log.Error("failed to write schema file to external storage",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.Error(err))
					return err
				}
				if hasNewerSchemaVersion {
					d.ignoreTableTask(singleTask)
					log.Warn("ignore messages belonging to an old schema version",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.String("schema", table.TableNameWithPhysicTableID.Schema),
						zap.String("table", table.TableNameWithPhysicTableID.Table),
						zap.Uint64("version", table.TableInfoVersion))
					continue
				}

				date := d.filePathGenerator.GenerateDateStr()
				dataFilePath, err := d.filePathGenerator.GenerateDataFilePath(ctx, table, date)
				if err != nil {
					d.recordStageError("data_path")
					log.Error("failed to generate data file path",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.Error(err))
					return err
				}
				indexFilePath, err := d.filePathGenerator.GenerateIndexFilePath(table, date)
				if err != nil {
					d.recordStageError("index_path")
					log.Error("failed to generate index file path",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.Error(err))
					return errors.Trace(err)
				}

				if err := d.writeDataFile(ctx, dataFilePath, indexFilePath, singleTask); err != nil {
					log.Error("failed to write data file to external storage",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.String("path", dataFilePath),
						zap.Error(err))
					return err
				}

				log.Debug("write file to storage success",
					zap.Int("shardID", d.shardID),
					zap.String("keyspace", d.changeFeedID.Keyspace()),
					zap.Stringer("changefeed", d.changeFeedID.ID()),
					zap.String("schema", table.TableNameWithPhysicTableID.Schema),
					zap.String("table", table.TableNameWithPhysicTableID.Table),
					zap.String("path", dataFilePath))
			}

			flushDuration := time.Since(start)
			flushTimeSlice += flushDuration
		}
	}
}

func (d *writer) writeIndexFile(ctx context.Context, path, content string) error {
	start := time.Now()
	err := d.storage.WriteFile(ctx, path, []byte(content))
	d.metricFlushDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		d.recordStageError("index_write")
		return errors.Trace(err)
	}
	return nil
}

func runCallbacks(callbacks []func()) {
	for _, callback := range callbacks {
		if callback == nil {
			continue
		}
		callback()
	}
}

func (d *writer) releaseEntries(entries []*spool.Entry) {
	for _, entry := range entries {
		d.spool.Release(entry)
	}
}

func (d *writer) appendEntryToBuffer(
	buf *bytes.Buffer,
	entry *spool.Entry,
	rowsCnt *int,
	bytesCnt *int64,
) ([]func(), error) {
	msgs, callbacks, err := d.spool.Load(entry)
	if err != nil {
		d.recordStageError("load")
		return nil, err
	}

	for _, msg := range msgs {
		if msg.Key != nil && *rowsCnt == 0 {
			buf.Write(msg.Key)
			*bytesCnt += int64(len(msg.Key))
		}
		*bytesCnt += int64(len(msg.Value))
		*rowsCnt += msg.GetRowsCount()
		buf.Write(msg.Value)
	}
	return callbacks, nil
}

func (d *writer) ignoreTableTask(task *singleTableTask) {
	for _, entry := range task.entries {
		d.spool.Discard(entry)
	}
}

func (d *writer) writeDataFile(ctx context.Context, dataFilePath, indexFilePath string, task *singleTableTask) error {
	var callbacks []func()
	buf := bytes.NewBuffer(make([]byte, 0, task.size))
	rowsCnt := 0
	bytesCnt := int64(0)

	for _, entry := range task.entries {
		entryCallbacks, err := d.appendEntryToBuffer(buf, entry, &rowsCnt, &bytesCnt)
		if err != nil {
			return err
		}
		callbacks = append(callbacks, entryCallbacks...)
	}

	if err := d.statistics.RecordBatchExecution(func() (int, int64, error) {
		start := time.Now()
		if d.config.FlushConcurrency <= 1 {
			err := d.storage.WriteFile(ctx, dataFilePath, buf.Bytes())
			if err != nil {
				d.recordStageError("data_write")
				return 0, 0, errors.Trace(err)
			}
			d.metricWriteDuration.Observe(time.Since(start).Seconds())
			return rowsCnt, bytesCnt, nil
		}

		writer, inErr := d.storage.Create(ctx, dataFilePath, &storage.WriterOption{
			Concurrency: d.config.FlushConcurrency,
		})
		if inErr != nil {
			d.recordStageError("data_write")
			return 0, 0, errors.Trace(inErr)
		}

		if _, inErr = writer.Write(ctx, buf.Bytes()); inErr != nil {
			d.recordStageError("data_write")
			return 0, 0, errors.Trace(inErr)
		}
		if inErr = writer.Close(ctx); inErr != nil {
			d.recordStageError("data_write")
			log.Error("failed to close writer",
				zap.Error(inErr),
				zap.Int("shardID", d.shardID),
				zap.Any("table", task.tableInfo.TableName),
				zap.String("keyspace", d.changeFeedID.Keyspace()),
				zap.Stringer("changefeed", d.changeFeedID.ID()))
			return 0, 0, errors.Trace(inErr)
		}

		d.metricFlushDuration.Observe(time.Since(start).Seconds())
		return rowsCnt, bytesCnt, nil
	}); err != nil {
		return err
	}

	d.metricWriteBytes.Add(float64(bytesCnt))
	d.metricFileCount.Add(1)

	if err := d.writeIndexFile(ctx, indexFilePath, path.Base(dataFilePath)+"\n"); err != nil {
		log.Error("failed to write index file to external storage",
			zap.Int("shardID", d.shardID),
			zap.String("keyspace", d.changeFeedID.Keyspace()),
			zap.Stringer("changefeed", d.changeFeedID.ID()),
			zap.String("path", indexFilePath),
			zap.Error(err))
		return err
	}

	runCallbacks(callbacks)
	d.releaseEntries(task.entries)
	return nil
}

func (d *writer) enqueueTask(ctx context.Context, t *task) error {
	return d.bufferManager.enqueueTask(ctx, t)
}

func (d *writer) recordStageError(stage string) {
	metrics.CloudStorageWriterErrorCounter.WithLabelValues(
		d.changeFeedID.Keyspace(),
		d.changeFeedID.ID().String(),
		d.writerLabel,
		stage,
	).Inc()
}

func (d *writer) deleteMetrics() {
	metrics.CloudStorageWorkerBusyRatio.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel)
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "schema")
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "data_path")
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "index_path")
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "load")
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "data_write")
	metrics.CloudStorageWriterErrorCounter.DeleteLabelValues(d.changeFeedID.Keyspace(), d.changeFeedID.ID().String(), d.writerLabel, "index_write")
}
