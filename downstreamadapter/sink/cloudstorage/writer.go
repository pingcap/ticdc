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

	// flushCh is owned by writer. bufferManager emits flush work only through
	// writer methods. Both writer and bufferManager stop on the shared ctx, so
	// the channel does not need to be closed explicitly.
	flushCh chan flushTask

	statistics        *pmetrics.Statistics
	filePathGenerator *cloudstorage.FilePathGenerator

	metricWriteBytes       prometheus.Observer
	metricFileCount        prometheus.Counter
	metricFlushDuration    prometheus.Observer
	metricsWorkerBusyRatio prometheus.Counter
	writerLabel            string
}

// flushTask is internal and never crosses component boundary.
// marker task and data batch are mutually exclusive in normal flow.
type flushTask struct {
	batch  map[cloudstorage.VersionedTableName]*payload
	marker *flushMarker
}

type payload struct {
	tableInfo          *common.TableInfo
	data               []byte
	rowsCount          int
	nBytes             int64
	entries            []*spool.Entry
	postFlushCallbacks []func()
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
	var (
		keyspace   = changefeedID.Keyspace()
		changefeed = changefeedID.Name()
	)
	d := &writer{
		shardID:           id,
		changeFeedID:      changefeedID,
		storage:           storage,
		config:            config,
		spool:             spoolBuffer,
		flushCh:           make(chan flushTask, 64),
		statistics:        statistics,
		filePathGenerator: cloudstorage.NewFilePathGenerator(changefeedID, config, storage, extension),

		metricWriteBytes:       metrics.CloudStorageWriteBytesHist.WithLabelValues(keyspace, changefeed),
		metricFileCount:        metrics.CloudStorageFileCounter.WithLabelValues(keyspace, changefeed),
		metricFlushDuration:    metrics.CloudStorageFlushDurationHistogram.WithLabelValues(keyspace, changefeed),
		metricsWorkerBusyRatio: metrics.CloudStorageWorkerBusyRatio.WithLabelValues(keyspace, changefeed, strconv.Itoa(id)),
		writerLabel:            strconv.Itoa(id),
	}
	d.bufferManager = newBufferManager(d.shardID, d.changeFeedID, d.config, d.spool, d.enqueueFlushTask)
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
	keyspace := d.changeFeedID.Keyspace()
	changefeed := d.changeFeedID.Name()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-overseerTicker.C:
			d.metricsWorkerBusyRatio.Add(flushTimeSlice.Seconds())
			flushTimeSlice = 0
		case task := <-d.flushCh:
			if task.marker != nil {
				// Flush marker ack point:
				// marker is emitted only after the pending batch of the same dispatcher
				// is emitted by bufferManager.
				task.marker.finish()
				continue
			}
			if len(task.batch) == 0 {
				continue
			}

			start := time.Now()
			for table, payload := range task.batch {
				if payload == nil || len(payload.entries) == 0 {
					continue
				}

				hasNewerSchemaVersion, err := d.filePathGenerator.CheckOrWriteSchema(ctx, table, payload.tableInfo)
				if err != nil {
					log.Error("failed to write schema file to external storage",
						zap.String("keyspace", keyspace),
						zap.String("changefeed", changefeed),
						zap.Int("shardID", d.shardID),
						zap.Error(err))
					return err
				}
				if hasNewerSchemaVersion {
					d.discardPayload(payload)
					log.Warn("ignore messages belonging to an old schema version",
						zap.String("keyspace", keyspace),
						zap.String("changefeed", changefeed),
						zap.String("schema", table.TableNameWithPhysicTableID.Schema),
						zap.String("table", table.TableNameWithPhysicTableID.Table),
						zap.Uint64("version", table.TableInfoVersion),
						zap.Int("shardID", d.shardID))
					continue
				}

				date := d.filePathGenerator.GenerateDateStr()
				dataFilePath, err := d.filePathGenerator.GenerateDataFilePath(ctx, table, date)
				if err != nil {
					log.Error("failed to generate data file path",
						zap.String("keyspace", keyspace),
						zap.String("changefeed", changefeed),
						zap.Int("shardID", d.shardID),
						zap.Error(err))
					return err
				}
				indexFilePath, err := d.filePathGenerator.GenerateIndexFilePath(table, date)
				if err != nil {
					log.Error("failed to generate index file path",
						zap.String("keyspace", keyspace),
						zap.String("changefeed", changefeed),
						zap.Int("shardID", d.shardID),
						zap.Error(err))
					return errors.Trace(err)
				}

				if err := d.writeDataFile(ctx, dataFilePath, indexFilePath, payload); err != nil {
					log.Error("failed to write data file to external storage",
						zap.String("keyspace", keyspace),
						zap.String("changefeed", changefeed),
						zap.String("path", dataFilePath),
						zap.Int("shardID", d.shardID),
						zap.Error(err))
					return err
				}

				log.Debug("write file to storage success",
					zap.String("keyspace", keyspace),
					zap.String("changefeed", changefeed),
					zap.String("path", dataFilePath),
					zap.String("schema", table.TableNameWithPhysicTableID.Schema),
					zap.String("table", table.TableNameWithPhysicTableID.Table),
					zap.Int("shardID", d.shardID))
			}

			flushDuration := time.Since(start)
			flushTimeSlice += flushDuration
		}
	}
}

func (d *writer) discardPayload(payload *payload) {
	for _, entry := range payload.entries {
		d.spool.Discard(entry)
	}
}

func (d *writer) writeDataFile(ctx context.Context, dataFilePath, indexFilePath string, payload *payload) error {
	keyspace := d.changeFeedID.Keyspace()
	changefeed := d.changeFeedID.Name()

	start := time.Now()
	if err := d.statistics.RecordBatchExecution(func() (int, int64, error) {
		if d.config.FlushConcurrency <= 1 {
			err := d.storage.WriteFile(ctx, dataFilePath, payload.data)
			if err != nil {
				return 0, 0, errors.Trace(err)
			}
			return payload.rowsCount, payload.nBytes, nil
		}

		writer, inErr := d.storage.Create(ctx, dataFilePath, &storage.WriterOption{
			Concurrency: d.config.FlushConcurrency,
		})
		if inErr != nil {
			return 0, 0, errors.Trace(inErr)
		}

		if _, inErr = writer.Write(ctx, payload.data); inErr != nil {
			return 0, 0, errors.Trace(inErr)
		}
		if inErr = writer.Close(ctx); inErr != nil {
			log.Error("failed to close writer",
				zap.String("keyspace", keyspace),
				zap.String("changefeed", changefeed),
				zap.Any("table", payload.tableInfo.TableName),
				zap.Int("shardID", d.shardID),
				zap.Error(inErr))
			return 0, 0, errors.Trace(inErr)
		}
		return payload.rowsCount, payload.nBytes, nil
	}); err != nil {
		return err
	}

	if err := d.storage.WriteFile(ctx, indexFilePath, []byte(path.Base(dataFilePath)+"\n")); err != nil {
		log.Error("failed to write index file to external storage",
			zap.String("keyspace", keyspace),
			zap.String("changefeed", changefeed),
			zap.String("path", indexFilePath),
			zap.Int("shardID", d.shardID),
			zap.Error(err))
		return err
	}

	d.metricFlushDuration.Observe(time.Since(start).Seconds())
	d.metricWriteBytes.Observe(float64(payload.nBytes))
	d.metricFileCount.Inc()

	for _, postFlushCallback := range payload.postFlushCallbacks {
		if postFlushCallback != nil {
			postFlushCallback()
		}
	}
	for _, entry := range payload.entries {
		d.spool.Release(entry)
	}
	return nil
}

func (d *writer) enqueueTask(ctx context.Context, t *task) error {
	return d.bufferManager.enqueueTask(ctx, t)
}

func (d *writer) enqueueFlushTask(ctx context.Context, task flushTask) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case d.flushCh <- task:
		return nil
	}
}

func (d *writer) deleteMetrics() {
	keyspace := d.changeFeedID.Keyspace()
	changefeedName := d.changeFeedID.Name()
	metrics.CloudStorageWorkerBusyRatio.DeleteLabelValues(keyspace, changefeedName, d.writerLabel)
}
