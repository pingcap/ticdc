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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	pmetrics "github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type writer struct {
	shardID      int
	changeFeedID commonType.ChangeFeedID
	storage      storage.ExternalStorage
	config       *cloudstorage.Config

	toBeFlushedCh chan writerTask
	inputCh       *chann.DrainableChann[*task]

	statistics        *pmetrics.Statistics
	filePathGenerator *cloudstorage.FilePathGenerator

	metricWriteBytes       prometheus.Gauge
	metricFileCount        prometheus.Gauge
	metricWriteDuration    prometheus.Observer
	metricFlushDuration    prometheus.Observer
	metricsWorkerBusyRatio prometheus.Counter
}

// writerTask is internal and never crosses component boundary.
// marker task and data batch are mutually exclusive in normal flow.
type writerTask struct {
	batch  batchedTask
	marker *drainMarker
}

func newWriter(
	id int,
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	extension string,
	statistics *pmetrics.Statistics,
) *writer {
	return &writer{
		shardID:       id,
		changeFeedID:  changefeedID,
		storage:       storage,
		config:        config,
		inputCh:       chann.NewAutoDrainChann[*task](),
		toBeFlushedCh: make(chan writerTask, 64),
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
	}
}

func (d *writer) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return d.flushMessages(ctx)
	})
	eg.Go(func() error {
		return d.genAndDispatchTask(ctx)
	})
	return eg.Wait()
}

func (d *writer) flushMessages(ctx context.Context) error {
	var flushTimeSlice time.Duration
	overseerDuration := d.config.FlushInterval * 2
	overseerTicker := time.NewTicker(overseerDuration)
	defer overseerTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-overseerTicker.C:
			d.metricsWorkerBusyRatio.Add(flushTimeSlice.Seconds())
			flushTimeSlice = 0
		case task, ok := <-d.toBeFlushedCh:
			if !ok {
				return nil
			}
			if task.marker != nil {
				// Drain marker ack point:
				// marker is emitted only after pending batch is flushed in genAndDispatchTask.
				task.marker.finish(nil)
				continue
			}
			if len(task.batch.batch) == 0 {
				continue
			}

			start := time.Now()
			for table, singleTask := range task.batch.batch {
				if len(singleTask.msgs) == 0 {
					continue
				}

				hasNewerSchemaVersion, err := d.filePathGenerator.CheckOrWriteSchema(ctx, table, singleTask.tableInfo)
				if err != nil {
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
					log.Error("failed to generate data file path",
						zap.Int("shardID", d.shardID),
						zap.String("keyspace", d.changeFeedID.Keyspace()),
						zap.Stringer("changefeed", d.changeFeedID.ID()),
						zap.Error(err))
					return err
				}
				indexFilePath := d.filePathGenerator.GenerateIndexFilePath(table, date)

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
		return errors.Trace(err)
	}
	return nil
}

func (d *writer) ignoreTableTask(task *singleTableTask) {
	for _, msg := range task.msgs {
		if msg.Callback != nil {
			msg.Callback()
		}
	}
}

func (d *writer) writeDataFile(ctx context.Context, dataFilePath, indexFilePath string, task *singleTableTask) error {
	var callbacks []func()
	buf := bytes.NewBuffer(make([]byte, 0, task.size))
	rowsCnt := 0
	bytesCnt := int64(0)

	for _, msg := range task.msgs {
		if msg.Key != nil && rowsCnt == 0 {
			buf.Write(msg.Key)
			bytesCnt += int64(len(msg.Key))
		}
		bytesCnt += int64(len(msg.Value))
		rowsCnt += msg.GetRowsCount()
		buf.Write(msg.Value)
		callbacks = append(callbacks, msg.Callback)
	}

	if err := d.statistics.RecordBatchExecution(func() (int, int64, error) {
		start := time.Now()
		if d.config.FlushConcurrency <= 1 {
			err := d.storage.WriteFile(ctx, dataFilePath, buf.Bytes())
			if err != nil {
				return 0, 0, errors.Trace(err)
			}
			d.metricWriteDuration.Observe(time.Since(start).Seconds())
			return rowsCnt, bytesCnt, nil
		}

		writer, inErr := d.storage.Create(ctx, dataFilePath, &storage.WriterOption{
			Concurrency: d.config.FlushConcurrency,
		})
		if inErr != nil {
			return 0, 0, errors.Trace(inErr)
		}

		if _, inErr = writer.Write(ctx, buf.Bytes()); inErr != nil {
			return 0, 0, errors.Trace(inErr)
		}
		if inErr = writer.Close(ctx); inErr != nil {
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

	for _, cb := range callbacks {
		if cb != nil {
			cb()
		}
	}
	return nil
}

// genAndDispatchTask builds table batches and emits flush tasks.
// Invariants:
//  1. DDL marker will flush current batch first, then emit marker task.
//  2. Context cancellation stops batching immediately.
//  3. Size-triggered flush only flushes the target table shard batch.
func (d *writer) genAndDispatchTask(ctx context.Context) error {
	batchedTask := newBatchedTask()
	ticker := time.NewTicker(d.config.FlushInterval)
	defer ticker.Stop()
	defer close(d.toBeFlushedCh)

	for {
		failpoint.Inject("passTickerOnce", func() {
			<-ticker.C
		})

		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if len(batchedTask.batch) == 0 {
				continue
			}
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case d.toBeFlushedCh <- writerTask{batch: batchedTask}:
				log.Debug("flush task is emitted successfully when flush interval exceeds",
					zap.Int("tablesLength", len(batchedTask.batch)))
				batchedTask = newBatchedTask()
			default:
			}
		case tableTask, ok := <-d.inputCh.Out():
			if !ok {
				if len(batchedTask.batch) == 0 {
					return nil
				}
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case d.toBeFlushedCh <- writerTask{batch: batchedTask}:
					return nil
				}
			}

			if tableTask.isDrainTask() {
				if len(batchedTask.batch) > 0 {
					select {
					case <-ctx.Done():
						return errors.Trace(ctx.Err())
					case d.toBeFlushedCh <- writerTask{batch: batchedTask}:
						batchedTask = newBatchedTask()
					}
				}
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case d.toBeFlushedCh <- writerTask{marker: tableTask.marker}:
				}
				continue
			}

			batchedTask.handleSingleTableEvent(tableTask)
			table := tableTask.versionedTable
			if batchedTask.batch[table].size >= uint64(d.config.FileSize) {
				taskByTable := batchedTask.generateTaskByTable(table)
				select {
				case <-ctx.Done():
					return errors.Trace(ctx.Err())
				case d.toBeFlushedCh <- writerTask{batch: taskByTable}:
					log.Debug("flush task is emitted successfully when file size exceeds",
						zap.Any("table", table),
						zap.Int("eventsLength", len(taskByTable.batch[table].msgs)))
				}
			}
		}
	}
}

func (d *writer) enqueueTask(ctx context.Context, taskValue *task) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case d.inputCh.In() <- taskValue:
		return nil
	}
}

type batchedTask struct {
	batch map[cloudstorage.VersionedTableName]*singleTableTask
}

type singleTableTask struct {
	size      uint64
	tableInfo *commonType.TableInfo
	msgs      []*common.Message
}

func newBatchedTask() batchedTask {
	return batchedTask{
		batch: make(map[cloudstorage.VersionedTableName]*singleTableTask),
	}
}

func (t *batchedTask) handleSingleTableEvent(event *task) {
	table := event.versionedTable
	if _, ok := t.batch[table]; !ok {
		t.batch[table] = &singleTableTask{
			size:      0,
			tableInfo: event.event.TableInfo,
		}
	}

	tableTask := t.batch[table]
	for _, msg := range event.encodedMsgs {
		tableTask.size += uint64(len(msg.Value))
	}
	tableTask.msgs = append(tableTask.msgs, event.encodedMsgs...)
}

func (t *batchedTask) generateTaskByTable(table cloudstorage.VersionedTableName) batchedTask {
	tableTask := t.batch[table]
	if tableTask == nil {
		log.Panic("table not found in dml task", zap.Any("table", table), zap.Any("task", t))
	}
	delete(t.batch, table)

	return batchedTask{
		batch: map[cloudstorage.VersionedTableName]*singleTableTask{table: tableTask},
	}
}
