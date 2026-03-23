// Copyright 2026 PingCAP, Inc.
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
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	flushReasonSize     = "size"
	flushReasonInterval = "interval"
	flushReasonBarrier  = "barrier"
	flushReasonClose    = "close"
	flushReasonQuota    = "quota"
	flushReasonOversize = "oversized"
)

// bufferManager owns pending DML batches for one writer shard. It decides
// when to emit a flush batch and how to react when local spool disk quota is tight.
type bufferManager struct {
	shardID      int
	changeFeedID common.ChangeFeedID
	config       *cloudstorage.Config
	spool        *spool.Spool

	inputCh *chann.DrainableChann[*task]
	flushCh chan writerTask

	writerLabel          string
	metricPendingTables  prometheus.Gauge
	metricPendingEntries prometheus.Gauge
	metricPendingBytes   prometheus.Gauge
}

func newBufferManager(
	shardID int,
	changefeedID common.ChangeFeedID,
	config *cloudstorage.Config,
	spoolBuffer *spool.Spool,
	flushCh chan writerTask,
) *bufferManager {
	return &bufferManager{
		shardID:      shardID,
		changeFeedID: changefeedID,
		config:       config,
		spool:        spoolBuffer,
		inputCh:      chann.NewAutoDrainChann[*task](),
		flushCh:      flushCh,
		writerLabel:  strconv.Itoa(shardID),
		metricPendingTables: metrics.CloudStoragePendingTablesGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), strconv.Itoa(shardID)),
		metricPendingEntries: metrics.CloudStoragePendingEntriesGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), strconv.Itoa(shardID)),
		metricPendingBytes: metrics.CloudStoragePendingBytesGauge.
			WithLabelValues(changefeedID.Keyspace(), changefeedID.ID().String(), strconv.Itoa(shardID)),
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	batch := newBatchedTask()
	ticker := time.NewTicker(c.config.FlushInterval)
	defer ticker.Stop()
	defer close(c.flushCh)
	defer c.deleteMetrics()
	c.updatePendingMetrics(batch)

	for {
		failpoint.Inject("passTickerOnce", func() {
			<-ticker.C
		})

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-ticker.C:
			if batch.isEmpty() {
				continue
			}
			tablesLength := len(batch.batch)
			var err error
			batch, err = c.emitBatch(ctx, batch, flushReasonInterval)
			if err != nil {
				return err
			}
			log.Debug("flush task is emitted successfully when flush interval exceeds",
				zap.Int("tablesLength", tablesLength))
		case task, ok := <-c.inputCh.Out():
			if !ok {
				if batch.isEmpty() {
					return nil
				}
				_, err := c.emitBatch(ctx, batch, flushReasonClose)
				return err
			}

			if task.isFlushTask() {
				dispatcherBatch := batch.detachTaskByDispatcher(task.dispatcherID)
				if !dispatcherBatch.isEmpty() {
					var err error
					dispatcherBatch, err = c.emitBatch(ctx, dispatcherBatch, flushReasonBarrier)
					if err != nil {
						return err
					}
				}
				select {
				case <-ctx.Done():
					return errors.Trace(context.Cause(ctx))
				case c.flushCh <- writerTask{marker: task.marker}:
				}
				continue
			}

			var err error
			batch, err = c.handleDMLEvent(ctx, batch, task)
			if err != nil {
				return err
			}
		}
	}
}

func (c *bufferManager) handleDMLEvent(ctx context.Context, batch batchedTask, task *task) (batchedTask, error) {
	if len(task.encodedMsgs) == 0 {
		task.event.PostEnqueue()
		return batch, nil
	}

	for {
		action, entry, err := c.spool.TryEnqueue(task.encodedMsgs, task.event.PostEnqueue)
		if err != nil {
			return batch, err
		}
		if action == spool.EnqueueActionAcceptedOversized {
			batch.handleSingleTableEvent(task, entry)
			return c.emitTableBatch(ctx, batch, task.versionedTable, flushReasonOversize)
		}

		if action == spool.EnqueueActionWaitDiskQuota {
			batch, err = c.emitBatch(ctx, batch, flushReasonQuota)
			if err != nil {
				return batch, err
			}
			if err := c.spool.WaitForDiskQuota(ctx, task.encodedMsgs); err != nil {
				return batch, err
			}
			continue
		}

		batch.handleSingleTableEvent(task, entry)

		table := task.versionedTable
		if batch.batch[table].size < uint64(c.config.FileSize) {
			c.updatePendingMetrics(batch)
			return batch, nil
		}
		return c.emitTableBatch(ctx, batch, table, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, batch batchedTask, reason string) (batchedTask, error) {
	if batch.isEmpty() {
		c.updatePendingMetrics(batch)
		return batch, nil
	}

	select {
	case <-ctx.Done():
		return batch, errors.Trace(context.Cause(ctx))
	case c.flushCh <- writerTask{batch: batch}:
		c.recordFlush(reason)
		emptyBatch := newBatchedTask()
		c.updatePendingMetrics(emptyBatch)
		return emptyBatch, nil
	}
}

func (c *bufferManager) emitTableBatch(
	ctx context.Context,
	batch batchedTask,
	table cloudstorage.VersionedTableName,
	reason string,
) (batchedTask, error) {
	tableBatch := batch.detachTaskByTable(table)
	select {
	case <-ctx.Done():
		return batch, errors.Trace(context.Cause(ctx))
	case c.flushCh <- writerTask{batch: tableBatch}:
		c.recordFlush(reason)
		c.updatePendingMetrics(batch)
		log.Debug("flush task is emitted successfully when file size exceeds",
			zap.Any("table", table),
			zap.Int("eventsLength", len(tableBatch.batch[table].entries)))
		return batch, nil
	}
}

func (c *bufferManager) enqueueTask(ctx context.Context, t *task) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case c.inputCh.In() <- t:
		return nil
	}
}

func (c *bufferManager) recordFlush(reason string) {
	metrics.CloudStorageFlushCountCounter.WithLabelValues(
		c.changeFeedID.Keyspace(),
		c.changeFeedID.ID().String(),
		c.writerLabel,
		reason,
	).Inc()
}

func (c *bufferManager) updatePendingMetrics(batch batchedTask) {
	pendingTables := len(batch.batch)
	pendingEntries := 0
	pendingBytes := uint64(0)
	for _, tableTask := range batch.batch {
		pendingEntries += len(tableTask.entries)
		pendingBytes += tableTask.size
	}
	c.metricPendingTables.Set(float64(pendingTables))
	c.metricPendingEntries.Set(float64(pendingEntries))
	c.metricPendingBytes.Set(float64(pendingBytes))
}

func (c *bufferManager) deleteMetrics() {
	metrics.CloudStoragePendingTablesGauge.DeleteLabelValues(
		c.changeFeedID.Keyspace(),
		c.changeFeedID.ID().String(),
		c.writerLabel,
	)
	metrics.CloudStoragePendingEntriesGauge.DeleteLabelValues(
		c.changeFeedID.Keyspace(),
		c.changeFeedID.ID().String(),
		c.writerLabel,
	)
	metrics.CloudStoragePendingBytesGauge.DeleteLabelValues(
		c.changeFeedID.Keyspace(),
		c.changeFeedID.ID().String(),
		c.writerLabel,
	)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonSize)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonInterval)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonBarrier)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonClose)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonQuota)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.ID().String(), c.writerLabel, flushReasonOversize)
}

type batchedTask struct {
	batch map[cloudstorage.VersionedTableName]*singleTableTask
}

type singleTableTask struct {
	size      uint64
	tableInfo *common.TableInfo
	entries   []*spool.Entry
}

func newBatchedTask() batchedTask {
	return batchedTask{
		batch: make(map[cloudstorage.VersionedTableName]*singleTableTask),
	}
}

func (t *batchedTask) isEmpty() bool {
	return len(t.batch) == 0
}

func (t *batchedTask) handleSingleTableEvent(event *task, entry *spool.Entry) {
	table := event.versionedTable
	if _, ok := t.batch[table]; !ok {
		t.batch[table] = &singleTableTask{
			size:      0,
			tableInfo: event.event.TableInfo,
		}
	}

	tableTask := t.batch[table]
	tableTask.size += entry.FileBytes()
	tableTask.entries = append(tableTask.entries, entry)
}

func (t *batchedTask) detachTaskByTable(table cloudstorage.VersionedTableName) batchedTask {
	tableTask := t.batch[table]
	if tableTask == nil {
		log.Panic("table not found in dml task", zap.Any("table", table), zap.Any("task", t))
	}
	delete(t.batch, table)

	return batchedTask{
		batch: map[cloudstorage.VersionedTableName]*singleTableTask{table: tableTask},
	}
}

func (t *batchedTask) detachTaskByDispatcher(dispatcherID common.DispatcherID) batchedTask {
	batchByDispatcher := newBatchedTask()
	for table, tableTask := range t.batch {
		if table.DispatcherID != dispatcherID {
			continue
		}
		batchByDispatcher.batch[table] = tableTask
		delete(t.batch, table)
	}
	return batchByDispatcher
}
