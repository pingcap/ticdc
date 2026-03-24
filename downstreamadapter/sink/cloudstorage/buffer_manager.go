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
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/prometheus/client_golang/prometheus"
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
	var (
		keyspace    = changefeedID.Keyspace()
		name        = changefeedID.Name()
		writerLabel = strconv.Itoa(shardID)
	)
	return &bufferManager{
		changeFeedID:         changefeedID,
		config:               config,
		spool:                spoolBuffer,
		inputCh:              chann.NewAutoDrainChann[*task](),
		flushCh:              flushCh,
		writerLabel:          writerLabel,
		metricPendingTables:  metrics.CloudStoragePendingTablesGauge.WithLabelValues(keyspace, name, writerLabel),
		metricPendingEntries: metrics.CloudStoragePendingEntriesGauge.WithLabelValues(keyspace, name, writerLabel),
		metricPendingBytes:   metrics.CloudStoragePendingBytesGauge.WithLabelValues(keyspace, name, writerLabel),
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	buffered := newBufferedTasks()
	ticker := time.NewTicker(c.config.FlushInterval)
	defer func() {
		ticker.Stop()
		close(c.flushCh)
		c.deleteMetrics()
	}()
	for {
		failpoint.Inject("passTickerOnce", func() {
			<-ticker.C
		})

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-ticker.C:
			if buffered.isEmpty() {
				continue
			}
			var err error
			buffered, err = c.emitBatch(ctx, buffered, flushReasonInterval)
			if err != nil {
				return err
			}
		case task, ok := <-c.inputCh.Out():
			if !ok {
				if buffered.isEmpty() {
					return nil
				}
				_, err := c.emitBatch(ctx, buffered, flushReasonClose)
				return err
			}

			if task.isFlushTask() {
				dispatcherBatch := buffered.detachDispatcher(task.dispatcherID)
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
			buffered, err = c.handleDMLEvent(ctx, buffered, task)
			if err != nil {
				return err
			}
		}
	}
}

func (c *bufferManager) handleDMLEvent(ctx context.Context, batch bufferedTasks, task *task) (bufferedTasks, error) {
	if len(task.encodedMsgs) == 0 {
		task.event.PostEnqueue()
		return batch, nil
	}

	for {
		action, entry, err := c.spool.TryEnqueue(task.encodedMsgs, task.event.PostEnqueue)
		if err != nil {
			return batch, err
		}
		switch action {
		case spool.EnqueueActionAcceptedOversized:
			batch.addEntry(task, entry)
			return c.emitTableBatch(ctx, batch, task.versionedTable, flushReasonOversize)
		case spool.EnqueueActionWaitDiskQuota:
			batch, err = c.emitBatch(ctx, batch, flushReasonQuota)
			if err != nil {
				return batch, err
			}
			if err := c.spool.WaitForDiskQuota(ctx, task.encodedMsgs); err != nil {
				return batch, err
			}
			continue
		default:
			batch.addEntry(task, entry)
		}

		table := task.versionedTable
		if batch.tables[table].size < uint64(c.config.FileSize) {
			c.updatePendingMetrics(batch)
			return batch, nil
		}
		return c.emitTableBatch(ctx, batch, table, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, batch bufferedTasks, reason string) (bufferedTasks, error) {
	if batch.isEmpty() {
		c.updatePendingMetrics(batch)
		return batch, nil
	}

	select {
	case <-ctx.Done():
		return batch, errors.Trace(context.Cause(ctx))
	case c.flushCh <- writerTask{tableBatch: batch}:
		metrics.CloudStorageFlushCountCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), c.writerLabel, reason).Inc()
		emptyBatch := newBufferedTasks()
		c.updatePendingMetrics(emptyBatch)
		return emptyBatch, nil
	}
}

func (c *bufferManager) emitTableBatch(
	ctx context.Context,
	batch bufferedTasks,
	table cloudstorage.VersionedTableName,
	reason string,
) (bufferedTasks, error) {
	tableBatch := batch.detachTable(table)
	select {
	case <-ctx.Done():
		return batch, errors.Trace(context.Cause(ctx))
	case c.flushCh <- writerTask{tableBatch: tableBatch}:
		metrics.CloudStorageFlushCountCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), c.writerLabel, reason).Inc()
		c.updatePendingMetrics(batch)
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

func (c *bufferManager) updatePendingMetrics(batch bufferedTasks) {
	pendingTables := len(batch.tables)
	pendingEntries := 0
	pendingBytes := uint64(0)
	for _, tableTask := range batch.tables {
		pendingEntries += len(tableTask.entries)
		pendingBytes += tableTask.size
	}
	c.metricPendingTables.Set(float64(pendingTables))
	c.metricPendingEntries.Set(float64(pendingEntries))
	c.metricPendingBytes.Set(float64(pendingBytes))
}

func (c *bufferManager) deleteMetrics() {
	var (
		keyspace = c.changeFeedID.Keyspace()
		name     = c.changeFeedID.Name()
	)
	metrics.CloudStoragePendingTablesGauge.DeleteLabelValues(keyspace, name, c.writerLabel)
	metrics.CloudStoragePendingEntriesGauge.DeleteLabelValues(keyspace, name, c.writerLabel)
	metrics.CloudStoragePendingBytesGauge.DeleteLabelValues(keyspace, name, c.writerLabel)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonSize)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonInterval)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonBarrier)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonClose)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonQuota)
	metrics.CloudStorageFlushCountCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonOversize)
}

type bufferedTasks struct {
	tables map[cloudstorage.VersionedTableName]*singleTableTask
}

type singleTableTask struct {
	size      uint64
	tableInfo *common.TableInfo
	entries   []*spool.Entry
}

func newBufferedTasks() bufferedTasks {
	return bufferedTasks{
		tables: make(map[cloudstorage.VersionedTableName]*singleTableTask),
	}
}

func (t *bufferedTasks) isEmpty() bool {
	return len(t.tables) == 0
}

func (t *bufferedTasks) addEntry(event *task, entry *spool.Entry) {
	table := event.versionedTable
	if _, ok := t.tables[table]; !ok {
		t.tables[table] = &singleTableTask{
			size:      0,
			tableInfo: event.event.TableInfo,
		}
	}

	tableTask := t.tables[table]
	tableTask.size += entry.FileBytes()
	tableTask.entries = append(tableTask.entries, entry)
}

func (t *bufferedTasks) detachTable(table cloudstorage.VersionedTableName) bufferedTasks {
	tableTask := t.tables[table]
	if tableTask == nil {
		panic("table batch not found")
	}
	delete(t.tables, table)

	return bufferedTasks{
		tables: map[cloudstorage.VersionedTableName]*singleTableTask{table: tableTask},
	}
}

func (t *bufferedTasks) detachDispatcher(dispatcherID common.DispatcherID) bufferedTasks {
	batchByDispatcher := newBufferedTasks()
	for table, tableTask := range t.tables {
		if table.DispatcherID != dispatcherID {
			continue
		}
		batchByDispatcher.tables[table] = tableTask
		delete(t.tables, table)
	}
	return batchByDispatcher
}
