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

	inputCh  *chann.DrainableChann[*task]
	outputCh chan flushTask
	buffer   bufferedTasks

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
	outputCh chan flushTask,
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
		outputCh:             outputCh,
		buffer:               newBufferedTasks(),
		writerLabel:          writerLabel,
		metricPendingTables:  metrics.CloudStoragePendingTablesGauge.WithLabelValues(keyspace, name, writerLabel),
		metricPendingEntries: metrics.CloudStoragePendingEntriesGauge.WithLabelValues(keyspace, name, writerLabel),
		metricPendingBytes:   metrics.CloudStoragePendingBytesGauge.WithLabelValues(keyspace, name, writerLabel),
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	ticker := time.NewTicker(c.config.FlushInterval)
	defer func() {
		ticker.Stop()
		close(c.outputCh)
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
			if err := c.emitBatch(ctx, flushReasonInterval); err != nil {
				return err
			}
		case task, ok := <-c.inputCh.Out():
			if !ok {
				return c.emitBatch(ctx, flushReasonClose)
			}
			if task.isFlushTask() {
				dispatcherBatch := c.buffer.detachByDispatcher(task.dispatcherID)
				if !dispatcherBatch.isEmpty() {
					if err := c.sendToWriter(ctx, flushTask{batch: dispatcherBatch}, flushReasonBarrier); err != nil {
						return err
					}
				}
				c.updatePendingMetrics()
				select {
				case <-ctx.Done():
					return errors.Trace(context.Cause(ctx))
				case c.outputCh <- flushTask{marker: task.marker}:
				}
				continue
			}
			if err := c.handleDMLTask(ctx, task); err != nil {
				return err
			}
		}
	}
}

func (c *bufferManager) handleDMLTask(ctx context.Context, task *task) error {
	if len(task.encodedMsgs) == 0 {
		task.event.PostEnqueue()
		return nil
	}

	for {
		action, entry, err := c.spool.TryEnqueue(task.encodedMsgs, task.event.PostEnqueue)
		if err != nil {
			return err
		}
		switch action {
		case spool.EnqueueActionAcceptedOversized:
			c.buffer.addEntry(task, entry)
			return c.emitTableBatch(ctx, task.versionedTable, flushReasonOversize)
		case spool.EnqueueActionWaitDiskQuota:
			if err := c.emitBatch(ctx, flushReasonQuota); err != nil {
				return err
			}
			if err := c.spool.WaitForDiskQuota(ctx, task.encodedMsgs); err != nil {
				return err
			}
			continue
		default:
			c.buffer.addEntry(task, entry)
		}

		version := task.versionedTable
		if c.buffer.tables[version].size < uint64(c.config.FileSize) {
			c.updatePendingMetrics()
			return nil
		}
		return c.emitTableBatch(ctx, version, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, reason string) error {
	if c.buffer.isEmpty() {
		c.updatePendingMetrics()
		return nil
	}

	if err := c.sendToWriter(ctx, flushTask{batch: c.buffer}, reason); err != nil {
		return err
	}

	c.buffer = newBufferedTasks()
	c.updatePendingMetrics()
	return nil
}

func (c *bufferManager) emitTableBatch(
	ctx context.Context,
	table cloudstorage.VersionedTableName,
	reason string,
) error {
	tableBatch, err := c.buffer.detachByTable(table)
	if err != nil {
		return err
	}
	if err := c.sendToWriter(ctx, flushTask{batch: tableBatch}, reason); err != nil {
		return err
	}
	c.updatePendingMetrics()
	return nil
}

func (c *bufferManager) enqueueTask(ctx context.Context, t *task) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case c.inputCh.In() <- t:
		return nil
	}
}

func (c *bufferManager) updatePendingMetrics() {
	var (
		pendingEntries = 0
		pendingBytes   = uint64(0)
	)
	for _, tableTask := range c.buffer.tables {
		pendingEntries += len(tableTask.entries)
		pendingBytes += tableTask.size
	}
	c.metricPendingTables.Set(float64(len(c.buffer.tables)))
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

func (t *bufferedTasks) detachByTable(version cloudstorage.VersionedTableName) (bufferedTasks, error) {
	tableTask := t.tables[version]
	if tableTask == nil {
		return bufferedTasks{}, errors.ErrInternalCheckFailed.GenWithStack(
			"table batch not found: %+v",
			version,
		)
	}
	delete(t.tables, version)

	return bufferedTasks{
		tables: map[cloudstorage.VersionedTableName]*singleTableTask{version: tableTask},
	}, nil
}

func (t *bufferedTasks) detachByDispatcher(dispatcherID common.DispatcherID) bufferedTasks {
	detached := newBufferedTasks()
	for version, tableTask := range t.tables {
		if version.DispatcherID != dispatcherID {
			continue
		}
		detached.tables[version] = tableTask
		delete(t.tables, version)
	}
	return detached
}

func (c *bufferManager) sendToWriter(ctx context.Context, task flushTask, reason string) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case c.outputCh <- task:
		metrics.CloudStorageFlushCountCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), c.writerLabel, reason).Inc()
		return nil
	}
}
