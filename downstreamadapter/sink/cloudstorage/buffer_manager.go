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
	outputCh chan writerTask

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
	outputCh chan writerTask,
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
		close(c.outputCh)
		c.deleteMetrics()
	}()

	var err error
	for {
		failpoint.Inject("passTickerOnce", func() {
			<-ticker.C
		})

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-ticker.C:
			buffered, err = c.emitBatch(ctx, buffered, flushReasonInterval)
			if err != nil {
				return err
			}
		case task, ok := <-c.inputCh.Out():
			if !ok {
				_, err := c.emitBatch(ctx, buffered, flushReasonClose)
				return err
			}
			if task.isFlushTask() {
				dispatcherBatch := buffered.detachByDispatcher(task.dispatcherID)
				if !dispatcherBatch.isEmpty() {
					select {
					case <-ctx.Done():
						return errors.Trace(context.Cause(ctx))
					case c.outputCh <- writerTask{tableBatch: dispatcherBatch}:
						metrics.CloudStorageFlushCountCounter.WithLabelValues(
							c.changeFeedID.Keyspace(),
							c.changeFeedID.Name(),
							c.writerLabel,
							flushReasonBarrier,
						).Inc()
					}
				}
				c.updatePendingMetrics(buffered)
				select {
				case <-ctx.Done():
					return errors.Trace(context.Cause(ctx))
				case c.outputCh <- writerTask{marker: task.marker}:
				}
				continue
			}
			buffered, err = c.handleDMLEvent(ctx, buffered, task)
			if err != nil {
				return err
			}
		}
	}
}

func (c *bufferManager) handleDMLEvent(ctx context.Context, buffered bufferedTasks, task *task) (bufferedTasks, error) {
	if len(task.encodedMsgs) == 0 {
		task.event.PostEnqueue()
		return buffered, nil
	}

	for {
		action, entry, err := c.spool.TryEnqueue(task.encodedMsgs, task.event.PostEnqueue)
		if err != nil {
			return buffered, err
		}
		switch action {
		case spool.EnqueueActionAcceptedOversized:
			buffered.addEntry(task, entry)
			return c.emitTableBatch(ctx, buffered, task.versionedTable, flushReasonOversize)
		case spool.EnqueueActionWaitDiskQuota:
			buffered, err = c.emitBatch(ctx, buffered, flushReasonQuota)
			if err != nil {
				return buffered, err
			}
			if err := c.spool.WaitForDiskQuota(ctx, task.encodedMsgs); err != nil {
				return buffered, err
			}
			continue
		default:
			buffered.addEntry(task, entry)
		}

		version := task.versionedTable
		if buffered.tasks[version].size < uint64(c.config.FileSize) {
			c.updatePendingMetrics(buffered)
			return buffered, nil
		}
		return c.emitTableBatch(ctx, buffered, version, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, buffer bufferedTasks, reason string) (bufferedTasks, error) {
	if buffer.isEmpty() {
		c.updatePendingMetrics(buffer)
		return buffer, nil
	}

	select {
	case <-ctx.Done():
		return buffer, errors.Trace(context.Cause(ctx))
	case c.outputCh <- writerTask{tableBatch: buffer}:
		metrics.CloudStorageFlushCountCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), c.writerLabel, reason).Inc()
		buffered := newBufferedTasks()
		c.updatePendingMetrics(buffered)
		return buffered, nil
	}
}

func (c *bufferManager) emitTableBatch(
	ctx context.Context,
	batch bufferedTasks,
	table cloudstorage.VersionedTableName,
	reason string,
) (bufferedTasks, error) {
	tableBatch, err := batch.detachByTable(table)
	if err != nil {
		return batch, err
	}
	select {
	case <-ctx.Done():
		return batch, errors.Trace(context.Cause(ctx))
	case c.outputCh <- writerTask{tableBatch: tableBatch}:
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
	pendingTables := len(batch.tasks)
	pendingEntries := 0
	pendingBytes := uint64(0)
	for _, tableTask := range batch.tasks {
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
	tasks map[cloudstorage.VersionedTableName]*singleTableTask
}

type singleTableTask struct {
	size      uint64
	tableInfo *common.TableInfo
	entries   []*spool.Entry
}

func newBufferedTasks() bufferedTasks {
	return bufferedTasks{
		tasks: make(map[cloudstorage.VersionedTableName]*singleTableTask),
	}
}

func (t *bufferedTasks) isEmpty() bool {
	return len(t.tasks) == 0
}

func (t *bufferedTasks) addEntry(event *task, entry *spool.Entry) {
	table := event.versionedTable
	if _, ok := t.tasks[table]; !ok {
		t.tasks[table] = &singleTableTask{
			size:      0,
			tableInfo: event.event.TableInfo,
		}
	}

	tableTask := t.tasks[table]
	tableTask.size += entry.FileBytes()
	tableTask.entries = append(tableTask.entries, entry)
}

func (t *bufferedTasks) detachByTable(version cloudstorage.VersionedTableName) (bufferedTasks, error) {
	tableTask := t.tasks[version]
	if tableTask == nil {
		return bufferedTasks{}, errors.ErrInternalCheckFailed.GenWithStack(
			"table batch not found: %+v",
			version,
		)
	}
	delete(t.tasks, version)

	return bufferedTasks{
		tasks: map[cloudstorage.VersionedTableName]*singleTableTask{version: tableTask},
	}, nil
}

func (t *bufferedTasks) detachByDispatcher(dispatcherID common.DispatcherID) bufferedTasks {
	batchByDispatcher := newBufferedTasks()
	for version, tableTask := range t.tasks {
		if version.DispatcherID != dispatcherID {
			continue
		}
		batchByDispatcher.tasks[version] = tableTask
		delete(t.tasks, version)
	}
	return batchByDispatcher
}
