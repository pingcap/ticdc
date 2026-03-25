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
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultBufferManagerChannelSize = 64

	flushReasonSize     = "size"
	flushReasonInterval = "interval"
	flushReasonBarrier  = "barrier"
	flushReasonQuota    = "quota"
	flushReasonOversize = "oversized"
)

// bufferManager owns pending DML batches for one writer shard. It decides
// when to emit a flush batch and how to react when local spool disk quota is tight.
type bufferManager struct {
	changeFeedID common.ChangeFeedID
	config       *cloudstorage.Config
	spool        *spool.Spool

	// inputCh is a bounded task queue owned by bufferManager.
	// Producers stop on ctx cancellation, so the channel does not need to be closed.
	inputCh          chan *task
	enqueueFlushTask func(context.Context, flushTask) error
	buffer           bufferedTasks

	writerLabel        string
	metricPendingBytes prometheus.Gauge
}

func newBufferManager(
	shardID int,
	changefeedID common.ChangeFeedID,
	config *cloudstorage.Config,
	spoolBuffer *spool.Spool,
	enqueueFlushTask func(context.Context, flushTask) error,
) *bufferManager {
	var (
		keyspace    = changefeedID.Keyspace()
		name        = changefeedID.Name()
		writerLabel = strconv.Itoa(shardID)
	)
	return &bufferManager{
		changeFeedID:       changefeedID,
		config:             config,
		spool:              spoolBuffer,
		inputCh:            make(chan *task, defaultBufferManagerChannelSize),
		enqueueFlushTask:   enqueueFlushTask,
		buffer:             newBufferedTasks(),
		writerLabel:        writerLabel,
		metricPendingBytes: metrics.CloudStoragePendingBytesGauge.WithLabelValues(keyspace, name, writerLabel),
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	ticker := time.NewTicker(c.config.FlushInterval)
	defer func() {
		ticker.Stop()
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
		case task := <-c.inputCh:
			if task.isFlushTask() {
				dispatcherBatch := c.buffer.detachByDispatcher(task.dispatcherID)
				if !dispatcherBatch.isEmpty() {
					if err := c.sendToWriter(ctx, dispatcherBatch, flushReasonBarrier); err != nil {
						return err
					}
				}
				c.metricPendingBytes.Set(float64(c.buffer.nBytes))
				if err := c.enqueueFlushTask(ctx, flushTask{marker: task.marker}); err != nil {
					return err
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
			c.addEntry(task, entry)
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
			c.addEntry(task, entry)
		}

		version := task.versionedTable
		if c.buffer.tables[version].size < uint64(c.config.FileSize) {
			c.metricPendingBytes.Set(float64(c.buffer.nBytes))
			return nil
		}
		return c.emitTableBatch(ctx, version, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, reason string) error {
	if c.buffer.isEmpty() {
		c.metricPendingBytes.Set(float64(c.buffer.nBytes))
		return nil
	}

	if err := c.sendToWriter(ctx, c.buffer, reason); err != nil {
		return err
	}

	c.buffer = newBufferedTasks()
	c.metricPendingBytes.Set(float64(c.buffer.nBytes))
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
	if err := c.sendToWriter(ctx, tableBatch, reason); err != nil {
		return err
	}
	c.metricPendingBytes.Set(float64(c.buffer.nBytes))
	return nil
}

func (c *bufferManager) enqueueTask(ctx context.Context, t *task) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case c.inputCh <- t:
		return nil
	}
}

func (c *bufferManager) deleteMetrics() {
	var (
		keyspace = c.changeFeedID.Keyspace()
		name     = c.changeFeedID.Name()
	)
	metrics.CloudStoragePendingBytesGauge.DeleteLabelValues(keyspace, name, c.writerLabel)
	metrics.CloudStorageFlushReasonCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonSize)
	metrics.CloudStorageFlushReasonCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonInterval)
	metrics.CloudStorageFlushReasonCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonBarrier)
	metrics.CloudStorageFlushReasonCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonQuota)
	metrics.CloudStorageFlushReasonCounter.DeleteLabelValues(keyspace, name, c.writerLabel, flushReasonOversize)
}

type bufferedTasks struct {
	tables map[cloudstorage.VersionedTableName]*singleTableTask
	nBytes uint64
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
	t.nBytes += entry.FileBytes()
}

func (c *bufferManager) addEntry(event *task, entry *spool.Entry) {
	c.buffer.addEntry(event, entry)
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
	t.nBytes -= tableTask.size

	return bufferedTasks{
		tables: map[cloudstorage.VersionedTableName]*singleTableTask{version: tableTask},
		nBytes: tableTask.size,
	}, nil
}

func (t *bufferedTasks) detachByDispatcher(dispatcherID common.DispatcherID) bufferedTasks {
	detached := newBufferedTasks()
	for version, tableTask := range t.tables {
		if version.DispatcherID != dispatcherID {
			continue
		}
		detached.tables[version] = tableTask
		detached.nBytes += tableTask.size
		t.nBytes -= tableTask.size
		delete(t.tables, version)
	}
	return detached
}

func (c *bufferManager) sendToWriter(ctx context.Context, batch bufferedTasks, reason string) error {
	payloads := make(map[cloudstorage.VersionedTableName]*payload, len(batch.tables))
	for table, tableTask := range batch.tables {
		payload, err := c.buildPayload(tableTask)
		if err != nil {
			return err
		}
		payloads[table] = payload
	}

	if err := c.enqueueFlushTask(ctx, flushTask{batch: payloads}); err != nil {
		return err
	}
	metrics.CloudStorageFlushReasonCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), c.writerLabel, reason).Inc()
	return nil
}

func (c *bufferManager) buildPayload(task *singleTableTask) (*payload, error) {
	var (
		buf                bytes.Buffer
		rowsCount          int
		nBytes             int64
		postFlushCallbacks []func()
	)
	buf.Grow(int(task.size))

	for _, entry := range task.entries {
		msgs, entryPostFlushCallbacks, err := c.spool.Load(entry)
		if err != nil {
			return nil, err
		}
		postFlushCallbacks = append(postFlushCallbacks, entryPostFlushCallbacks...)
		for _, msg := range msgs {
			if msg.Key != nil && rowsCount == 0 {
				buf.Write(msg.Key)
				nBytes += int64(len(msg.Key))
			}
			nBytes += int64(len(msg.Value))
			rowsCount += msg.GetRowsCount()
			buf.Write(msg.Value)
		}
	}

	return &payload{
		tableInfo:          task.tableInfo,
		data:               buf.Bytes(),
		rowsCount:          rowsCount,
		nBytes:             nBytes,
		entries:            task.entries,
		postFlushCallbacks: postFlushCallbacks,
	}, nil
}
