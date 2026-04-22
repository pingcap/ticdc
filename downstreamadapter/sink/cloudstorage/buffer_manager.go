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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
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
	buffer           tableBatches
}

func newBufferManager(
	changefeedID common.ChangeFeedID,
	config *cloudstorage.Config,
	spoolBuffer *spool.Spool,
	enqueueFlushTask func(context.Context, flushTask) error,
) *bufferManager {
	return &bufferManager{
		changeFeedID:     changefeedID,
		config:           config,
		spool:            spoolBuffer,
		inputCh:          make(chan *task, defaultBufferManagerChannelSize),
		enqueueFlushTask: enqueueFlushTask,
		buffer:           newTableBatches(),
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	ticker := time.NewTicker(c.config.FlushInterval)
	defer ticker.Stop()

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
					if err := c.emitFlushTask(ctx, dispatcherBatch, flushReasonBarrier); err != nil {
						return err
					}
				}
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
			return nil
		}
		return c.emitTableBatch(ctx, version, flushReasonSize)
	}
}

func (c *bufferManager) emitBatch(ctx context.Context, reason string) error {
	if c.buffer.isEmpty() {
		return nil
	}

	if err := c.emitFlushTask(ctx, c.buffer, reason); err != nil {
		return err
	}

	c.buffer = newTableBatches()
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
	if err := c.emitFlushTask(ctx, tableBatch, reason); err != nil {
		return err
	}
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

type tableBatches struct {
	tables map[cloudstorage.VersionedTableName]*tableBatch
	nBytes uint64
}

type tableBatch struct {
	size      uint64
	tableInfo *common.TableInfo
	entries   []*spool.Entry
}

func newTableBatches() tableBatches {
	return tableBatches{
		tables: make(map[cloudstorage.VersionedTableName]*tableBatch),
	}
}

func (t *tableBatches) isEmpty() bool {
	return len(t.tables) == 0
}

func (t *tableBatches) addEntry(event *task, entry *spool.Entry) {
	table := event.versionedTable
	if _, ok := t.tables[table]; !ok {
		t.tables[table] = &tableBatch{
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

func (t *tableBatches) detachByTable(version cloudstorage.VersionedTableName) (tableBatches, error) {
	tableTask := t.tables[version]
	if tableTask == nil {
		return tableBatches{}, errors.ErrInternalCheckFailed.GenWithStack(
			"table batch not found: %+v",
			version,
		)
	}
	delete(t.tables, version)
	t.nBytes -= tableTask.size

	return tableBatches{
		tables: map[cloudstorage.VersionedTableName]*tableBatch{version: tableTask},
		nBytes: tableTask.size,
	}, nil
}

func (t *tableBatches) detachByDispatcher(dispatcherID common.DispatcherID) tableBatches {
	detached := newTableBatches()
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

func (c *bufferManager) emitFlushTask(ctx context.Context, batch tableBatches, reason string) error {
	batches := make([]tablePayload, 0, len(batch.tables))
	for table, tableTask := range batch.tables {
		payload, err := c.buildPayload(tableTask)
		if err != nil {
			return err
		}
		batches = append(batches, tablePayload{
			table:   table,
			payload: payload,
		})
	}

	if err := c.enqueueFlushTask(ctx, flushTask{batches: batches}); err != nil {
		return err
	}
	metrics.CloudStorageFlushReasonCounter.WithLabelValues(c.changeFeedID.Keyspace(), c.changeFeedID.Name(), reason).Inc()
	return nil
}

func (c *bufferManager) buildPayload(batch *tableBatch) (*payload, error) {
	builder := newPayloadBuilder(batch)
	for _, entry := range batch.entries {
		if err := builder.appendEntry(c.spool, entry); err != nil {
			return nil, err
		}
	}
	return builder.Build(), nil
}

type payloadBuilder struct {
	buf                bytes.Buffer
	batch              *tableBatch
	rowsCount          int
	nBytes             int64
	shouldWriteKey     bool
	postFlushCallbacks []func()
}

func newPayloadBuilder(batch *tableBatch) *payloadBuilder {
	builder := &payloadBuilder{}
	builder.batch = batch
	builder.shouldWriteKey = true
	builder.buf.Grow(int(batch.size))
	return builder
}

func (b *payloadBuilder) Build() *payload {
	return &payload{
		tableInfo:          b.batch.tableInfo,
		data:               b.buf.Bytes(),
		rowsCount:          b.rowsCount,
		nBytes:             b.nBytes,
		entries:            b.batch.entries,
		postFlushCallbacks: b.postFlushCallbacks,
	}
}

func (b *payloadBuilder) appendEntry(spoolBuffer *spool.Spool, entry *spool.Entry) error {
	reader, err := spoolBuffer.NewMessageReader(entry)
	if err != nil {
		return err
	}
	for {
		key, value, rowCount, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		// Only the first encoded message contributes the leading key/header.
		if b.shouldWriteKey {
			b.shouldWriteKey = false
			if key != nil {
				b.buf.Write(key)
				b.nBytes += int64(len(key))
			}
		}
		b.buf.Write(value)
		b.nBytes += int64(len(value))
		b.rowsCount += rowCount
	}
	b.postFlushCallbacks = append(b.postFlushCallbacks, reader.PostFlushCallbacks()...)
	return nil
}
