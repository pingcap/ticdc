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
	now              func() time.Time
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
		now:              time.Now,
	}
}

func (c *bufferManager) run(ctx context.Context) error {
	var intervalFlushTimer *time.Timer
	var intervalFlushC <-chan time.Time
	defer func() {
		stopIntervalFlushTimer(intervalFlushTimer)
	}()

	for {
		var injectedErr error
		failpoint.Inject("passTickerOnce", func() {
			if intervalFlushC != nil {
				now := <-intervalFlushC
				injectedErr = c.emitExpiredBatch(ctx, now)
				intervalFlushTimer, intervalFlushC = c.resetIntervalFlushTimer(intervalFlushTimer)
			}
		})
		if injectedErr != nil {
			return injectedErr
		}

		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case now := <-intervalFlushC:
			if err := c.emitExpiredBatch(ctx, now); err != nil {
				return err
			}
			intervalFlushTimer, intervalFlushC = c.resetIntervalFlushTimer(intervalFlushTimer)
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
				intervalFlushTimer, intervalFlushC = c.resetIntervalFlushTimer(intervalFlushTimer)
				continue
			}
			timerDirty, err := c.handleDMLTask(ctx, task)
			if err != nil {
				return err
			}
			if timerDirty {
				intervalFlushTimer, intervalFlushC = c.resetIntervalFlushTimer(intervalFlushTimer)
			}
		}
	}
}

func (c *bufferManager) handleDMLTask(ctx context.Context, task *task) (bool, error) {
	if len(task.encodedMsgs) == 0 {
		task.event.PostEnqueue()
		return false, nil
	}

	timerDirty := false
	for {
		action, entry, err := c.spool.TryEnqueue(task.encodedMsgs, task.event.PostEnqueue)
		if err != nil {
			return false, err
		}
		switch action {
		case spool.EnqueueActionAcceptedOversized:
			c.addEntry(task, entry)
			return true, c.emitTableBatch(ctx, task.versionedTable, flushReasonOversize)
		case spool.EnqueueActionWaitDiskQuota:
			if err := c.emitBatch(ctx, flushReasonQuota); err != nil {
				return false, err
			}
			timerDirty = true
			if err := c.spool.WaitForDiskQuota(ctx, task.encodedMsgs); err != nil {
				return false, err
			}
			continue
		default:
			if c.addEntry(task, entry) {
				timerDirty = true
			}
		}

		flushed, err := c.emitTableBatchIfSizeReached(ctx, task.versionedTable)
		return timerDirty || flushed, err
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

func (c *bufferManager) emitExpiredBatch(ctx context.Context, now time.Time) error {
	batch := c.buffer.detachExpired(now, c.config.FlushInterval)
	if batch.isEmpty() {
		return nil
	}
	return c.emitFlushTask(ctx, batch, flushReasonInterval)
}

func (c *bufferManager) emitTableBatchIfSizeReached(
	ctx context.Context,
	table cloudstorage.VersionedTableName,
) (bool, error) {
	if c.buffer.tableSize(table) < uint64(c.config.FileSize) {
		return false, nil
	}
	return true, c.emitTableBatch(ctx, table, flushReasonSize)
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
	size         uint64
	firstEntryAt time.Time
	tableInfo    *common.TableInfo
	entries      []*spool.Entry
}

func newTableBatches() tableBatches {
	return tableBatches{
		tables: make(map[cloudstorage.VersionedTableName]*tableBatch),
	}
}

func (t *tableBatches) isEmpty() bool {
	return len(t.tables) == 0
}

func (t *tableBatches) tableSize(table cloudstorage.VersionedTableName) uint64 {
	tableTask := t.tables[table]
	if tableTask == nil {
		return 0
	}
	return tableTask.size
}

func (t *tableBatches) addEntry(event *task, entry *spool.Entry, now time.Time) bool {
	table := event.versionedTable
	created := false
	if _, ok := t.tables[table]; !ok {
		t.tables[table] = &tableBatch{
			size:         0,
			firstEntryAt: now,
			tableInfo:    event.event.TableInfo,
		}
		created = true
	}

	tableTask := t.tables[table]
	tableTask.size += entry.FileBytes()
	tableTask.entries = append(tableTask.entries, entry)
	t.nBytes += entry.FileBytes()
	return created
}

func (c *bufferManager) addEntry(event *task, entry *spool.Entry) bool {
	return c.buffer.addEntry(event, entry, c.now())
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

func (t *tableBatches) detachExpired(now time.Time, maxAge time.Duration) tableBatches {
	detached := newTableBatches()
	for version, tableTask := range t.tables {
		if maxAge > 0 && !tableTask.firstEntryAt.IsZero() &&
			now.Sub(tableTask.firstEntryAt) < maxAge {
			continue
		}
		detached.tables[version] = tableTask
		detached.nBytes += tableTask.size
		t.nBytes -= tableTask.size
		delete(t.tables, version)
	}
	return detached
}

func (t *tableBatches) nextIntervalFlushDeadline(maxAge time.Duration) (time.Time, bool) {
	var deadline time.Time
	for _, tableTask := range t.tables {
		next := tableTask.firstEntryAt
		if maxAge > 0 {
			next = next.Add(maxAge)
		}
		if deadline.IsZero() || next.Before(deadline) {
			deadline = next
		}
	}
	return deadline, !deadline.IsZero()
}

func (c *bufferManager) resetIntervalFlushTimer(timer *time.Timer) (*time.Timer, <-chan time.Time) {
	deadline, ok := c.buffer.nextIntervalFlushDeadline(c.config.FlushInterval)
	if !ok {
		stopIntervalFlushTimer(timer)
		return timer, nil
	}

	delay := deadline.Sub(c.now())
	if delay < 0 {
		delay = 0
	}
	if timer == nil {
		timer = time.NewTimer(delay)
	} else {
		stopIntervalFlushTimer(timer)
		timer.Reset(delay)
	}
	return timer, timer.C
}

func stopIntervalFlushTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
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
