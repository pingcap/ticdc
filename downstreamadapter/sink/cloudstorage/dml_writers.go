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
	"sync"
	"time"

	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// dmlWriters coordinates encoding and output shard writers.
type dmlWriters struct {
	changefeedID commonType.ChangeFeedID
	statistics   *metrics.Statistics

	// msgCh is a channel to hold task.
	// The caller of WriteEvents will write tasks to msgCh and
	// encoding pipelines will read tasks from msgCh to encode events.
	msgCh *chann.UnlimitedChannel[*task, any]

	encodeGroup *encoderGroup

	writers []*writer

	runCtxMu sync.RWMutex
	runCtx   context.Context
}

const (
	defaultEncodingConcurrency = 8
)

func newDMLWriters(
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	encoderConfig *common.Config,
	extension string,
	statistics *metrics.Statistics,
) *dmlWriters {
	messageCh := chann.NewUnlimitedChannelDefault[*task]()
	encoderGroup := newEncoderGroup(
		encoderConfig,
		defaultEncodingConcurrency,
		config.WorkerCount,
	)

	writers := make([]*writer, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		writers[i] = newWriter(i, changefeedID, storage, config, extension, statistics)
	}

	return &dmlWriters{
		changefeedID: changefeedID,
		statistics:   statistics,
		msgCh:        messageCh,
		encodeGroup:  encoderGroup,
		writers:      writers,
	}
}

func (d *dmlWriters) run(ctx context.Context) error {
	d.setRunCtx(ctx)
	defer d.setRunCtx(nil)

	runDone := make(chan struct{})
	defer close(runDone)

	go func() {
		select {
		case <-ctx.Done():
			d.msgCh.Close()
		case <-runDone:
		}
	}()

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer d.encodeGroup.closeInput()
		return d.submitTaskToEncoder(ctx)
	})

	eg.Go(func() error {
		return d.encodeGroup.run(ctx)
	})

	// One dispatcher goroutine per output shard.
	// Invariant: each output shard has a single consumer to keep shard ordering.
	for i := range d.writers {
		index := i
		eg.Go(func() error {
			return d.dispatchTaskToWriter(ctx, index)
		})
	}

	for i := range d.writers {
		writer := d.writers[i]
		eg.Go(func() error {
			return writer.run(ctx)
		})
	}
	return eg.Wait()
}

func (d *dmlWriters) setRunCtx(ctx context.Context) {
	d.runCtxMu.Lock()
	defer d.runCtxMu.Unlock()
	d.runCtx = ctx
}

func (d *dmlWriters) getRunCtx() context.Context {
	d.runCtxMu.RLock()
	defer d.runCtxMu.RUnlock()
	return d.runCtx
}

func (d *dmlWriters) submitTaskToEncoder(ctx context.Context) error {
	for {
		taskValue, ok := d.msgCh.Get()
		if !ok {
			return nil
		}
		if err := d.encodeGroup.add(ctx, taskValue); err != nil {
			return err
		}
	}
}

func (d *dmlWriters) dispatchTaskToWriter(ctx context.Context, outputIndex int) error {
	writerShard := d.writers[outputIndex]
	defer writerShard.closeInput()

	return d.encodeGroup.consumeOutputShard(ctx, outputIndex, func(taskValue *task) error {
		return writerShard.enqueueTask(ctx, taskValue)
	})
}

func (d *dmlWriters) addDMLEvent(event *commonEvent.DMLEvent) {
	table := cloudstorage.VersionedTableName{
		TableNameWithPhysicTableID: commonType.TableName{
			Schema:      event.TableInfo.GetSchemaName(),
			Table:       event.TableInfo.GetTableName(),
			TableID:     event.PhysicalTableID,
			IsPartition: event.TableInfo.IsPartitionTable(),
		},
		TableInfoVersion: event.TableInfoVersion,
		DispatcherID:     event.GetDispatcherID(),
	}

	_ = d.statistics.RecordBatchExecution(func() (int, int64, error) {
		d.msgCh.Push(newDMLTask(table, event))
		return int(event.Len()), event.GetSize(), nil
	})
}

func (d *dmlWriters) flushDMLBeforeBlock(event commonEvent.BlockEvent) error {
	if event == nil {
		return nil
	}

	start := time.Now()
	defer sinkmetrics.CloudStorageDDLDrainDurationHistogram.WithLabelValues(
		d.changefeedID.Keyspace(),
		d.changefeedID.ID().String(),
	).Observe(time.Since(start).Seconds())

	// Invariant for DDL ordering:
	// marker follows the same dispatcher route and is acked only after prior tasks
	// in that route are fully drained by writer.
	runCtx := d.getRunCtx()
	if runCtx == nil {
		return errors.New("cloud storage dml writers is not running")
	}
	drainTask := newDrainTask(event.GetDispatcherID(), event.GetCommitTs())
	d.msgCh.Push(drainTask)
	return drainTask.wait(runCtx)
}

func (d *dmlWriters) close() {
	d.msgCh.Close()
	for _, w := range d.writers {
		w.close()
	}
}
