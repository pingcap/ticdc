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
	"errors"
	"sync/atomic"
	"time"

	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
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

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

func newDMLWriters(
	ctx context.Context,
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

	ctx, cancel := context.WithCancel(ctx)
	return &dmlWriters{
		changefeedID: changefeedID,
		statistics:   statistics,
		msgCh:        messageCh,
		encodeGroup:  encoderGroup,
		writers:      writers,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (d *dmlWriters) run(_ context.Context) error {
	g, ctx := errgroup.WithContext(d.ctx)

	g.Go(func() error {
		<-ctx.Done()
		d.msgCh.Close()
		return nil
	})

	g.Go(func() error {
		return d.submitTaskToEncoder(ctx)
	})

	g.Go(func() error {
		return d.encodeGroup.run(ctx)
	})

	// One dispatcher goroutine per output shard.
	// Invariant: each output shard has a single consumer to keep shard ordering.
	for idx := range d.writers {
		g.Go(func() error {
			return d.dispatchTaskToWriter(ctx, idx)
		})
	}

	for i := range d.writers {
		writer := d.writers[i]
		g.Go(func() error {
			return writer.run(ctx)
		})
	}
	err := g.Wait()
	if d.closed.Load() && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (d *dmlWriters) submitTaskToEncoder(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}

		taskValue, ok, err := d.msgCh.GetWithContext(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := d.encodeGroup.add(ctx, taskValue); err != nil {
			return err
		}
	}
}

func (d *dmlWriters) dispatchTaskToWriter(ctx context.Context, outputIndex int) error {
	outputs := d.encodeGroup.Outputs()
	if outputIndex < 0 || outputIndex >= len(outputs) {
		return errors.New("cloud storage encoder group output index out of range")
	}

	outputCh := outputs[outputIndex]
	writerShard := d.writers[outputIndex]
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case future := <-outputCh:
			if err := future.Ready(ctx); err != nil {
				return err
			}
			if err := writerShard.enqueueTask(ctx, future.task); err != nil {
				return err
			}
		}
	}
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
	drainTask := newDrainTask(event.GetDispatcherID(), event.GetCommitTs())
	d.msgCh.Push(drainTask)
	return drainTask.wait(d.ctx)
}

func (d *dmlWriters) close() {
	if !d.closed.CompareAndSwap(false, true) {
		return
	}
	d.cancel()
	d.msgCh.Close()
}
