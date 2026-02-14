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
	"time"

	spoolpkg "github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	pkgcloudstorage "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// dmlWriters coordinates encoding and output shard writers.
type dmlWriters struct {
	ctx          context.Context
	changefeedID commonType.ChangeFeedID
	statistics   *metrics.Statistics

	// msgCh is a channel to hold task.
	// The caller of WriteEvents will write tasks to msgCh and
	// encoding pipelines will read tasks from msgCh to encode events.
	msgCh *chann.UnlimitedChannel[*task, any]

	encodeGroup *encodingGroup

	writers []*writer
	spool   *spoolpkg.Manager
}

func newDMLWriters(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *pkgcloudstorage.Config,
	encoderConfig *common.Config,
	extension string,
	statistics *metrics.Statistics,
) (*dmlWriters, error) {
	spoolManager, err := spoolpkg.New(changefeedID, config.SpoolDiskQuota, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	messageCh := chann.NewUnlimitedChannelDefault[*task]()
	encoderGroup := newEncodingGroup(
		changefeedID,
		encoderConfig,
		defaultEncodingConcurrency,
		config.WorkerCount,
	)

	writers := make([]*writer, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		writers[i] = newWriter(i, changefeedID, storage, config, extension, statistics, spoolManager)
	}

	return &dmlWriters{
		ctx:          ctx,
		changefeedID: changefeedID,
		statistics:   statistics,
		msgCh:        messageCh,
		encodeGroup:  encoderGroup,
		writers:      writers,
		spool:        spoolManager,
	}, nil
}

func (d *dmlWriters) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		<-ctx.Done()
		d.msgCh.Close()
		return nil
	})

	eg.Go(func() error {
		return d.submitTaskToEncoder(ctx)
	})

	eg.Go(func() error {
		return d.encodeGroup.Run(ctx)
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
			return writer.Run(ctx)
		})
	}
	return eg.Wait()
}

func (d *dmlWriters) submitTaskToEncoder(ctx context.Context) error {
	for {
		taskValue, ok := d.msgCh.Get()
		if !ok {
			return nil
		}
		if err := d.encodeGroup.Add(ctx, taskValue); err != nil {
			return err
		}
	}
}

func (d *dmlWriters) dispatchTaskToWriter(ctx context.Context, outputIndex int) error {
	writerShard := d.writers[outputIndex]
	defer writerShard.closeInput()

	return d.encodeGroup.ConsumeOutputShard(ctx, outputIndex, func(future *taskFuture) error {
		// Principle: downstream must observe encode completion before consuming task payload.
		if err := future.Ready(ctx); err != nil {
			return err
		}

		taskValue := future.task
		if d.spool != nil && !taskValue.isDrainTask() {
			// Invariant: after task is materialized in spool, encodedMsgs can be released
			// to cap memory footprint in writer path.
			entry, err := d.spool.Enqueue(taskValue.encodedMsgs, taskValue.event.PostEnqueue)
			if err != nil {
				return err
			}
			taskValue.spoolEntry = entry
			taskValue.encodedMsgs = nil
		}

		return writerShard.enqueueTask(ctx, taskValue)
	})
}

func (d *dmlWriters) AddDMLEvent(event *commonEvent.DMLEvent) {
	table := pkgcloudstorage.VersionedTableName{
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

func (d *dmlWriters) PassBlockEvent(event commonEvent.BlockEvent) error {
	if event == nil {
		return nil
	}

	start := time.Now()
	defer sinkmetrics.CloudStorageDDLDrainDurationHistogram.WithLabelValues(
		d.changefeedID.Keyspace(),
		d.changefeedID.ID().String(),
	).Observe(time.Since(start).Seconds())

	doneCh := make(chan error, 1)
	// Invariant for DDL ordering:
	// marker follows the same dispatcher route and is acked only after prior tasks
	// in that route are fully drained by writer.
	d.msgCh.Push(newDrainTask(event.GetDispatcherID(), event.GetCommitTs(), doneCh))

	select {
	case err := <-doneCh:
		return err
	case <-d.ctx.Done():
		return errors.Trace(d.ctx.Err())
	}
}

func (d *dmlWriters) close() {
	d.msgCh.Close()
	for _, w := range d.writers {
		w.close()
	}
	if d.spool != nil {
		d.spool.Close()
	}
}
