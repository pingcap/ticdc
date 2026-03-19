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

	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/atomic"
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
	spool       *spool.Manager

	writers []*writer
	closed  atomic.Bool
}

func newDMLWriters(
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	encoderConfig *common.Config,
	extension string,
	statistics *metrics.Statistics,
) (*dmlWriters, error) {
	messageCh := chann.NewUnlimitedChannelDefault[*task]()
	encoderGroup := newEncoderGroup(
		encoderConfig,
		defaultEncodingConcurrency,
		config.WorkerCount,
	)
	spoolManager, err := spool.New(changefeedID, spool.WithQuotaBytes(config.SpoolDiskQuota))
	if err != nil {
		return nil, err
	}

	writers := make([]*writer, config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		writers[i] = newWriter(i, changefeedID, storage, config, extension, statistics, spoolManager)
	}

	return &dmlWriters{
		changefeedID: changefeedID,
		statistics:   statistics,
		msgCh:        messageCh,
		encodeGroup:  encoderGroup,
		spool:        spoolManager,
		writers:      writers,
	}, nil
}

func (d *dmlWriters) run(ctx context.Context) error {
	if d.spool != nil {
		defer d.spool.Close()
	}
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return d.encodeGroup.run(ctx)
	})

	g.Go(func() error {
		return d.addTasks(ctx)
	})

	outputs := d.encodeGroup.Outputs()
	for idx := range d.writers {
		writer := d.writers[idx]
		g.Go(func() error {
			return writer.run(ctx)
		})
		g.Go(func() error {
			outputCh := outputs[idx]
			for {
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case future := <-outputCh:
					if err := future.Ready(ctx); err != nil {
						return err
					}
					if err := writer.enqueueTask(ctx, future.task); err != nil {
						return err
					}
				}
			}
		})
	}
	return g.Wait()
}

func (d *dmlWriters) addTasks(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}

		task, ok, err := d.msgCh.GetWithContext(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			return nil
		}
		if err := d.encodeGroup.add(ctx, task); err != nil {
			return err
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
	d.msgCh.Push(newDMLTask(table, event))
}

func (d *dmlWriters) flushDMLBeforeBlock(ctx context.Context, event commonEvent.BlockEvent) error {
	if event == nil {
		return nil
	}

	start := time.Now()
	defer func() {
		sinkmetrics.CloudStorageDDLFlushDurationHistogram.WithLabelValues(
			d.changefeedID.Keyspace(),
			d.changefeedID.ID().String(),
		).Observe(time.Since(start).Seconds())
	}()

	// Invariant for DDL ordering:
	// marker follows the same dispatcher route and is acked only after prior tasks
	// in that route are fully flushed by writer.
	flushTask := newFlushTask(event.GetDispatcherID(), event.GetCommitTs())
	d.msgCh.Push(flushTask)
	return flushTask.wait(ctx)
}

func (d *dmlWriters) close() {
	if !d.closed.CompareAndSwap(false, true) {
		return
	}
	d.msgCh.Close()
}
