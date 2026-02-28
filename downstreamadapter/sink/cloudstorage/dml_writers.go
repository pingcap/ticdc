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

// dmlWriters denotes a worker responsible for writing messages to cloud storage.
type dmlWriters struct {
	ctx          context.Context
	changefeedID commonType.ChangeFeedID
	statistics   *metrics.Statistics

	// msgCh is a channel to hold eventFragment.
	// The caller of WriteEvents will write eventFragment to msgCh and
	// the encodingWorkers will read eventFragment from msgCh to encode events.
	msgCh       *chann.UnlimitedChannel[eventFragment, any]
	encodeGroup *encodingGroup

	// defragmenter is used to defragment the out-of-order encoded messages and
	// sends encoded messages to individual dmlWorkers.
	defragmenter *defragmenter

	writers []*writer

	// last sequence number
	lastSeqNum atomic.Uint64
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
	messageCh := chann.NewUnlimitedChannelDefault[eventFragment]()
	encodedOutCh := make(chan eventFragment, defaultChannelSize)
	encoderGroup := newEncodingGroup(changefeedID, encoderConfig, defaultEncodingConcurrency, messageCh, encodedOutCh)

	writers := make([]*writer, config.WorkerCount)
	writerInputChs := make([]*chann.DrainableChann[eventFragment], config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		inputCh := chann.NewAutoDrainChann[eventFragment]()
		writerInputChs[i] = inputCh
		writers[i] = newWriter(i, changefeedID, storage, config, extension, inputCh, statistics)
	}

	return &dmlWriters{
		ctx:          ctx,
		changefeedID: changefeedID,
		statistics:   statistics,
		msgCh:        messageCh,

		encodeGroup:  encoderGroup,
		defragmenter: newDefragmenter(encodedOutCh, writerInputChs),
		writers:      writers,
	}
}

func (d *dmlWriters) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		<-ctx.Done()
		d.msgCh.Close()
		return nil
	})

	eg.Go(func() error {
		return d.encodeGroup.Run(ctx)
	})

	eg.Go(func() error {
		return d.defragmenter.Run(ctx)
	})

	for i := range d.writers {
		writer := d.writers[i]
		eg.Go(func() error {
			return writer.Run(ctx)
		})
	}
	return eg.Wait()
}

func (d *dmlWriters) AddDMLEvent(event *commonEvent.DMLEvent) {
	tbl := cloudstorage.VersionedTableName{
		TableNameWithPhysicTableID: commonType.TableName{
			Schema:      event.TableInfo.GetSchemaName(),
			Table:       event.TableInfo.GetTableName(),
			TableID:     event.PhysicalTableID,
			IsPartition: event.TableInfo.IsPartitionTable(),
		},
		TableInfoVersion: event.TableInfoVersion,
		DispatcherID:     event.GetDispatcherID(),
	}
	seq := d.lastSeqNum.Inc()
	// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
	d.msgCh.Push(newEventFragment(seq, tbl, event.GetDispatcherID(), event))
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
	seq := d.lastSeqNum.Inc()
	// Drain marker shares the same global sequence as DML fragments.
	// Defragmenter and writer will place it after all prior fragments, so once
	// doneCh returns, previous DML of this dispatcher are already enqueued/drained.
	d.msgCh.Push(newDrainEventFragment(seq, event.GetDispatcherID(), event.GetCommitTs(), doneCh))

	select {
	case err := <-doneCh:
		return err
	case <-d.ctx.Done():
		return errors.Trace(d.ctx.Err())
	}
}

func (d *dmlWriters) close() {
	d.msgCh.Close()
	d.encodeGroup.close()
	for _, w := range d.writers {
		w.close()
	}
}
