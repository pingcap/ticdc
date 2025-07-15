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
	"sync/atomic"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// dmlWriters denotes a worker responsible for writing messages to cloud storage.
type dmlWriters struct {
	changefeedID commonType.ChangeFeedID
	statistics   *metrics.Statistics

	// msgCh is a channel to hold eventFragment.
	// The caller of WriteEvents will write eventFragment to msgCh and
	// the encodingWorkers will read eventFragment from msgCh to encode events.
	msgCh       *chann.DrainableChann[eventFragment]
	encodeGroup *encodingGroup

	// defragmenter is used to defragment the out-of-order encoded messages and
	// sends encoded messages to individual dmlWorkers.
	defragmenter *defragmenter

	writers []*writer

	// last sequence number
	lastSeqNum uint64
}

func newDMLWriters(
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	encoderConfig *common.Config,
	extension string,
	statistics *metrics.Statistics,
) *dmlWriters {
	messageCh := chann.NewAutoDrainChann[eventFragment]()
	encodedOutCh := make(chan eventFragment, defaultChannelSize)
	encoderGroup := newEncodingGroup(changefeedID, encoderConfig, defaultEncodingConcurrency, messageCh.Out(), encodedOutCh)

	writers := make([]*writer, config.WorkerCount)
	writerInputChs := make([]*chann.UnlimitedChannel[eventFragment, any], config.WorkerCount)
	for i := 0; i < config.WorkerCount; i++ {
		inputCh := chann.NewUnlimitedChannel[eventFragment, any](nil, nil)
		writerInputChs[i] = inputCh
		writers[i] = newWriter(i, changefeedID, storage, config, extension, inputCh, statistics)
	}

	return &dmlWriters{
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
		return d.encodeGroup.Run(ctx)
	})

	eg.Go(func() error {
		return d.defragmenter.Run(ctx)
	})

	for i := 0; i < len(d.writers); i++ {
		eg.Go(func() error {
			return d.writers[i].Run(ctx)
		})
	}
	return eg.Wait()
}

func (d *dmlWriters) AddDMLEvent(event *commonEvent.DMLEvent) {
	if event.State != commonEvent.EventSenderStateNormal {
		// The table where the event comes from is in stopping, so it's safe
		// to drop the event directly.
		event.PostFlush()
		return
	}

	tbl := cloudstorage.VersionedTableName{
		TableNameWithPhysicTableID: commonType.TableName{
			Schema:      event.TableInfo.GetSchemaName(),
			Table:       event.TableInfo.GetTableName(),
			TableID:     event.PhysicalTableID,
			IsPartition: event.TableInfo.IsPartitionTable(),
		},
		TableInfoVersion: event.TableInfoVersion,
	}
	seq := atomic.AddUint64(&d.lastSeqNum, 1)
	_ = d.statistics.RecordBatchExecution(func() (int, int64, error) {
		// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
		d.msgCh.In() <- newEventFragment(seq, tbl, event)
		return int(event.Len()), event.GetRowsSize(), nil
	})
}

func (d *dmlWriters) close() {
	d.msgCh.CloseAndDrain()
	d.encodeGroup.close()
	for _, w := range d.writers {
		w.close()
	}
}
