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
package worker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/worker/writer"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

// CloudStorageDMLWorker denotes a worker responsible for writing messages to cloud storage.
type CloudStorageDMLWorker struct {
	changefeedID commonType.ChangeFeedID
	storage      storage.ExternalStorage
	config       *cloudstorage.Config
	statistics   *metrics.Statistics

	// last sequence number
	lastSeqNum uint64
	// workers defines a group of workers for encoding events.
	workers []*writer.Worker
	writers []*writer.Writer
	// defragmenter is used to defragment the out-of-order encoded messages and
	// sends encoded messages to individual dmlWorkers.
	defragmenter *writer.Defragmenter
	alive        struct {
		sync.RWMutex
		// msgCh is a channel to hold eventFragment.
		// The caller of WriteEvents will write eventFragment to msgCh and
		// the encodingWorkers will read eventFragment from msgCh to encode events.
		msgCh  *chann.DrainableChann[writer.EventFragment]
		isDead bool
	}
}

func NewCloudStorageDMLWorker(
	changefeedID commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	config *cloudstorage.Config,
	encoderConfig *common.Config,
	extension string,
	statistics *metrics.Statistics,
) (*CloudStorageDMLWorker, error) {
	w := &CloudStorageDMLWorker{
		changefeedID: changefeedID,
		storage:      storage,
		config:       config,
		statistics:   statistics,
		workers:      make([]*writer.Worker, defaultEncodingConcurrency),
		writers:      make([]*writer.Writer, config.WorkerCount),
	}
	w.alive.msgCh = chann.NewAutoDrainChann[writer.EventFragment]()
	encodedOutCh := make(chan writer.EventFragment, defaultChannelSize)
	workerChannels := make([]*chann.DrainableChann[writer.EventFragment], config.WorkerCount)
	// create a group of encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoderBuilder, err := codec.NewTxnEventEncoder(encoderConfig)
		if err != nil {
			return nil, err
		}
		w.workers[i] = writer.NewWorker(i, w.changefeedID, encoderBuilder, w.alive.msgCh.Out(), encodedOutCh)
	}
	// create a group of dml workers.
	for i := 0; i < w.config.WorkerCount; i++ {
		inputCh := chann.NewAutoDrainChann[writer.EventFragment]()
		w.writers[i] = writer.NewWriter(i, w.changefeedID, storage, config, extension,
			inputCh, w.statistics)
		workerChannels[i] = inputCh
	}
	// create defragmenter.
	// The defragmenter is used to defragment the out-of-order encoded messages from encoding workers and
	// sends encoded messages to related dmlWorkers in order. Messages of the same table will be sent to
	// the same dml
	w.defragmenter = writer.NewDefragmenter(encodedOutCh, workerChannels)

	return w, nil
}

// run creates a set of background goroutines.
func (w *CloudStorageDMLWorker) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	for i := 0; i < len(w.workers); i++ {
		encodingWorker := w.workers[i]
		eg.Go(func() error {
			return encodingWorker.Run(ctx)
		})
	}

	eg.Go(func() error {
		return w.defragmenter.Run(ctx)
	})

	for i := 0; i < len(w.writers); i++ {
		worker := w.writers[i]
		eg.Go(func() error {
			return worker.Run(ctx)
		})
	}

	return eg.Wait()
}

func (w *CloudStorageDMLWorker) AddDMLEvent(event *commonEvent.DMLEvent) {
	w.alive.RLock()
	defer w.alive.RUnlock()
	if w.alive.isDead {
		log.Error("dead dmlSink", zap.Error(errors.Trace(errors.New("dead dmlSink"))))
		return
	}

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
	seq := atomic.AddUint64(&w.lastSeqNum, 1)

	w.statistics.RecordBatchExecution(func() (int, int64, error) {
		// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
		w.alive.msgCh.In() <- writer.NewEventFragment(seq, tbl, event)
		return int(event.Len()), event.GetRowsSize(), nil
	})
}

func (w *CloudStorageDMLWorker) Close() {
	w.alive.Lock()
	w.alive.isDead = true
	w.alive.msgCh.CloseAndDrain()
	w.alive.Unlock()

	for _, worker := range w.workers {
		worker.Close()
	}

	for _, writer := range w.writers {
		writer.Close()
	}
}
