// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sink

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/defragmenter"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/pdutil"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/robfig/cron"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

// It will send the events to cloud storage systems.
// Messages are encoded in the specific protocol and then sent to the defragmenter.
// The data flow is as follows: **data** -> encodingWorkers -> defragmenter -> dmlWorkers -> external storage
// The defragmenter will defragment the out-of-order encoded messages and sends encoded
// messages to individual dmlWorkers.
// The dmlWorkers will write the encoded messages to external storage in parallel between different tables.
type CloudStorageSink struct {
	changefeedID         common.ChangeFeedID
	scheme               string
	outputRawChangeEvent bool
	// last sequence number
	lastSeqNum uint64
	// encodingWorkers defines a group of workers for encoding events.
	encodingWorkers []*worker.CloudStorageEncodingWorker
	// defragmenter is used to defragment the out-of-order encoded messages and
	// sends encoded messages to individual dmlWorkers.
	defragmenter *defragmenter.Defragmenter
	// workers defines a group of workers for writing events to external storage.
	workers []*worker.CloudStorageWorker

	alive struct {
		sync.RWMutex
		// msgCh is a channel to hold eventFragment.
		// The caller of WriteEvents will write eventFragment to msgCh and
		// the encodingWorkers will read eventFragment from msgCh to encode events.
		msgCh  *chann.DrainableChann[defragmenter.EventFragment]
		isDead bool
	}

	statistics *metrics.Statistics

	isNormal uint32

	// DDL
	storage storage.ExternalStorage
	cfg     *cloudstorage.Config
	cron    *cron.Cron

	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time
}

func verifyCloudStorageSink(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	var (
		protocol config.Protocol
		storage  storage.ExternalStorage
		err      error
	)
	cfg := cloudstorage.NewConfig()
	if err = cfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return err
	}
	if protocol, err = helper.GetProtocol(putil.GetOrZero(sinkConfig.Protocol)); err != nil {
		return err
	}
	if _, err = util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt); err != nil {
		return err
	}
	if storage, err = helper.GetExternalStorageFromURI(ctx, sinkURI.String()); err != nil {
		return err
	}
	s := &CloudStorageSink{changefeedID: changefeedID, cfg: cfg, lastSendCheckpointTsTime: time.Now()}
	if err = s.initCron(ctx, sinkURI, nil); err != nil {
		return err
	}
	storage.Close()
	return nil
}

func newCloudStorageSink(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
	cleanupJobs []func(), /* only for test */
) (*CloudStorageSink, error) {
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return nil, err
	}
	// fetch protocol from replicaConfig defined by changefeed config file.
	protocol, err := helper.GetProtocol(
		putil.GetOrZero(sinkConfig.Protocol),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get cloud storage file extension according to the specific protocol.
	ext := helper.GetFileExtension(protocol)
	// the last param maxMsgBytes is mainly to limit the size of a single message for
	// batch protocols in mq scenario. In cloud storage sink, we just set it to max int.
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storage, err := helper.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}

	s := &CloudStorageSink{
		changefeedID:             changefeedID,
		scheme:                   strings.ToLower(sinkURI.Scheme),
		outputRawChangeEvent:     sinkConfig.CloudStorageConfig.GetOutputRawChangeEvent(),
		encodingWorkers:          make([]*worker.CloudStorageEncodingWorker, defaultEncodingConcurrency),
		workers:                  make([]*worker.CloudStorageWorker, cfg.WorkerCount),
		statistics:               metrics.NewStatistics(changefeedID, "CloudStorageSink"),
		storage:                  storage,
		cfg:                      cfg,
		lastSendCheckpointTsTime: time.Now(),
	}
	s.alive.msgCh = chann.NewAutoDrainChann[defragmenter.EventFragment]()

	encodedOutCh := make(chan defragmenter.EventFragment, defaultChannelSize)
	workerChannels := make([]*chann.DrainableChann[defragmenter.EventFragment], cfg.WorkerCount)

	// create a group of encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoderBuilder, err := codec.NewTxnEventEncoder(encoderConfig)
		if err != nil {
			return nil, err
		}
		s.encodingWorkers[i] = worker.NewCloudStorageEncodingWorker(i, s.changefeedID, encoderBuilder, s.alive.msgCh.Out(), encodedOutCh)
	}

	// create a group of dml workers.
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	for i := 0; i < cfg.WorkerCount; i++ {
		inputCh := chann.NewAutoDrainChann[defragmenter.EventFragment]()
		s.workers[i] = worker.NewCloudStorageWorker(i, s.changefeedID, storage, cfg, ext,
			inputCh, pdClock, s.statistics)
		workerChannels[i] = inputCh
	}

	// create defragmenter.
	// The defragmenter is used to defragment the out-of-order encoded messages from encoding workers and
	// sends encoded messages to related dmlWorkers in order. Messages of the same table will be sent to
	// the same dmlWorker.
	s.defragmenter = defragmenter.NewDefragmenter(encodedOutCh, workerChannels)

	if err := s.initCron(ctx, sinkURI, cleanupJobs); err != nil {
		return nil, errors.Trace(err)
	}

	return s, nil
}

func (s *CloudStorageSink) SinkType() common.SinkType {
	return common.CloudStorageSinkType
}

func (s *CloudStorageSink) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	// run the encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encodingWorker := s.encodingWorkers[i]
		eg.Go(func() error {
			return encodingWorker.Run(ctx)
		})
		log.Debug("encoding worker started",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.ID().String()))
	}

	// run the defragmenter.
	eg.Go(func() error {
		return s.defragmenter.Run(ctx)
	})

	// run dml workers.
	for i := 0; i < len(s.workers); i++ {
		worker := s.workers[i]
		eg.Go(func() error {
			return worker.Run(ctx)
		})
	}

	// Note: It is intended to run the cleanup goroutine in the background.
	// we don't wait for it to finish since the gourotine would be stuck if
	// the downstream is abnormal, especially when the downstream is a nfs.
	eg.Go(func() error {
		s.bgCleanup(ctx)
		return nil
	})

	return eg.Wait()
}

func (s *CloudStorageSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *CloudStorageSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.alive.RLock()
	defer s.alive.RUnlock()
	if s.alive.isDead {
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
	seq := atomic.AddUint64(&s.lastSeqNum, 1)

	// s.statistics.ObserveRows(event.Rows...)
	// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
	s.alive.msgCh.In() <- defragmenter.EventFragment{
		SeqNumber:      seq,
		VersionedTable: tbl,
		Event:          event,
	}
}

func (s *CloudStorageSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *CloudStorageSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch v := event.(type) {
	case *commonEvent.DDLEvent:
		if v.TiDBOnly {
			// run callback directly and return
			v.PostFlush()
			return nil
		}

		writeFile := func(def cloudstorage.TableDefinition) error {
			encodedDef, err := def.MarshalWithQuery()
			if err != nil {
				return errors.Trace(err)
			}

			path, err := def.GenerateSchemaFilePath()
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("write ddl event to external storage",
				zap.String("path", path), zap.Any("ddl", v))
			return s.statistics.RecordDDLExecution(func() error {
				err1 := s.storage.WriteFile(context.Background(), path, encodedDef)
				if err1 != nil {
					return err1
				}
				return nil
			})
		}

		var def cloudstorage.TableDefinition
		def.FromDDLEvent(v, s.cfg.OutputColumnID)
		if err := writeFile(def); err != nil {
			atomic.StoreUint32(&s.isNormal, 0)
			return errors.Trace(err)
		}

		if v.GetDDLType() == model.ActionExchangeTablePartition {
			// For exchange partition, we need to write the schema of the source table.
			var sourceTableDef cloudstorage.TableDefinition
			sourceTableDef.FromTableInfo(v.MultipleTableInfos[v.Version-1], v.TableInfo.UpdateTS(), s.cfg.OutputColumnID)
			writeFile(sourceTableDef)
		}
	case *commonEvent.SyncPointEvent:
		log.Error("CloudStorageSink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("CloudStorageSink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *CloudStorageSink) AddCheckpointTs(ts uint64) {
	if time.Since(s.lastSendCheckpointTsTime) < 2*time.Second {
		log.Debug("skip write checkpoint ts to external storage",
			zap.Any("changefeedID", s.changefeedID),
			zap.Uint64("ts", ts))
		return
	}

	defer func() {
		s.lastSendCheckpointTsTime = time.Now()
		s.lastCheckpointTs.Store(ts)
	}()
	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		log.Error("CloudStorageSink marshal checkpoint-ts failed",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Error(err))
		return
	}
	err = s.storage.WriteFile(context.Background(), "metadata", ckpt)
	if err != nil {
		log.Error("CloudStorageSink storage write file failed",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Error(err))
	}
}

func (s *CloudStorageSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	// s.SetTableSchemaStore(tableSchemaStore)
}

func (s *CloudStorageSink) initCron(
	ctx context.Context, sinkURI *url.URL, cleanupJobs []func(),
) (err error) {
	if cleanupJobs == nil {
		cleanupJobs = s.genCleanupJob(ctx, sinkURI)
	}

	s.cron = cron.New()
	for _, job := range cleanupJobs {
		err = s.cron.AddFunc(s.cfg.FileCleanupCronSpec, job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *CloudStorageSink) bgCleanup(ctx context.Context) {
	if s.cfg.DateSeparator != config.DateSeparatorDay.String() || s.cfg.FileExpirationDays <= 0 {
		log.Info("skip cleanup expired files for storage sink",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.Stringer("changefeedID", s.changefeedID.ID()),
			zap.String("dateSeparator", s.cfg.DateSeparator),
			zap.Int("expiredFileTTL", s.cfg.FileExpirationDays))
		return
	}

	s.cron.Start()
	defer s.cron.Stop()
	log.Info("start schedule cleanup expired files for storage sink",
		zap.String("namespace", s.changefeedID.Namespace()),
		zap.Stringer("changefeedID", s.changefeedID.ID()),
		zap.String("dateSeparator", s.cfg.DateSeparator),
		zap.Int("expiredFileTTL", s.cfg.FileExpirationDays))

	// wait for the context done
	<-ctx.Done()
	log.Info("stop schedule cleanup expired files for storage sink",
		zap.String("namespace", s.changefeedID.Namespace()),
		zap.Stringer("changefeedID", s.changefeedID.ID()),
		zap.Error(ctx.Err()))
}

func (s *CloudStorageSink) genCleanupJob(ctx context.Context, uri *url.URL) []func() {
	ret := []func(){}

	isLocal := uri.Scheme == "file" || uri.Scheme == "local" || uri.Scheme == ""
	isRemoveEmptyDirsRuning := atomic.Bool{}
	if isLocal {
		ret = append(ret, func() {
			if !isRemoveEmptyDirsRuning.CompareAndSwap(false, true) {
				log.Warn("remove empty dirs is already running, skip this round",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.Stringer("changefeedID", s.changefeedID.ID()))
				return
			}

			checkpointTs := s.lastCheckpointTs.Load()
			start := time.Now()
			cnt, err := cloudstorage.RemoveEmptyDirs(ctx, s.changefeedID, uri.Path)
			if err != nil {
				log.Error("failed to remove empty dirs",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.Stringer("changefeedID", s.changefeedID.ID()),
					zap.Uint64("checkpointTs", checkpointTs),
					zap.Duration("cost", time.Since(start)),
					zap.Error(err),
				)
				return
			}
			log.Info("remove empty dirs",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.Stringer("changefeedID", s.changefeedID.ID()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Uint64("count", cnt),
				zap.Duration("cost", time.Since(start)))
		})
	}

	isCleanupRunning := atomic.Bool{}
	ret = append(ret, func() {
		if !isCleanupRunning.CompareAndSwap(false, true) {
			log.Warn("cleanup expired files is already running, skip this round",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.Stringer("changefeedID", s.changefeedID.ID()))
			return
		}

		defer isCleanupRunning.Store(false)
		start := time.Now()
		checkpointTs := s.lastCheckpointTs.Load()
		cnt, err := cloudstorage.RemoveExpiredFiles(ctx, s.changefeedID, s.storage, s.cfg, checkpointTs)
		if err != nil {
			log.Error("failed to remove expired files",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.Stringer("changefeedID", s.changefeedID.ID()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Duration("cost", time.Since(start)),
				zap.Error(err),
			)
			return
		}
		log.Info("remove expired files",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.Stringer("changefeedID", s.changefeedID.ID()),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("count", cnt),
			zap.Duration("cost", time.Since(start)))
	})
	return ret
}

func (s *CloudStorageSink) Close(_ bool) {
	for _, encodingWorker := range s.encodingWorkers {
		encodingWorker.Close()
	}

	for _, worker := range s.workers {
		worker.Close()
	}

	if s.statistics != nil {
		s.statistics.Close()
	}
	if s.cron != nil {
		s.cron.Stop()
	}
}
