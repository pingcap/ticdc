// Copyright 2025 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"encoding/json"
	"math"
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/util"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/robfig/cron"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// It will send the events to cloud storage systems.
// Messages are encoded in the specific protocol and then sent to the defragmenter.
// The data flow is as follows: **data** -> encodingWorkers -> defragmenter -> dmlWorkers -> external storage
// The defragmenter will defragment the out-of-order encoded messages and sends encoded
// messages to individual dmlWorkers.
// The dmlWorkers will write the encoded messages to external storage in parallel between different tables.
type sink struct {
	changefeedID         common.ChangeFeedID
	outputRawChangeEvent bool

	cfg     *cloudstorage.Config
	storage storage.ExternalStorage

	sinkURI     *url.URL
	cleanupJobs []func() /* only for test */

	dmlWriters *dmlWriters

	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time
	tableSchemaStore         *util.TableSchemaStore
	cron                     *cron.Cron

	statistics *metrics.Statistics
	isNormal   *atomic.Bool
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return err
	}
	protocol, err := helper.GetProtocol(putil.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return err
	}
	_, err = util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return err
	}
	storage, err := helper.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return err
	}
	storage.Close()
	return nil
}

func New(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
	cleanupJobs []func(), /* only for test */
) (*sink, error) {
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
	statistics := metrics.NewStatistics(changefeedID, "cloudstorage")
	return &sink{
		changefeedID:             changefeedID,
		sinkURI:                  sinkURI,
		cfg:                      cfg,
		cleanupJobs:              cleanupJobs,
		storage:                  storage,
		dmlWriters:               newDMLWriters(changefeedID, storage, cfg, encoderConfig, ext, statistics),
		lastSendCheckpointTsTime: time.Now(),
		outputRawChangeEvent:     sinkConfig.CloudStorageConfig.GetOutputRawChangeEvent(),
		statistics:               statistics,
		isNormal:                 atomic.NewBool(true),
	}, nil
}

func (s *sink) SinkType() common.SinkType {
	return common.CloudStorageSinkType
}

func (s *sink) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.dmlWriters.Run(ctx)
	})

	eg.Go(func() error {
		if err := s.initCron(ctx, s.sinkURI, s.cleanupJobs); err != nil {
			return errors.Trace(err)
		}
		s.bgCleanup(ctx)
		return nil
	})
	return eg.Wait()
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWriters.AddDMLEvent(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		if err := s.writeDDLEvent(e); err != nil {
			s.isNormal.Store(false)
			return err
		}
		event.PostFlush()
		return nil
	default:
		log.Panic("sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *sink) writeDDLEvent(event *commonEvent.DDLEvent) error {
	if event.TiDBOnly {
		return nil
	}
	for _, e := range event.GetEvents() {
		var def cloudstorage.TableDefinition
		def.FromDDLEvent(e, s.cfg.OutputColumnID)
		if err := s.writeFile(e, def); err != nil {
			return err
		}
	}
	if event.GetDDLType() == model.ActionExchangeTablePartition {
		// For exchange partition, we need to write the schema of the source table.
		var sourceTableDef cloudstorage.TableDefinition
		sourceTableDef.FromTableInfo(event.ExtraSchemaName, event.ExtraTableName, event.MultipleTableInfos[1], event.GetCommitTs(), s.cfg.OutputColumnID)
		if err := s.writeFile(event, sourceTableDef); err != nil {
			return err
		}
	}
	return nil
}

func (s *sink) writeFile(v *commonEvent.DDLEvent, def cloudstorage.TableDefinition) error {
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
		err = s.storage.WriteFile(context.Background(), path, encodedDef)
		if err != nil {
			return err
		}
		return nil
	})
}

// todo: this method may be buggy, the error is not handled.
func (s *sink) AddCheckpointTs(ts uint64) {
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
		log.Panic("CloudStorageSink marshal checkpoint failed, this should never happen",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Uint64("checkpoint", ts),
			zap.Error(err))
	}
	err = s.storage.WriteFile(context.Background(), "metadata", ckpt)
	if err != nil {
		log.Error("CloudStorageSink storage write file failed",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Error(err))
	}
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.tableSchemaStore = tableSchemaStore
}

func (s *sink) GetStartTsList(_ []int64, startTsList []int64, _ bool) ([]int64, []bool, error) {
	return startTsList, make([]bool, len(startTsList)), nil
}

func (s *sink) initCron(
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

func (s *sink) bgCleanup(ctx context.Context) {
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

func (s *sink) genCleanupJob(ctx context.Context, uri *url.URL) []func() {
	var ret []func()

	isLocal := uri.Scheme == "file" || uri.Scheme == "local" || uri.Scheme == ""
	var isRemoveEmptyDirsRunning atomic.Bool
	if isLocal {
		ret = append(ret, func() {
			if !isRemoveEmptyDirsRunning.CompareAndSwap(false, true) {
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

	var isCleanupRunning atomic.Bool
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

func (s *sink) Close(_ bool) {
	s.dmlWriters.close()
	s.cron.Stop()
	if s.statistics != nil {
		s.statistics.Close()
	}
	s.storage.Close()
}
