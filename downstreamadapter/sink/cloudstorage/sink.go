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
	changefeedID common.ChangeFeedID
	cfg          *cloudstorage.Config
	sinkURI      *url.URL
	// todo: this field is not take effects yet, should be fixed.
	outputRawChangeEvent bool
	storage              storage.ExternalStorage

	dmlWriters *dmlWriters

	checkpointChan           chan uint64
	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time

	tableSchemaStore *util.TableSchemaStore
	cron             *cron.Cron
	statistics       *metrics.Statistics

	isNormal    *atomic.Bool
	cleanupJobs []func() /* only for test */
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
	_, err = helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return err
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
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
	encoderConfig, err := helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
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
		checkpointChan:           make(chan uint64, 16),
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
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.dmlWriters.Run(ctx)
	})

	g.Go(func() error {
		return s.sendCheckpointTs(ctx)
	})

	g.Go(func() error {
		if err := s.initCron(ctx, s.sinkURI, s.cleanupJobs); err != nil {
			return err
		}
		s.bgCleanup(ctx)
		return nil
	})
	return g.Wait()
}

func (s *sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWriters.AddDMLEvent(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	var err error
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		err = s.writeDDLEvent(e)
	default:
		log.Panic("sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	if err != nil {
		s.isNormal.Store(false)
		return err
	}
	event.PostFlush()
	return nil
}

func (s *sink) writeDDLEvent(event *commonEvent.DDLEvent) error {
	if event.TiDBOnly {
		return nil
	}
	// For exchange partition, we need to write the schema of the source table.
	// write the previous table first
	if event.GetDDLType() == model.ActionExchangeTablePartition {
		var def cloudstorage.TableDefinition
		def.FromTableInfo(event.ExtraSchemaName, event.ExtraTableName, event.TableInfo, event.FinishedTs, s.cfg.OutputColumnID)
		def.Query = event.Query
		def.Type = event.Type
		if err := s.writeFile(event, def); err != nil {
			return err
		}
		var sourceTableDef cloudstorage.TableDefinition
		sourceTableDef.FromTableInfo(event.SchemaName, event.TableName, event.MultipleTableInfos[1], event.FinishedTs, s.cfg.OutputColumnID)
		if err := s.writeFile(event, sourceTableDef); err != nil {
			return err
		}
	} else {
		for _, e := range event.GetEvents() {
			var def cloudstorage.TableDefinition
			def.FromDDLEvent(e, s.cfg.OutputColumnID)
			if err := s.writeFile(e, def); err != nil {
				return err
			}
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

func (s *sink) AddCheckpointTs(ts uint64) {
	s.checkpointChan <- ts
}

func (s *sink) sendCheckpointTs(ctx context.Context) error {
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
		metrics.CheckpointTsMessageCount.DeleteLabelValues(s.changefeedID.Namespace(), s.changefeedID.Name())
	}()

	var (
		checkpoint uint64
		ok         bool
	)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case checkpoint, ok = <-s.checkpointChan:
			if !ok {
				log.Warn("cloud storage sink checkpoint channel closed",
					zap.String("namespace", s.changefeedID.Namespace()),
					zap.String("changefeed", s.changefeedID.Name()))
				return nil
			}
		}

		if time.Since(s.lastSendCheckpointTsTime) < 2*time.Second {
			log.Warn("skip write checkpoint ts to external storage",
				zap.Any("changefeedID", s.changefeedID),
				zap.Uint64("checkpoint", checkpoint))
			continue
		}

		start := time.Now()
		message, err := json.Marshal(map[string]uint64{"checkpoint-ts": checkpoint})
		if err != nil {
			log.Panic("CloudStorageSink marshal checkpoint failed, this should never happen",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Uint64("checkpoint", checkpoint),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		}
		err = s.storage.WriteFile(ctx, "metadata", message)
		if err != nil {
			log.Error("CloudStorageSink storage write file failed",
				zap.String("namespace", s.changefeedID.Namespace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
			return errors.Trace(err)
		}
		s.lastSendCheckpointTsTime = time.Now()
		s.lastCheckpointTs.Store(checkpoint)

		checkpointTsMessageCount.Inc()
		checkpointTsMessageDuration.Observe(time.Since(start).Seconds())
	}
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.tableSchemaStore = tableSchemaStore
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
			return errors.Trace(err)
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
