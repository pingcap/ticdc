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
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/robfig/cron"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// sink is the top-level runtime object of the cloud storage sink.
// It coordinates three paths that run at the same time:
//  1. DML events enter through AddDMLEvent, it's encoded into messages, and buffered
//     in the spool before it's flushed to external storage as data and index files.
//  2. Block events is only DDL event. FlushDMLBeforeBlock is called first, it make sure
//     all dml events belongs to the specified dispatcher is flushed to the downstream.
//     After that, WriteBlockEvent is called, it writes the DDL events directly to external storage.
//  3. Checkpoint ts updates periodically.
//  4. Background cleanup runs periodically, cleanup only removes expired files according
//     to persisted storage state.
type sink struct {
	changefeedID common.ChangeFeedID
	cfg          *cloudstorage.Config
	sinkURI      *url.URL
	// todo: this field is not take effects yet, should be fixed.
	outputRawChangeEvent bool
	storage              storeapi.Storage

	dmlWriters *dmlWriters

	// checkpointChan is a bounded best-effort queue. It is not closed
	// explicitly; both senders and the background checkpoint worker stop on ctx.
	checkpointChan           chan uint64
	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time

	cron       *cron.Cron
	statistics *metrics.Statistics

	isNormal    *atomic.Bool
	cleanupJobs []func() /* only for test */

	// some method lack of the context parameter,
	// we have to use the context from the struct to perceive the context done from the upper layer
	// To perceive the context done from the upper layer
	// it's the same as the context passed into the Run method.
	ctx context.Context
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig, enableTableAcrossNodes bool) error {
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig, enableTableAcrossNodes)
	if err != nil {
		return err
	}
	protocol, err := helper.GetProtocol(util.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return err
	}
	_, err = helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt, math.MaxInt)
	if err != nil {
		return err
	}
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
	if err != nil {
		return err
	}
	storage.Close()
	return nil
}

//nolint:revive // Keep the constructor shape consistent with other sink implementations.
func New(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig, enableTableAcrossNodes bool,
	cleanupJobs []func(), /* only for test */
	keyspaceID uint32,
) (*sink, error) {
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig, enableTableAcrossNodes)
	if err != nil {
		return nil, err
	}
	// fetch protocol from replicaConfig defined by changefeed config file.
	protocol, err := helper.GetProtocol(util.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return nil, err
	}
	// get cloud storage file extension according to the specific protocol.
	ext := helper.GetFileExtension(protocol)
	// Message size limits are mainly for MQ batch protocols. Cloud storage uses
	// max int for both the final message limit and the batch threshold.
	encoderConfig, err := helper.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt, math.MaxInt)
	if err != nil {
		return nil, err
	}
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}
	statistics := metrics.NewStatistics(changefeedID, keyspaceID, "cloudstorage")
	defer func() {
		if err != nil {
			statistics.Close()
			storage.Close()
		}
	}()
	dmlWriters, err := newDMLWriters(changefeedID, storage, cfg, encoderConfig, ext, statistics)
	if err != nil {
		return nil, err
	}
	return &sink{
		changefeedID:             changefeedID,
		sinkURI:                  sinkURI,
		cfg:                      cfg,
		cleanupJobs:              cleanupJobs,
		storage:                  storage,
		dmlWriters:               dmlWriters,
		checkpointChan:           make(chan uint64, 16),
		lastSendCheckpointTsTime: time.Now(),
		outputRawChangeEvent:     sinkConfig.CloudStorageConfig.GetOutputRawChangeEvent(),
		statistics:               statistics,
		isNormal:                 atomic.NewBool(true),

		ctx: ctx,
	}, nil
}

func (s *sink) SinkType() common.SinkType {
	return common.CloudStorageSinkType
}

// Run the sink, the ctx is the same as the ctx in the New function.
func (s *sink) Run(ctx context.Context) error {
	defer func() {
		s.isNormal.Store(false)
	}()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.dmlWriters.run(ctx)
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
	if !s.IsNormal() {
		log.Warn("ignore dml event because sink is not normal",
			zap.String("keyspace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.String("dispatcher", event.GetDispatcherID().String()))
		return
	}
	s.dmlWriters.addDMLEvent(event)
}

func (s *sink) FlushDMLBeforeBlock(event commonEvent.BlockEvent) error {
	if !s.IsNormal() {
		return errors.ErrInternalCheckFailed.GenWithStack("cloudstorage sink is not normal")
	}
	if err := s.dmlWriters.flushDMLBeforeBlock(s.ctx, event); err != nil {
		s.isNormal.Store(false)
		return err
	}
	return nil
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	if !s.IsNormal() {
		return errors.ErrInternalCheckFailed.GenWithStack("cloudstorage sink is not normal")
	}
	var err error
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		err = s.writeDDLEvent(e)
	default:
		log.Error("cloudstorage sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Keyspace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.String("eventType", commonEvent.TypeToString(event.GetType())))
		return errors.ErrInvalidEventType.GenWithStackByArgs(commonEvent.TypeToString(event.GetType()))
	}
	if err != nil {
		s.isNormal.Store(false)
		return err
	}
	event.PostFlush()
	return nil
}

func (s *sink) writeDDLEvent(event *commonEvent.DDLEvent) error {
	// For exchange partition, we need to write the schema of the source table.
	// write the previous table first
	if event.GetDDLType() == model.ActionExchangeTablePartition {
		if len(event.MultipleTableInfos) < 2 || event.MultipleTableInfos[1] == nil {
			return errors.ErrInternalCheckFailed.GenWithStackByArgs(
				"invalid exchange partition ddl event, source table info is missing")
		}
		sourceTableInfo := event.MultipleTableInfos[1]

		schemaEvent := *event
		schemaEvent.TableInfo = event.TableInfo.CloneWithRouting(
			event.GetTargetExtraSchemaName(),
			event.GetTargetExtraTableName(),
		)
		var schemaFile cloudstorage.SchemaFile
		schemaFile.Build(&schemaEvent, s.cfg.OutputColumnID)
		if err := s.writeFile(event, schemaFile); err != nil {
			return err
		}
		sourceEvent := *event
		sourceEvent.TableInfo = sourceTableInfo.CloneWithRouting(
			event.GetTargetSchemaName(),
			event.GetTargetTableName(),
		)
		var sourceSchemaFile cloudstorage.SchemaFile
		sourceSchemaFile.Build(&sourceEvent, s.cfg.OutputColumnID)
		// Source schema file carries table structure only. The DDL is replayed
		// from the exchanged table schema file.
		sourceSchemaFile.Query = ""
		sourceSchemaFile.Type = 0
		if err := s.writeFile(&sourceEvent, sourceSchemaFile); err != nil {
			return err
		}
	}
	if event.GetDDLType() != model.ActionExchangeTablePartition {
		for _, e := range event.GetEvents() {
			var schemaFile cloudstorage.SchemaFile
			schemaFile.Build(e, s.cfg.OutputColumnID)
			if err := s.writeFile(e, schemaFile); err != nil {
				return err
			}
		}
	}
	log.Info("storage sink executed ddl event",
		zap.String("keyspace", s.changefeedID.Keyspace()),
		zap.String("changefeed", s.changefeedID.Name()),
		zap.String("schema", event.GetTargetSchemaName()),
		zap.String("table", event.GetTargetTableName()),
		zap.String("dispatcher", event.GetDispatcherID().String()),
		zap.String("query", event.GetDDLQuery()),
		zap.Uint64("finishedTs", event.GetCommitTs()),
		zap.Stringer("ddlType", event.GetDDLType()))
	return nil
}

func (s *sink) writeFile(v *commonEvent.DDLEvent, schemaFile cloudstorage.SchemaFile) error {
	// skip write database-level event for 'use-table-id-as-path' mode
	if s.cfg.UseTableIDAsPath && schemaFile.Table == "" {
		return nil
	}
	encodedSchemaFile := schemaFile.Marshal()
	path := schemaFile.Path(s.cfg.UseTableIDAsPath, v.GetTableID())
	return s.statistics.RecordDDLExecution(func() (string, error) {
		err := s.storage.WriteFile(s.ctx, path, encodedSchemaFile)
		if err != nil {
			return "", err
		}
		return v.GetDDLType().String(), nil
	})
}

func (s *sink) AddCheckpointTs(ts uint64) {
	if !s.IsNormal() {
		return
	}
	select {
	case s.checkpointChan <- ts:
	case <-s.ctx.Done():
		return
		// We can just drop the checkpoint ts if the channel is full to avoid blocking since the  checkpointTs will come indefinitely
	default:
	}
}

func (s *sink) sendCheckpointTs(ctx context.Context) error {
	var (
		keyspace   = s.changefeedID.Keyspace()
		changefeed = s.changefeedID.Name()
	)
	checkpointTsMessageDuration := metrics.CheckpointTsMessageDuration.WithLabelValues(keyspace, changefeed)
	checkpointTsMessageCount := metrics.CheckpointTsMessageCount.WithLabelValues(keyspace, changefeed)
	defer func() {
		metrics.CheckpointTsMessageDuration.DeleteLabelValues(keyspace, changefeed)
		metrics.CheckpointTsMessageCount.DeleteLabelValues(keyspace, changefeed)
	}()

	var checkpoint uint64
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case checkpoint = <-s.checkpointChan:
		}

		if checkpoint < s.lastCheckpointTs.Load() {
			continue
		}

		if time.Since(s.lastSendCheckpointTsTime) < 2*time.Second {
			continue
		}

		start := time.Now()
		message, err := json.Marshal(map[string]uint64{"checkpoint-ts": checkpoint})
		if err != nil {
			log.Panic("cloud storage sink marshal checkpoint failed, this should never happen",
				zap.String("keyspace", keyspace),
				zap.String("changefeed", changefeed),
				zap.Uint64("checkpoint", checkpoint),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		}
		err = s.storage.WriteFile(ctx, "metadata", message)
		if err != nil {
			log.Error("cloud storage sink write file failed",
				zap.String("keyspace", keyspace),
				zap.String("changefeed", changefeed),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
			return err
		}
		s.lastSendCheckpointTsTime = time.Now()
		s.lastCheckpointTs.Store(checkpoint)

		checkpointTsMessageCount.Inc()
		checkpointTsMessageDuration.Observe(time.Since(start).Seconds())
	}
}

func (s *sink) SetTableSchemaStore(_ *commonEvent.TableSchemaStore) {
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
			return errors.WrapError(errors.ErrStorageSinkInvalidConfig, err, "add cloud storage cleanup job")
		}
	}
	return nil
}

func (s *sink) bgCleanup(ctx context.Context) {
	if s.cfg.DateSeparator != config.DateSeparatorDay.String() || s.cfg.FileExpirationDays <= 0 {
		log.Info("skip cleanup expired files for storage sink",
			zap.String("keyspace", s.changefeedID.Keyspace()),
			zap.String("changefeedID", s.changefeedID.Name()),
			zap.String("dateSeparator", s.cfg.DateSeparator),
			zap.Int("expiredFileTTL", s.cfg.FileExpirationDays))
		return
	}

	s.cron.Start()
	defer s.cron.Stop()
	log.Info("start schedule cleanup expired files for storage sink",
		zap.String("keyspace", s.changefeedID.Keyspace()),
		zap.String("changefeedID", s.changefeedID.Name()),
		zap.String("dateSeparator", s.cfg.DateSeparator),
		zap.Int("expiredFileTTL", s.cfg.FileExpirationDays))

	// wait for the context done
	<-ctx.Done()
	log.Info("stop schedule cleanup expired files for storage sink",
		zap.String("keyspace", s.changefeedID.Keyspace()),
		zap.String("changefeedID", s.changefeedID.Name()),
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
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeedID", s.changefeedID.Name()))
				return
			}
			defer isRemoveEmptyDirsRunning.Store(false)

			checkpointTs := s.lastCheckpointTs.Load()
			start := time.Now()
			err := cloudstorage.RemoveEmptyDirs(ctx, s.changefeedID, uri.Path)
			if err != nil {
				log.Error("failed to remove empty dirs",
					zap.String("keyspace", s.changefeedID.Keyspace()),
					zap.String("changefeedID", s.changefeedID.Name()),
					zap.Uint64("checkpointTs", checkpointTs),
					zap.Duration("cost", time.Since(start)),
					zap.Error(err),
				)
				return
			}
			log.Info("remove empty dirs",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeedID", s.changefeedID.Name()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Duration("cost", time.Since(start)))
		})
	}

	var isCleanupRunning atomic.Bool
	ret = append(ret, func() {
		if !isCleanupRunning.CompareAndSwap(false, true) {
			log.Warn("cleanup expired files is already running, skip this round",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeedID", s.changefeedID.Name()))
			return
		}

		defer isCleanupRunning.Store(false)
		start := time.Now()
		checkpointTs := s.lastCheckpointTs.Load()
		err := cloudstorage.RemoveExpiredFiles(ctx, s.changefeedID, s.storage, s.cfg, checkpointTs)
		if err != nil {
			log.Error("failed to remove expired files",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeedID", s.changefeedID.Name()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Duration("cost", time.Since(start)),
				zap.Error(err),
			)
			return
		}
		log.Info("remove expired files",
			zap.String("keyspace", s.changefeedID.Keyspace()),
			zap.String("changefeedID", s.changefeedID.Name()),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Duration("cost", time.Since(start)))
	})
	return ret
}

func (s *sink) Close() {
	if s.dmlWriters != nil {
		s.dmlWriters.close()
	}
	if s.cron != nil {
		s.cron.Stop()
	}
	if s.statistics != nil {
		s.statistics.Close()
	}
	if s.storage != nil {
		s.storage.Close()
	}
}

func (s *sink) BatchCount() int {
	return 4096
}

func (s *sink) BatchBytes() int {
	return s.cfg.FileSize
}
