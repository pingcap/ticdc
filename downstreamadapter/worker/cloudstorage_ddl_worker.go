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
	"encoding/json"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/robfig/cron"
	"go.uber.org/zap"
)

type CloudStorageDDLWorker struct {
	changefeedID commonType.ChangeFeedID
	sinkURI      *url.URL
	statistics   *metrics.Statistics
	storage      storage.ExternalStorage
	cfg          *cloudstorage.Config
	cron         *cron.Cron

	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time
	tableSchemaStore         *util.TableSchemaStore

	cleanupJobs []func() /* only for test */
}

// NewCloudStorageDDLWorker return a ddl worker instance.
func NewCloudStorageDDLWorker(
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	cfg *cloudstorage.Config,
	cleanupJobs []func(),
	storage storage.ExternalStorage,
	statistics *metrics.Statistics,
) *CloudStorageDDLWorker {
	return &CloudStorageDDLWorker{
		changefeedID:             changefeedID,
		sinkURI:                  sinkURI,
		cfg:                      cfg,
		cleanupJobs:              cleanupJobs,
		storage:                  storage,
		statistics:               statistics,
		lastSendCheckpointTsTime: time.Now(),
	}
}

func (w *CloudStorageDDLWorker) Run(ctx context.Context) error {
	if err := w.initCron(ctx, w.sinkURI, w.cleanupJobs); err != nil {
		return errors.Trace(err)
	}
	w.bgCleanup(ctx)
	return nil
}

func (w *CloudStorageDDLWorker) WriteBlockEvent(event *commonEvent.DDLEvent) error {
	var def cloudstorage.TableDefinition
	def.FromDDLEvent(event, w.cfg.OutputColumnID)
	if err := w.writeFile(event, def); err != nil {
		return err
	}
	if event.GetDDLType() == model.ActionExchangeTablePartition {
		// For exchange partition, we need to write the schema of the source table.
		var sourceTableDef cloudstorage.TableDefinition
		schemaName := event.GetPrevSchemaName()
		tableName := event.GetPrevTableName()
		sourceTableDef.FromTableInfo(schemaName, tableName, event.PrevTableInfo, event.GetCommitTs(), w.cfg.OutputColumnID)
		w.writeFile(event, sourceTableDef)
	}
	event.PostFlush()
	return nil
}

func (w *CloudStorageDDLWorker) AddCheckpointTs(ts uint64) {
	if time.Since(w.lastSendCheckpointTsTime) < 2*time.Second {
		log.Debug("skip write checkpoint ts to external storage",
			zap.Any("changefeedID", w.changefeedID),
			zap.Uint64("ts", ts))
		return
	}

	defer func() {
		w.lastSendCheckpointTsTime = time.Now()
		w.lastCheckpointTs.Store(ts)
	}()
	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		log.Error("CloudStorageSink marshal checkpoint-ts failed",
			zap.String("namespace", w.changefeedID.Namespace()),
			zap.String("changefeed", w.changefeedID.Name()),
			zap.Error(err))
		return
	}
	err = w.storage.WriteFile(context.Background(), "metadata", ckpt)
	if err != nil {
		log.Error("CloudStorageSink storage write file failed",
			zap.String("namespace", w.changefeedID.Namespace()),
			zap.String("changefeed", w.changefeedID.Name()),
			zap.Error(err))
	}
}

func (w *CloudStorageDDLWorker) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.tableSchemaStore = tableSchemaStore
}

func (w *CloudStorageDDLWorker) Close() {
	if w.cron != nil {
		w.cron.Stop()
	}
}

func (w *CloudStorageDDLWorker) writeFile(v *commonEvent.DDLEvent, def cloudstorage.TableDefinition) error {
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
	return w.statistics.RecordDDLExecution(func() error {
		err1 := w.storage.WriteFile(context.Background(), path, encodedDef)
		if err1 != nil {
			return err1
		}
		return nil
	})
}

func (w *CloudStorageDDLWorker) initCron(
	ctx context.Context, sinkURI *url.URL, cleanupJobs []func(),
) (err error) {
	if cleanupJobs == nil {
		cleanupJobs = w.genCleanupJob(ctx, sinkURI)
	}

	w.cron = cron.New()
	for _, job := range cleanupJobs {
		err = w.cron.AddFunc(w.cfg.FileCleanupCronSpec, job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *CloudStorageDDLWorker) bgCleanup(ctx context.Context) {
	if w.cfg.DateSeparator != config.DateSeparatorDay.String() || w.cfg.FileExpirationDays <= 0 {
		log.Info("skip cleanup expired files for storage sink",
			zap.String("namespace", w.changefeedID.Namespace()),
			zap.Stringer("changefeedID", w.changefeedID.ID()),
			zap.String("dateSeparator", w.cfg.DateSeparator),
			zap.Int("expiredFileTTL", w.cfg.FileExpirationDays))
		return
	}

	w.cron.Start()
	defer w.cron.Stop()
	log.Info("start schedule cleanup expired files for storage sink",
		zap.String("namespace", w.changefeedID.Namespace()),
		zap.Stringer("changefeedID", w.changefeedID.ID()),
		zap.String("dateSeparator", w.cfg.DateSeparator),
		zap.Int("expiredFileTTL", w.cfg.FileExpirationDays))

	// wait for the context done
	<-ctx.Done()
	log.Info("stop schedule cleanup expired files for storage sink",
		zap.String("namespace", w.changefeedID.Namespace()),
		zap.Stringer("changefeedID", w.changefeedID.ID()),
		zap.Error(ctx.Err()))
}

func (w *CloudStorageDDLWorker) genCleanupJob(ctx context.Context, uri *url.URL) []func() {
	ret := []func(){}

	isLocal := uri.Scheme == "file" || uri.Scheme == "local" || uri.Scheme == ""
	isRemoveEmptyDirsRuning := atomic.Bool{}
	if isLocal {
		ret = append(ret, func() {
			if !isRemoveEmptyDirsRuning.CompareAndSwap(false, true) {
				log.Warn("remove empty dirs is already running, skip this round",
					zap.String("namespace", w.changefeedID.Namespace()),
					zap.Stringer("changefeedID", w.changefeedID.ID()))
				return
			}

			checkpointTs := w.lastCheckpointTs.Load()
			start := time.Now()
			cnt, err := cloudstorage.RemoveEmptyDirs(ctx, w.changefeedID, uri.Path)
			if err != nil {
				log.Error("failed to remove empty dirs",
					zap.String("namespace", w.changefeedID.Namespace()),
					zap.Stringer("changefeedID", w.changefeedID.ID()),
					zap.Uint64("checkpointTs", checkpointTs),
					zap.Duration("cost", time.Since(start)),
					zap.Error(err),
				)
				return
			}
			log.Info("remove empty dirs",
				zap.String("namespace", w.changefeedID.Namespace()),
				zap.Stringer("changefeedID", w.changefeedID.ID()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Uint64("count", cnt),
				zap.Duration("cost", time.Since(start)))
		})
	}

	isCleanupRunning := atomic.Bool{}
	ret = append(ret, func() {
		if !isCleanupRunning.CompareAndSwap(false, true) {
			log.Warn("cleanup expired files is already running, skip this round",
				zap.String("namespace", w.changefeedID.Namespace()),
				zap.Stringer("changefeedID", w.changefeedID.ID()))
			return
		}

		defer isCleanupRunning.Store(false)
		start := time.Now()
		checkpointTs := w.lastCheckpointTs.Load()
		cnt, err := cloudstorage.RemoveExpiredFiles(ctx, w.changefeedID, w.storage, w.cfg, checkpointTs)
		if err != nil {
			log.Error("failed to remove expired files",
				zap.String("namespace", w.changefeedID.Namespace()),
				zap.Stringer("changefeedID", w.changefeedID.ID()),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Duration("cost", time.Since(start)),
				zap.Error(err),
			)
			return
		}
		log.Info("remove expired files",
			zap.String("namespace", w.changefeedID.Namespace()),
			zap.Stringer("changefeedID", w.changefeedID.ID()),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("count", cnt),
			zap.Duration("cost", time.Since(start)))
	})
	return ret
}
