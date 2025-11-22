// Copyright 2021 PingCAP, Inc.
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

package applier

import (
	"context"
	"math"
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	misc "github.com/pingcap/ticdc/pkg/redo/common"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	warnDuration = 3 * time.Minute
)

var (
	// In the boundary case, non-idempotent DDLs will not be executed.
	// TODO: fix this
	unsupportedDDL = map[timodel.ActionType]struct{}{
		timodel.ActionExchangeTablePartition: {},
	}
	errApplyFinished = errors.New("apply finished, can exit safely")
)

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
	Dir     string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg            *RedoApplierConfig
	rd             reader.RedoLogReader
	updateSplitter *updateEventSplitter

	mysqlSink       *mysql.Sink
	appliedDDLCount uint64

	eventsGroup     map[commonType.TableID]*eventsGroup
	tableDDLTs      map[commonType.TableID]ddlTs
	appliedLogCount uint64

	needRecoveryInfo bool
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg:              cfg,
		tableDDLTs:       make(map[commonType.TableID]ddlTs),
		eventsGroup:      make(map[commonType.TableID]*eventsGroup),
		needRecoveryInfo: true,
	}
}

// toLogReaderConfig is an adapter to translate from applier config to redo reader config
// returns storageType, *reader.toLogReaderConfig and error
func (rac *RedoApplierConfig) toLogReaderConfig() (string, *reader.LogReaderConfig, error) {
	uri, err := url.Parse(rac.Storage)
	if err != nil {
		return "", nil, errors.WrapError(errors.ErrConsistentStorage, err)
	}
	if redo.IsLocalStorage(uri.Scheme) {
		uri.Scheme = "file"
	}
	cfg := &reader.LogReaderConfig{
		URI:                *uri,
		Dir:                rac.Dir,
		UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
	}
	return uri.Scheme, cfg, nil
}

func (ra *RedoApplier) getBlockTableIDs(blockTables *commonEvent.InfluencedTables) map[int64]struct{} {
	tableIDs := make(map[int64]struct{})
	if blockTables == nil {
		for tableID := range ra.eventsGroup {
			tableIDs[tableID] = struct{}{}
		}
		return tableIDs
	}
	switch blockTables.InfluenceType {
	case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
		for tableID := range ra.eventsGroup {
			tableIDs[tableID] = struct{}{}
		}
	case commonEvent.InfluenceTypeNormal:
		for _, item := range blockTables.TableIDs {
			if item != 0 {
				tableIDs[item] = struct{}{}
			}
		}
	default:
		log.Panic("unsupported influence type", zap.Any("influenceType", blockTables.InfluenceType))
	}
	return tableIDs
}

func (ra *RedoApplier) getTableDDLTs(tableIDs ...int64) ddlTs {
	// we have to refactor redo apply to tolerate DDL execution errors.
	// Besides, we have to query ddl_ts to get the correct checkpointTs to avoid inconsistency.
	minTs := ddlTs{ts: math.MaxInt64}
	for _, tableID := range tableIDs {
		currentTs, ok := ra.tableDDLTs[tableID]
		if !ok {
			newStartTsList, _, skipDMLAsStartTsList, err := ra.mysqlSink.GetTableRecoveryInfo([]int64{tableID}, []int64{-1}, false)
			if err != nil || len(newStartTsList) < 1 {
				log.Panic("get startTs list failed", zap.Any("tableID", tableID), zap.Any("newStartTsList", newStartTsList), zap.Error(err))
			}
			currentTs = ddlTs{
				ts:      newStartTsList[0],
				skipDML: skipDMLAsStartTsList[0],
			}
			log.Info("calculate real startTs for redo apply",
				zap.Stringer("changefeedID", ra.rd.GetChangefeedID()),
				zap.Int64("tableID", tableID),
				zap.Any("realStartTs", newStartTsList[0]),
				zap.Any("skipDML", skipDMLAsStartTsList[0]),
			)
			ra.tableDDLTs[tableID] = currentTs
		}
		if minTs.ts > currentTs.ts {
			minTs = currentTs
		}
	}
	return minTs
}

func (ra *RedoApplier) consumeLogs(ctx context.Context) error {
	checkpointTs, resolvedTs, version, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	log.Info("apply redo log starts",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs),
		zap.Int("version", version))

	shouldApplyDDL := func(row *commonEvent.RedoDMLEvent, ddl *commonEvent.RedoDDLEvent) bool {
		if ddl == nil {
			return false
		} else if row == nil {
			// no more rows to apply
			return true
		}
		// If all rows before the DDL (which means row.CommitTs <= ddl.CommitTs)
		// are applied, we should apply this DDL.
		return row.Row.CommitTs > ddl.DDL.CommitTs
	}

	row, err := ra.updateSplitter.readNextRow(ctx)
	if err != nil {
		return err
	}
	ddl, err := ra.rd.ReadNextDDL(ctx)
	if err != nil {
		return err
	}
	for {
		if row == nil && ddl == nil {
			break
		}
		if shouldApplyDDL(row, ddl) {
			if err := ra.applyDDL(ctx, ddl, checkpointTs); err != nil {
				return err
			}
			if ddl, err = ra.rd.ReadNextDDL(ctx); err != nil {
				return err
			}
		} else {
			if err := ra.applyRow(row, checkpointTs); err != nil {
				return err
			}
			if row, err = ra.updateSplitter.readNextRow(ctx); err != nil {
				return err
			}
		}
	}
	// wait all tables to flush data
	for tableID := range ra.eventsGroup {
		if err := ra.waitTableFlush(ctx, tableID, resolvedTs); err != nil {
			return err
		}
	}

	log.Info("apply redo log finishes",
		zap.Uint64("appliedLogCount", ra.appliedLogCount),
		zap.Uint64("appliedDDLCount", ra.appliedDDLCount),
		zap.Uint64("currentCheckpoint", resolvedTs))
	return errApplyFinished
}

// applyDDL will check the ddl and replicate the previous dmls and ddl to downstream.
//
// Before appling DDL, we have to query the start-ts of the table,
// because some dispatchers has replicates some ddls which commit-ts is bigger than the redo meta checkpoint-ts.
// see https://github.com/pingcap/ticdc/issues/1061#issuecomment-3266230636.
//
// we could query start-ts of table by specified table id according DDL type.
// For the cross table DDL, we could query the table id of table trigger event dispatcher.
// For the other DDL, we just query the block tables' id.
// When some ddls are ignored, we also ignored the dml events of their drop tables.
// This is because some dmls maybe flush by mistake.
//
// For example:
//
// DML + DROP TABLE: If the drop table ddl has replicated the downstream,
// we can't query the start-ts and have to drop these dml events.
func (ra *RedoApplier) applyDDL(
	ctx context.Context, ddl *commonEvent.RedoDDLEvent, checkpointTs uint64,
) error {
	shouldSkip := func() bool {
		if ddl.DDL == nil {
			// Note this could only happen when using old version of cdc, and the commit ts
			// of the DDL should be equal to checkpoint ts or resolved ts.
			log.Warn("ignore DDL without table info", zap.Any("ddl", ddl))
			return true
		}

		var tableDDLTs ddlTs
		if !ra.needRecoveryInfo {
			// If the checkpointTs is equal the DDL commitTs, the DDL will apply in old arch
			// keep same with old arch.
			tableDDLTs = ddlTs{ts: int64(checkpointTs - 1)}
		} else {
			// we get start-ts from different table id according the ddl type
			switch timodel.ActionType(ddl.Type) {
			case timodel.ActionAddColumn, timodel.ActionDropColumn,
				timodel.ActionAddIndex, timodel.ActionDropIndex,
				timodel.ActionAddForeignKey, timodel.ActionDropForeignKey,
				timodel.ActionModifyColumn, timodel.ActionRebaseAutoID, timodel.ActionSetDefaultValue,
				timodel.ActionShardRowID, timodel.ActionModifyTableComment, timodel.ActionRenameIndex,
				timodel.ActionModifyTableCharsetAndCollate, timodel.ActionLockTable, timodel.ActionUnlockTable,
				timodel.ActionRepairTable, timodel.ActionSetTiFlashReplica, timodel.ActionUpdateTiFlashReplicaStatus,
				timodel.ActionAddPrimaryKey, timodel.ActionDropPrimaryKey,
				timodel.ActionAddColumns, timodel.ActionDropColumns,
				timodel.ActionModifyTableAutoIDCache, timodel.ActionRebaseAutoRandomBase, timodel.ActionAlterIndexVisibility,
				timodel.ActionAddCheckConstraint, timodel.ActionDropCheckConstraint, timodel.ActionAlterCheckConstraint,
				timodel.ActionAlterTableAttributes, timodel.ActionAlterCacheTable, timodel.ActionAlterNoCacheTable,
				timodel.ActionMultiSchemaChange, timodel.ActionAlterTTLInfo, timodel.ActionAlterTTLRemove:
				tableDDLTs = ra.getTableDDLTs(ddl.DDL.BlockTables.TableIDs...)
			case timodel.ActionExchangeTablePartition, timodel.ActionReorganizePartition,
				timodel.ActionAddTablePartition, timodel.ActionDropTablePartition, timodel.ActionTruncateTablePartition,
				timodel.ActionAlterTablePartitionAttributes, timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning,
				timodel.ActionCreateView, timodel.ActionDropView,
				timodel.ActionFlashbackCluster, timodel.ActionModifySchemaCharsetAndCollate,
				timodel.ActionCreatePlacementPolicy, timodel.ActionAlterPlacementPolicy, timodel.ActionDropPlacementPolicy,
				timodel.ActionCreateSchema, timodel.ActionDropSchema,
				timodel.ActionCreateTable, timodel.ActionDropTable, timodel.ActionTruncateTable,
				timodel.ActionRecoverTable, timodel.ActionAlterTablePlacement, timodel.ActionModifySchemaDefaultPlacement,
				timodel.ActionCreateTables, timodel.ActionRenameTables, timodel.ActionRenameTable,
				timodel.ActionAlterTablePartitionPlacement, timodel.ActionRecoverSchema:
				tableDDLTs = ra.getTableDDLTs(commonType.DDLSpanTableID)
			default:
				log.Warn("ignore unsupport DDL", zap.Any("ddl", ddl), zap.Any("type", ddl.Type))
				return true
			}
		}
		if tableDDLTs.ts >= int64(ddl.DDL.CommitTs) {
			log.Warn("ignore DDL which commit ts is less than current ts", zap.Any("ddl", ddl), zap.Any("startTs", tableDDLTs.ts))
			// Ignore the previous dml events, because the drop ddl has replicated the downstream
			// DML + Drop Table: If the drop table ddl is ignored and the previous dmls should be replicated the downstream in the past.
			if ddl.DDL.NeedDroppedTables != nil {
				dropTableIds := make([]int64, 0)
				switch ddl.DDL.NeedDroppedTables.InfluenceType {
				case commonEvent.InfluenceTypeNormal:
					dropTableIds = append(dropTableIds, ddl.DDL.NeedDroppedTables.TableIDs...)
				case commonEvent.InfluenceTypeDB:
					ids := ddl.TableSchemaStore.GetNormalTableIdsByDB(ddl.DDL.NeedDroppedTables.SchemaID)
					dropTableIds = append(dropTableIds, ids...)
				case commonEvent.InfluenceTypeAll:
					ids := ddl.TableSchemaStore.GetAllNormalTableIds()
					dropTableIds = append(dropTableIds, ids...)
				}
				for _, dropTableId := range dropTableIds {
					delete(ra.eventsGroup, dropTableId)
				}
				log.Warn("drop table dml events", zap.Any("dropTableIds", dropTableIds))
			}
			return true
		}
		// compatible with old arch
		if ra.needRecoveryInfo && ddl.DDL.CommitTs == checkpointTs {
			if _, ok := unsupportedDDL[timodel.ActionType(ddl.Type)]; ok {
				log.Error("ignore unsupported DDL", zap.Any("ddl", ddl))
				return true
			}
		}
		return false
	}
	if shouldSkip() {
		return nil
	}
	log.Warn("apply DDL", zap.Any("ddl", ddl))
	// Wait block tables to flush data before applying DDL.
	tableIDs := ra.getBlockTableIDs(ddl.DDL.BlockTables)
	for tableID := range tableIDs {
		if err := ra.waitTableFlush(ctx, tableID, ddl.DDL.CommitTs); err != nil {
			return err
		}
	}
	ra.mysqlSink.SetTableSchemaStore(ddl.TableSchemaStore)
	if err := ra.mysqlSink.WriteBlockEvent(ddl.ToDDLEvent()); err != nil {
		return err
	}
	ra.appliedDDLCount++
	return nil
}

func (ra *RedoApplier) applyRow(
	row *commonEvent.RedoDMLEvent, checkpointTs uint64,
) error {
	tableID := row.Row.Table.TableID
	if _, ok := ra.eventsGroup[tableID]; !ok {
		ra.eventsGroup[tableID] = newEventsGroup(tableID)
	}

	if row.Row.CommitTs < ra.eventsGroup[tableID].highWatermark {
		log.Panic("commit ts of redo log regressed",
			zap.Int64("tableID", tableID),
			zap.Uint64("commitTs", row.Row.CommitTs),
			zap.Any("resolvedTs", ra.eventsGroup[tableID].highWatermark))
	}

	var tableDDLTs ddlTs
	if !ra.needRecoveryInfo {
		// If the checkpointTs is equal the DML commitTs, the DML will apply in old arch
		// keep same with old arch.
		tableDDLTs = ddlTs{ts: int64(checkpointTs)}
	} else {
		tableDDLTs = ra.getTableDDLTs(tableID)
	}
	if tableDDLTs.ts >= int64(row.Row.CommitTs) {
		log.Warn("ignore the dml event since the commitTs is less than startTs", zap.Int64("ts", tableDDLTs.ts), zap.Any("row", row))
		return nil
	}
	if tableDDLTs.skipDML && int64(row.Row.CommitTs)-1 == tableDDLTs.ts {
		log.Warn("ignore the dml event since the ddl is not finished", zap.Int64("ts", tableDDLTs.ts), zap.Any("row", row))
		return nil
	}

	ra.eventsGroup[tableID].append(row.ToDMLEvent())

	ra.appliedLogCount++
	return nil
}

func (ra *RedoApplier) waitTableFlush(
	ctx context.Context, tableID commonType.TableID, rts uint64,
) error {
	group, ok := ra.eventsGroup[tableID]
	if !ok {
		log.Warn("table id not found when flush dml events", zap.Any("tableID", tableID))
		return nil
	}
	if group.highWatermark > rts {
		log.Panic("resolved ts of redo log regressed",
			zap.Any("oldResolvedTs", group.highWatermark),
			zap.Any("newResolvedTs", rts))
	}

	events := group.getEvents()
	total := len(events)
	if total == 0 {
		return nil
	}
	var flushed atomic.Int64
	done := make(chan struct{})
	for _, e := range events {
		e.AddPostFlushFunc(func() {
			if flushed.Inc() == int64(total) {
				close(done)
			}
		})
		ra.mysqlSink.AddDMLEvent(e)
	}
	// Make sure all events are flushed to downstream.
	start := time.Now()
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-done:
			log.Info("flush DML events done", zap.Uint64("resolvedTs", rts),
				zap.Int("total", total), zap.Duration("duration", time.Since(start)))
			return nil
		case <-ticker.C:
			log.Warn("DML events cannot be flushed in time", zap.Uint64("resolvedTs", rts),
				zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
		}
	}
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
	storageType, readerCfg, err := cfg.toLogReaderConfig()
	if err != nil {
		return nil, err
	}
	return reader.NewRedoLogReader(ctx, storageType, readerCfg)
}

// ReadMeta creates a new redo applier and read meta from reader
func (ra *RedoApplier) ReadMeta(ctx context.Context) (checkpointTs uint64, resolvedTs uint64, version int, err error) {
	rd, err := createRedoReader(ctx, ra.cfg)
	if err != nil {
		return 0, 0, 0, err
	}
	return rd.ReadMeta(ctx)
}

// Apply applies redo log to given target
func (ra *RedoApplier) Apply(egCtx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)
	if ra.rd, err = createRedoReader(egCtx, ra.cfg); err != nil {
		return err
	}

	sinkURI, err := url.Parse(ra.cfg.SinkURI)
	if err != nil {
		return errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	query := sinkURI.Query()
	query.Set("batch-dml-enable", "false")
	if ra.rd.GetVersion() != misc.Version {
		ra.needRecoveryInfo = false
		query.Set("enable-ddl-ts", "false")
		log.Warn("The redo log version is different the current version, enable-ddl-ts will be set to false", zap.Any("logVersion", ra.rd.GetVersion()), zap.Any("currentVersion", misc.Version))
	}
	sinkURI.RawQuery = query.Encode()
	replicaConfig := &config.ChangefeedConfig{
		SinkURI:    sinkURI.String(),
		SinkConfig: &config.SinkConfig{},
	}
	if ra.mysqlSink == nil {
		ra.mysqlSink, err = mysql.New(egCtx, ra.rd.GetChangefeedID(), replicaConfig, sinkURI)
		if err != nil {
			return err
		}
	}
	eg.Go(func() error {
		return ra.mysqlSink.Run(egCtx)
	})
	eg.Go(func() error {
		return ra.rd.Run(egCtx)
	})
	ra.updateSplitter = newUpdateEventSplitter(ra.rd, ra.cfg.Dir)

	eg.Go(func() error {
		defer ra.mysqlSink.Close(false)
		return ra.consumeLogs(egCtx)
	})

	err = eg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
