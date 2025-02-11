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

package schemastore

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

var (
	WithoutTiDBOnly = false
	WithTiDBOnly    = true
)

type buildPersistedDDLEventFuncArgs struct {
	job          *model.Job
	databaseMap  map[int64]*BasicDatabaseInfo
	tableMap     map[int64]*BasicTableInfo
	partitionMap map[int64]BasicPartitionInfo
}

type updateDDLHistoryFuncArgs struct {
	ddlEvent               *PersistedDDLEvent
	databaseMap            map[int64]*BasicDatabaseInfo
	tableMap               map[int64]*BasicTableInfo
	partitionMap           map[int64]BasicPartitionInfo
	tablesDDLHistory       map[int64][]uint64
	tableTriggerDDLHistory []uint64
}

func (args *updateDDLHistoryFuncArgs) appendTableTriggerDDLHistory(ts uint64) {
	args.tableTriggerDDLHistory = append(args.tableTriggerDDLHistory, ts)
}

func (args *updateDDLHistoryFuncArgs) appendTablesDDLHistory(ts uint64, tableIDs ...int64) {
	for _, tableID := range tableIDs {
		args.tablesDDLHistory[tableID] = append(args.tablesDDLHistory[tableID], ts)
	}
}

type updateSchemaMetadataFuncArgs struct {
	event        *PersistedDDLEvent
	databaseMap  map[int64]*BasicDatabaseInfo
	tableMap     map[int64]*BasicTableInfo
	partitionMap map[int64]BasicPartitionInfo
}

func (args *updateSchemaMetadataFuncArgs) addTableToDB(tableID int64, schemaID int64) {
	databaseInfo, ok := args.databaseMap[schemaID]
	if !ok {
		log.Panic("database not found.", zap.Int64("schemaID", schemaID), zap.Int64("tableID", tableID))
	}
	databaseInfo.Tables[tableID] = true
}

func (args *updateSchemaMetadataFuncArgs) removeTableFromDB(tableID int64, schemaID int64) {
	databaseInfo, ok := args.databaseMap[schemaID]
	if !ok {
		log.Panic("database not found.", zap.Int64("schemaID", schemaID), zap.Int64("tableID", tableID))
	}
	delete(databaseInfo.Tables, tableID)
}

type persistStorageDDLHandler struct {
	// buildPersistedDDLEventFunc build a PersistedDDLEvent which will be write to disk from a ddl job
	buildPersistedDDLEventFunc func(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent
	// updateDDLHistoryFunc add the finished ts of ddl event to the history of table trigger and related tables
	updateDDLHistoryFunc func(args updateDDLHistoryFuncArgs) []uint64
	// updateSchemaMetadataFunc update database info, table info and partition info according to the ddl event
	updateSchemaMetadataFunc func(args updateSchemaMetadataFuncArgs)
	// iterateEventTablesFunc iterates through all physical table IDs affected by the DDL event
	// and calls the provided `apply` function with those IDs. For partition tables, it includes
	// all partition IDs.
	iterateEventTablesFunc func(event *PersistedDDLEvent, apply func(tableIDs ...int64))
	// extractTableInfoFunc extract (table info, deleted) for the specified `tableID` from ddl event
	extractTableInfoFunc func(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool)
	// buildDDLEvent build a DDLEvent from a PersistedDDLEvent
	buildDDLEventFunc func(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool)
}

var allDDLHandlers = map[model.ActionType]*persistStorageDDLHandler{
	model.ActionCreateSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateSchema,
	},
	model.ActionDropSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForSchemaDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForDropSchema,
	},
	model.ActionCreateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForNewTableDDL,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNewTableDDL,
	},
	model.ActionDropTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForDropTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropTable,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForDropTable,
		buildDDLEventFunc:          buildDDLEventForDropTable,
	},
	model.ActionAddColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForTruncateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncateTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForTruncateTable,
		iterateEventTablesFunc:     iterateEventTablesForTruncateTable,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateTable,
		buildDDLEventFunc:          buildDDLEventForTruncateTable,
	},
	model.ActionModifyColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRebaseAutoID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRenameTable,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForRenameTable,
	},
	model.ActionSetDefaultValue: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionShardRowID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionModifyTableComment: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForAddPartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForAddPartition,
		iterateEventTablesFunc:     iterateEventTablesForAddPartition,
		extractTableInfoFunc:       extractTableInfoFuncForAddPartition,
		buildDDLEventFunc:          buildDDLEventForAddPartition,
	},
	model.ActionDropTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForDropPartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropPartition,
		iterateEventTablesFunc:     iterateEventTablesForDropPartition,
		extractTableInfoFunc:       extractTableInfoFuncForDropPartition,
		buildDDLEventFunc:          buildDDLEventForDropPartition,
	},
	model.ActionCreateView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateView,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateView,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateView,
	},
	model.ActionModifyTableCharsetAndCollate: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncatePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForTruncateTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForTruncatePartition,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateAndReorganizePartition,
		buildDDLEventFunc:          buildDDLEventForTruncateAndReorganizePartition,
	},
	model.ActionDropView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForDropView,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForDropView,
	},
	model.ActionRecoverTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForNewTableDDL,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNewTableDDL,
	},
	model.ActionModifySchemaCharsetAndCollate: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForSchemaDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForModifySchemaCharsetAndCollate,
	},
	model.ActionAddPrimaryKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropPrimaryKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAlterIndexVisibility: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionExchangeTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForExchangePartition,
		updateDDLHistoryFunc:       updateDDLHistoryForExchangeTablePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForExchangeTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForExchangeTablePartition,
		extractTableInfoFunc:       extractTableInfoFuncForExchangeTablePartition,
		buildDDLEventFunc:          buildDDLEventForExchangeTablePartition,
	},
	model.ActionRenameTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTables,
		updateDDLHistoryFunc:       updateDDLHistoryForRenameTables,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRenameTables,
		iterateEventTablesFunc:     iterateEventTablesForRenameTables,
		extractTableInfoFunc:       extractTableInfoFuncForRenameTables,
		buildDDLEventFunc:          buildDDLEventForRenameTables,
	},
	model.ActionCreateTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTables,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateTables,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateTables,
		iterateEventTablesFunc:     iterateEventTablesForCreateTables,
		extractTableInfoFunc:       extractTableInfoFuncForCreateTables,
		buildDDLEventFunc:          buildDDLEventForCreateTables,
	},
	model.ActionMultiSchemaChange: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionReorganizePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForReorganizePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForReorganizePartition,
		iterateEventTablesFunc:     iterateEventTablesForReorganizePartition,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateAndReorganizePartition,
		buildDDLEventFunc:          buildDDLEventForTruncateAndReorganizePartition,
	},
	model.ActionAlterTTLInfo: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTableTTL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	model.ActionAlterTTLRemove: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTableTTL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	model.ActionAlterTablePartitioning: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForAlterTablePartitioning,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTablePartitioning,
		updateSchemaMetadataFunc:   updateSchemaMetadataForAlterTablePartitioning,
		iterateEventTablesFunc:     iterateEventTablesForAlterTablePartitioning,
		extractTableInfoFunc:       extractTableInfoFuncForAlterTablePartitioning,
		buildDDLEventFunc:          buildDDLEventForAlterTablePartitioning,
	},
	model.ActionRemovePartitioning: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRemovePartitioning,
		updateDDLHistoryFunc:       updateDDLHistoryForRemovePartitioning,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRemovePartitioning,
		iterateEventTablesFunc:     iterateEventTablesForRemovePartitioning,
		extractTableInfoFunc:       extractTableInfoFuncForRemovePartitioning,
		buildDDLEventFunc:          buildDDLEventForRemovePartitioning,
	},
}

func isPartitionTable(tableInfo *model.TableInfo) bool {
	// tableInfo may only be nil in unit test
	return tableInfo != nil && tableInfo.Partition != nil
}

func getAllPartitionIDs(tableInfo *model.TableInfo) []int64 {
	physicalIDs := make([]int64, 0, len(tableInfo.Partition.Definitions))
	for _, partition := range tableInfo.Partition.Definitions {
		physicalIDs = append(physicalIDs, partition.ID)
	}
	return physicalIDs
}

func getSchemaName(databaseMap map[int64]*BasicDatabaseInfo, schemaID int64) string {
	databaseInfo, ok := databaseMap[schemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", schemaID))
	}
	return databaseInfo.Name
}

func getTableName(tableMap map[int64]*BasicTableInfo, tableID int64) string {
	tableInfo, ok := tableMap[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", tableID))
	}
	return tableInfo.Name
}

func getSchemaID(tableMap map[int64]*BasicTableInfo, tableID int64) int64 {
	tableInfo, ok := tableMap[tableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", tableID))
	}
	return tableInfo.SchemaID
}

// =======
// buildPersistedDDLEventFunc start
// =======
func buildPersistedDDLEventCommon(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	var query string
	job := args.job
	// only in unit test job.Query is empty
	if job.Query != "" {
		var err error
		query, err = transformDDLJobQuery(job)
		if err != nil {
			log.Panic("transformDDLJobQuery failed", zap.Error(err))
		}
	}

	// Note: if a ddl involve multiple tables, job.TableID is different with job.BinlogInfo.TableInfo.ID
	// and usually job.BinlogInfo.TableInfo.ID will be the newly created IDs.
	event := PersistedDDLEvent{
		ID:             job.ID,
		Type:           byte(job.Type),
		SchemaID:       job.SchemaID,
		TableID:        job.TableID,
		Query:          query,
		SchemaVersion:  job.BinlogInfo.SchemaVersion,
		DBInfo:         job.BinlogInfo.DBInfo,
		TableInfo:      job.BinlogInfo.TableInfo,
		FinishedTs:     job.BinlogInfo.FinishedTS,
		BDRRole:        job.BDRRole,
		CDCWriteSource: job.CDCWriteSource,
	}
	return event
}

func buildPersistedDDLEventForSchemaDDL(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	log.Info("buildPersistedDDLEvent for create/drop schema",
		zap.Any("type", event.Type),
		zap.Int64("schemaID", event.SchemaID),
		zap.String("schemaName", event.DBInfo.Name.O))
	event.SchemaName = event.DBInfo.Name.O
	return event
}

func buildPersistedDDLEventForCreateView(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = args.job.TableName
	return event
}

func buildPersistedDDLEventForDropView(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	// We don't store the relationship: view_id -> table_name, get table name from args.job
	event.TableName = args.job.TableName
	// The query in job maybe "DROP VIEW test1.view1, test2.view2", we need rebuild it here.
	event.Query = fmt.Sprintf("DROP VIEW `%s`.`%s`", event.SchemaName, event.TableName)
	return event
}

func buildPersistedDDLEventForCreateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = event.TableInfo.Name.O
	return event
}

func buildPersistedDDLEventForDropTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.TableID)
	// The query in job maybe "DROP TABLE test1.table1, test2.table2", we need rebuild it here.
	event.Query = fmt.Sprintf("DROP TABLE `%s`.`%s`", event.SchemaName, event.TableName)
	return event
}

func buildPersistedDDLEventForNormalDDLOnSingleTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.TableID)
	return event
}

func buildPersistedDDLEventForTruncateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	// only table id change after truncate
	event.ExtraTableID = event.TableInfo.ID
	// schema/table name remains the same, so it is ok to get them using old table/schema id
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.TableID)
	if isPartitionTable(event.TableInfo) {
		for id := range args.partitionMap[event.TableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	}
	return event
}

func buildPersistedDDLEventForRenameTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	// Note: schema id/schema name/table name may be changed or not
	// table id does not change, we use it to get the table's prev schema id/name and table name
	event.ExtraSchemaID = getSchemaID(args.tableMap, event.TableID)
	// TODO: check how ExtraTableName will be used later
	event.ExtraTableName = getTableName(args.tableMap, event.TableID)
	event.ExtraSchemaName = getSchemaName(args.databaseMap, event.ExtraSchemaID)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	// get the table's current table name from the ddl job
	event.TableName = event.TableInfo.Name.O
	if len(args.job.InvolvingSchemaInfo) > 0 {
		log.Info("buildPersistedDDLEvent for rename table",
			zap.String("query", event.Query),
			zap.Int64("schemaID", event.SchemaID),
			zap.String("SchemaName", event.SchemaName),
			zap.String("tableName", event.TableName),
			zap.Int64("ExtraSchemaID", event.ExtraSchemaID),
			zap.String("ExtraSchemaName", event.ExtraSchemaName),
			zap.String("ExtraTableName", event.ExtraTableName),
			zap.Any("involvingSchemaInfo", args.job.InvolvingSchemaInfo))
		// The query in job maybe "RENAME TABLE table1 to test2.table2", we need rebuild it here.
		//
		// Note: Why use args.job.InvolvingSchemaInfo to build query?
		// because event.ExtraSchemaID may not be accurate for rename table in some case.
		// after pr: https://github.com/pingcap/tidb/pull/43341,
		// assume there is a table `test.t` and a ddl: `rename table t to test2.t;`, and its commit ts is `100`.
		// if you get a ddl snapshot at ts `99`, table `t` is already in `test2`.
		// so event.ExtraSchemaName will also be `test2`.
		// And because SchemaStore is the source of truth inside cdc,
		// we can use event.ExtraSchemaID(even it is wrong) to update the internal state of the cdc.
		// But event.Query will be emit to downstream(out of cdc), we must make it correct.
		event.Query = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`",
			args.job.InvolvingSchemaInfo[0].Database, args.job.InvolvingSchemaInfo[0].Table,
			event.SchemaName, event.TableName)

	}
	return event
}

func buildPersistedDDLEventForNormalPartitionDDL(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventForNormalDDLOnSingleTable(args)
	for id := range args.partitionMap[event.TableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

func buildPersistedDDLEventForExchangePartition(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.TableName = getTableName(args.tableMap, event.TableID)
	event.SchemaID = getSchemaID(args.tableMap, event.TableID)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)

	event.ExtraTableID = event.TableInfo.ID
	event.ExtraTableName = getTableName(args.tableMap, event.ExtraTableID)
	event.ExtraSchemaID = getSchemaID(args.tableMap, event.ExtraTableID)
	event.ExtraSchemaName = getSchemaName(args.databaseMap, event.ExtraSchemaID)
	for id := range args.partitionMap[event.ExtraTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	if event.Query != "" {
		upperQuery := strings.ToUpper(event.Query)
		idx1 := strings.Index(upperQuery, "EXCHANGE PARTITION") + len("EXCHANGE PARTITION")
		idx2 := strings.Index(upperQuery, "WITH TABLE")

		// Note that partition name should be parsed from original query, not the upperQuery.
		partName := strings.TrimSpace(event.Query[idx1:idx2])
		partName = strings.Replace(partName, "`", "", -1)
		event.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` EXCHANGE PARTITION `%s` WITH TABLE `%s`.`%s`",
			event.ExtraSchemaName, event.ExtraTableName, partName, event.SchemaName, event.TableName)

		if strings.HasSuffix(upperQuery, "WITHOUT VALIDATION") {
			event.Query += " WITHOUT VALIDATION"
		}
	} else {
		log.Warn("exchange partition query is empty, should only happen in unit tests",
			zap.Int64("jobID", event.ID))
	}
	return event
}

func buildPersistedDDLEventForRenameTables(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	// TODO: does rename tables has the same problem(finished ts is not the real commit ts) with rename table?
	event := buildPersistedDDLEventCommon(args)
	renameArgs, err := model.GetRenameTablesArgs(args.job)
	if err != nil {
		log.Panic("GetRenameTablesArgs failed",
			zap.String("query", args.job.Query),
			zap.Error(err))
	}
	if len(renameArgs.RenameTableInfos) != len(args.job.BinlogInfo.MultipleTableInfos) {
		log.Panic("should not happen",
			zap.Int("renameArgsLen", len(renameArgs.RenameTableInfos)),
			zap.Int("multipleTableInfosLen", len(args.job.BinlogInfo.MultipleTableInfos)))
	}

	var querys []string
	for i, tableInfo := range args.job.BinlogInfo.MultipleTableInfos {
		info := renameArgs.RenameTableInfos[i]
		extraSchemaID := getSchemaID(args.tableMap, tableInfo.ID)
		event.ExtraSchemaIDs = append(event.ExtraSchemaIDs, extraSchemaID)
		event.ExtraSchemaNames = append(event.ExtraSchemaNames, getSchemaName(args.databaseMap, extraSchemaID))
		extraTableName := getTableName(args.tableMap, tableInfo.ID)
		event.ExtraTableNames = append(event.ExtraTableNames, extraTableName)
		event.SchemaIDs = append(event.SchemaIDs, info.NewSchemaID)
		SchemaName := getSchemaName(args.databaseMap, info.NewSchemaID)
		event.SchemaNames = append(event.SchemaNames, SchemaName)
		querys = append(querys, fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`;", info.OldSchemaName.O, extraTableName, SchemaName, tableInfo.Name.L))
	}

	event.Query = strings.Join(querys, "")
	event.MultipleTableInfos = args.job.BinlogInfo.MultipleTableInfos
	return event
}

func buildPersistedDDLEventForCreateTables(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.MultipleTableInfos = args.job.BinlogInfo.MultipleTableInfos
	return event
}

func buildPersistedDDLEventForAlterTablePartitioning(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.ExtraTableID = event.TableID
	event.TableID = event.TableInfo.ID
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.ExtraTableID)
	if event.TableName != event.TableInfo.Name.O {
		log.Panic("table name should not change",
			zap.String("ExtraTableName", event.TableName),
			zap.String("tableName", event.TableInfo.Name.O))
	}
	// prev table may be a normal table or a partition table
	if partitions, ok := args.partitionMap[event.ExtraTableID]; ok {
		for id := range partitions {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	}
	return event
}

func buildPersistedDDLEventForRemovePartitioning(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.ExtraTableID = event.TableID
	event.TableID = event.TableInfo.ID
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.ExtraTableID)
	if event.TableName != event.TableInfo.Name.O {
		log.Panic("table name should not change",
			zap.String("ExtraTableName", event.TableName),
			zap.String("tableName", event.TableInfo.Name.O))
	}
	partitions, ok := args.partitionMap[event.ExtraTableID]
	if !ok {
		log.Panic("table is not a partition table", zap.Int64("tableID", event.ExtraTableID))
	}
	for id := range partitions {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

// =======
// updateDDLHistoryFunc begin
// =======
func getCreatedIDs(oldIDs []int64, newIDs []int64) []int64 {
	oldIDsMap := make(map[int64]interface{}, len(oldIDs))
	for _, id := range oldIDs {
		oldIDsMap[id] = nil
	}
	createdIDs := make([]int64, 0)
	for _, id := range newIDs {
		if _, ok := oldIDsMap[id]; !ok {
			createdIDs = append(createdIDs, id)
		}
	}
	return createdIDs
}

func getDroppedIDs(oldIDs []int64, newIDs []int64) []int64 {
	return getCreatedIDs(newIDs, oldIDs)
}

func updateDDLHistoryForTableTriggerOnlyDDL(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForSchemaDDL(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	for tableID := range args.databaseMap[args.ddlEvent.SchemaID].Tables {
		if partitionInfo, ok := args.partitionMap[tableID]; ok {
			for id := range partitionInfo {
				args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, id)
			}
		} else {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, tableID)
		}
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForAddDropTable(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	// Note: for create table, this ddl event will not be sent to table dispatchers.
	// add it to ddl history is just for building table info store.
	if isPartitionTable(args.ddlEvent.TableInfo) {
		// for partition table, we only care the ddl history of physical table ids.
		for _, partitionID := range getAllPartitionIDs(args.ddlEvent.TableInfo) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, partitionID)
		}
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForNormalDDLOnSingleTable(args updateDDLHistoryFuncArgs) []uint64 {
	if isPartitionTable(args.ddlEvent.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(args.ddlEvent.TableInfo) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, partitionID)
		}
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForTruncateTable(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	if isPartitionTable(args.ddlEvent.TableInfo) {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(args.ddlEvent.TableInfo)...)
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID, args.ddlEvent.ExtraTableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForAddPartition(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(args.ddlEvent.TableInfo)...)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForDropPartition(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForCreateView(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	for tableID := range args.tableMap {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, tableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForTruncatePartition(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	newCreateIDs := getCreatedIDs(args.ddlEvent.PrevPartitions, getAllPartitionIDs(args.ddlEvent.TableInfo))
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, newCreateIDs...)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForExchangeTablePartition(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	droppedIDs := getDroppedIDs(args.ddlEvent.PrevPartitions, getAllPartitionIDs(args.ddlEvent.TableInfo))
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition", zap.Int64s("droppedIDs", droppedIDs))
	}
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, droppedIDs[0], args.ddlEvent.TableID)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForRenameTables(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	// it won't be send to table dispatchers, just for build version store
	for _, info := range args.ddlEvent.MultipleTableInfos {
		if isPartitionTable(info) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(info)...)
		} else {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, info.ID)
		}
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForCreateTables(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	// it won't be send to table dispatchers, just for build version store
	for _, info := range args.ddlEvent.MultipleTableInfos {
		if isPartitionTable(info) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(info)...)
		} else {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, info.ID)
		}
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForReorganizePartition(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	newCreateIDs := getCreatedIDs(args.ddlEvent.PrevPartitions, getAllPartitionIDs(args.ddlEvent.TableInfo))
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, newCreateIDs...)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForAlterTablePartitioning(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	// TODO: is it possible that partition table does not have partition?
	if len(args.ddlEvent.PrevPartitions) > 0 {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.ExtraTableID)
	}
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(args.ddlEvent.TableInfo)...)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForRemovePartitioning(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID)
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForAlterTableTTL(args updateDDLHistoryFuncArgs) []uint64 {
	if isPartitionTable(args.ddlEvent.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(args.ddlEvent.TableInfo) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, partitionID)
		}
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID)
	}
	return args.tableTriggerDDLHistory
}

// =======
// updateDatabaseInfoAndTableInfoFunc begin
// =======
func updateSchemaMetadataForCreateSchema(args updateSchemaMetadataFuncArgs) {
	args.databaseMap[args.event.SchemaID] = &BasicDatabaseInfo{
		Name:   args.event.SchemaName,
		Tables: make(map[int64]bool),
	}
}

func updateSchemaMetadataForDropSchema(args updateSchemaMetadataFuncArgs) {
	schemaID := args.event.SchemaID
	for tableID := range args.databaseMap[schemaID].Tables {
		delete(args.tableMap, tableID)
		delete(args.partitionMap, tableID)
	}
	delete(args.databaseMap, schemaID)
}

func updateSchemaMetadataForNewTableDDL(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	schemaID := args.event.SchemaID
	args.addTableToDB(tableID, schemaID)
	args.tableMap[tableID] = &BasicTableInfo{
		SchemaID: schemaID,
		Name:     args.event.TableInfo.Name.O,
	}
	if isPartitionTable(args.event.TableInfo) {
		partitionInfo := make(BasicPartitionInfo)
		for _, id := range getAllPartitionIDs(args.event.TableInfo) {
			partitionInfo[id] = nil
		}
		args.partitionMap[tableID] = partitionInfo
	}
}

func updateSchemaMetadataForDropTable(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	schemaID := args.event.SchemaID
	args.removeTableFromDB(tableID, schemaID)
	delete(args.tableMap, tableID)
	if isPartitionTable(args.event.TableInfo) {
		delete(args.partitionMap, tableID)
	}
}

func updateSchemaMetadataIgnore(args updateSchemaMetadataFuncArgs) {}

func updateSchemaMetadataForTruncateTable(args updateSchemaMetadataFuncArgs) {
	oldTableID := args.event.TableID
	newTableID := args.event.ExtraTableID
	schemaID := args.event.SchemaID
	args.removeTableFromDB(oldTableID, schemaID)
	delete(args.tableMap, oldTableID)
	args.addTableToDB(newTableID, schemaID)
	args.tableMap[newTableID] = &BasicTableInfo{
		SchemaID: schemaID,
		Name:     args.event.TableInfo.Name.O,
	}
	if isPartitionTable(args.event.TableInfo) {
		delete(args.partitionMap, oldTableID)
		partitionInfo := make(BasicPartitionInfo)
		for _, id := range getAllPartitionIDs(args.event.TableInfo) {
			partitionInfo[id] = nil
		}
		args.partitionMap[newTableID] = partitionInfo
	}
}

func updateSchemaMetadataForRenameTable(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	if args.event.ExtraSchemaID != args.event.SchemaID {
		args.tableMap[tableID].SchemaID = args.event.SchemaID
		args.removeTableFromDB(tableID, args.event.ExtraSchemaID)
		args.addTableToDB(tableID, args.event.SchemaID)
	}
	args.tableMap[tableID].Name = args.event.TableName
}

func updateSchemaMetadataForAddPartition(args updateSchemaMetadataFuncArgs) {
	newCreatedIDs := getCreatedIDs(args.event.PrevPartitions, getAllPartitionIDs(args.event.TableInfo))
	for _, id := range newCreatedIDs {
		args.partitionMap[args.event.TableID][id] = nil
	}
}

func updateSchemaMetadataForDropPartition(args updateSchemaMetadataFuncArgs) {
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, getAllPartitionIDs(args.event.TableInfo))
	for _, id := range droppedIDs {
		delete(args.partitionMap[args.event.TableID], id)
	}
}

func updateSchemaMetadataForTruncateTablePartition(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	physicalIDs := getAllPartitionIDs(args.event.TableInfo)
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, physicalIDs)
	for _, id := range droppedIDs {
		delete(args.partitionMap[tableID], id)
	}
	newCreatedIDs := getCreatedIDs(args.event.PrevPartitions, physicalIDs)
	for _, id := range newCreatedIDs {
		args.partitionMap[tableID][id] = nil
	}
}

func updateSchemaMetadataForExchangeTablePartition(args updateSchemaMetadataFuncArgs) {
	physicalIDs := getAllPartitionIDs(args.event.TableInfo)
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, physicalIDs)
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition",
			zap.Int64s("droppedIDs", droppedIDs))
	}
	normalTableID := args.event.TableID
	normalSchemaID := args.event.SchemaID
	normalTableName := getTableName(args.tableMap, normalTableID)
	partitionTableID := args.event.ExtraTableID
	targetPartitionID := droppedIDs[0]
	args.removeTableFromDB(normalTableID, normalSchemaID)
	delete(args.tableMap, normalTableID)
	args.addTableToDB(targetPartitionID, normalSchemaID)
	args.tableMap[targetPartitionID] = &BasicTableInfo{
		SchemaID: normalSchemaID,
		Name:     normalTableName,
	}
	delete(args.partitionMap[partitionTableID], targetPartitionID)
	args.partitionMap[partitionTableID][normalTableID] = nil
}

func updateSchemaMetadataForRenameTables(args updateSchemaMetadataFuncArgs) {
	if args.event.MultipleTableInfos == nil {
		log.Panic("multiple table infos should not be nil")
	}
	for i, info := range args.event.MultipleTableInfos {
		if args.event.ExtraSchemaIDs[i] != args.event.SchemaIDs[i] {
			args.tableMap[info.ID].SchemaID = args.event.SchemaIDs[i]
			args.removeTableFromDB(info.ID, args.event.ExtraSchemaIDs[i])
			args.addTableToDB(info.ID, args.event.SchemaIDs[i])
		}
		args.tableMap[info.ID].Name = info.Name.O
	}
}

func updateSchemaMetadataForCreateTables(args updateSchemaMetadataFuncArgs) {
	if args.event.MultipleTableInfos == nil {
		log.Panic("multiple table infos should not be nil")
	}
	for _, info := range args.event.MultipleTableInfos {
		args.addTableToDB(info.ID, args.event.SchemaID)
		args.tableMap[info.ID] = &BasicTableInfo{
			SchemaID: args.event.SchemaID,
			Name:     info.Name.O,
		}
		if isPartitionTable(info) {
			partitionInfo := make(BasicPartitionInfo)
			for _, id := range getAllPartitionIDs(info) {
				partitionInfo[id] = nil
			}
			args.partitionMap[info.ID] = partitionInfo
		}
	}
}

func updateSchemaMetadataForReorganizePartition(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	physicalIDs := getAllPartitionIDs(args.event.TableInfo)
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, physicalIDs)
	for _, id := range droppedIDs {
		delete(args.partitionMap[tableID], id)
	}
	newCreatedIDs := getCreatedIDs(args.event.PrevPartitions, physicalIDs)
	for _, id := range newCreatedIDs {
		args.partitionMap[tableID][id] = nil
	}
}

func updateSchemaMetadataForAlterTablePartitioning(args updateSchemaMetadataFuncArgs) {
	// drop old table and its partitions(if old table is a partition table)
	oldTableID := args.event.ExtraTableID
	schemaID := args.event.SchemaID
	args.removeTableFromDB(oldTableID, schemaID)
	delete(args.tableMap, oldTableID)
	delete(args.partitionMap, oldTableID)
	// add new normal table
	newTableID := args.event.TableID
	args.addTableToDB(newTableID, schemaID)
	args.tableMap[newTableID] = &BasicTableInfo{
		SchemaID: args.event.SchemaID,
		Name:     args.event.TableName,
	}
	args.partitionMap[newTableID] = make(BasicPartitionInfo)
	for _, id := range getAllPartitionIDs(args.event.TableInfo) {
		args.partitionMap[newTableID][id] = nil
	}
}

func updateSchemaMetadataForRemovePartitioning(args updateSchemaMetadataFuncArgs) {
	// drop old partition table and its partitions
	oldTableID := args.event.ExtraTableID
	schemaID := args.event.SchemaID
	args.removeTableFromDB(oldTableID, schemaID)
	delete(args.tableMap, oldTableID)
	delete(args.partitionMap, oldTableID)
	// add new normal table
	newTableID := args.event.TableID
	args.addTableToDB(newTableID, schemaID)
	args.tableMap[newTableID] = &BasicTableInfo{
		SchemaID: args.event.SchemaID,
		Name:     args.event.TableName,
	}
}

// =======
// iterateEventTablesFunc begin
// =======

func iterateEventTablesIgnore(event *PersistedDDLEvent, apply func(tableId ...int64)) {}

func iterateEventTablesForSingleTableDDL(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	if isPartitionTable(event.TableInfo) {
		apply(getAllPartitionIDs(event.TableInfo)...)
	} else {
		apply(event.TableID)
	}
}

func iterateEventTablesForTruncateTable(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	if isPartitionTable(event.TableInfo) {
		apply(event.PrevPartitions...)
		apply(getAllPartitionIDs(event.TableInfo)...)
	} else {
		apply(event.ExtraTableID, event.TableID)
	}
}

func iterateEventTablesForAddPartition(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
	apply(newCreatedIDs...)
}

func iterateEventTablesForDropPartition(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	droppedIDs := getDroppedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
	apply(droppedIDs...)
}

func iterateEventTablesForTruncatePartition(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	physicalIDs := getAllPartitionIDs(event.TableInfo)
	droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
	apply(droppedIDs...)
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
	apply(newCreatedIDs...)
}

func iterateEventTablesForExchangeTablePartition(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	physicalIDs := getAllPartitionIDs(event.TableInfo)
	droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition",
			zap.Int64s("droppedIDs", droppedIDs))
	}
	targetPartitionID := droppedIDs[0]
	apply(targetPartitionID, event.TableID)
}

func iterateEventTablesForRenameTables(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	for _, info := range event.MultipleTableInfos {
		if isPartitionTable(info) {
			apply(getAllPartitionIDs(info)...)
		} else {
			apply(info.ID)
		}
	}
}

func iterateEventTablesForCreateTables(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	for _, info := range event.MultipleTableInfos {
		if isPartitionTable(info) {
			apply(getAllPartitionIDs(info)...)
		} else {
			apply(info.ID)
		}
	}
}

func iterateEventTablesForReorganizePartition(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	physicalIDs := getAllPartitionIDs(event.TableInfo)
	droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
	apply(droppedIDs...)
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
	apply(newCreatedIDs...)
}

func iterateEventTablesForAlterTablePartitioning(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	if len(event.PrevPartitions) > 0 {
		apply(event.PrevPartitions...)
	} else {
		apply(event.ExtraTableID)
	}
	apply(getAllPartitionIDs(event.TableInfo)...)
}

func iterateEventTablesForRemovePartitioning(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	apply(event.PrevPartitions...)
	apply(event.TableID)
}

// =======
// extractTableInfoFunc begin
// =======

func extractTableInfoFuncForSingleTableDDL(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
			}
		}
	} else {
		if tableID == event.TableID {
			return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
		}
	}
	log.Panic("should not reach here", zap.Any("event", event), zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForExchangeTablePartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if tableID == event.TableID {
		// old normal table id, return the table info of the partition table
		return common.WrapTableInfo(event.ExtraSchemaID, event.ExtraSchemaName, event.TableInfo), false
	} else {
		physicalIDs := getAllPartitionIDs(event.TableInfo)
		droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
		if len(droppedIDs) != 1 {
			log.Panic("exchange table partition should only drop one partition",
				zap.Int64s("droppedIDs", droppedIDs))
		}
		if tableID != droppedIDs[0] {
			log.Panic("should not reach here", zap.Int64("tableID", tableID), zap.Int64("expectedPartitionID", droppedIDs[0]))
		}
		if event.ExtraTableInfo == nil {
			log.Panic("cannot find extra table info", zap.Int64("tableID", tableID))
		}
		// old partition id, return the table info of the normal table
		columnSchema := event.ExtraTableInfo.ShadowCopyColumnSchema()
		tableInfo := common.NewTableInfo(
			event.SchemaID,
			event.SchemaName,
			pmodel.NewCIStr(event.TableName).O,
			tableID,
			false,
			columnSchema)
		return tableInfo, false
	}
}

func extractTableInfoFuncIgnore(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	return nil, false
}

func extractTableInfoFuncForDropTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if event.TableID == tableID {
		return nil, true
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForTruncateTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
			}
		}
		return nil, true
	} else {
		if tableID == event.ExtraTableID {
			return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
		} else if tableID == event.TableID {
			return nil, true
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForAddPartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
	for _, partition := range newCreatedIDs {
		if tableID == partition {
			return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
		}
	}
	return nil, false
}

func extractTableInfoFuncForDropPartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	droppedIDs := getDroppedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
	for _, partition := range droppedIDs {
		if tableID == partition {
			return nil, true
		}
	}
	return nil, false
}

func extractTableInfoFuncForTruncateAndReorganizePartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	physicalIDs := getAllPartitionIDs(event.TableInfo)
	droppedIDs := getDroppedIDs(event.PrevPartitions, physicalIDs)
	for _, partition := range droppedIDs {
		if tableID == partition {
			return nil, true
		}
	}
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, physicalIDs)
	for _, partition := range newCreatedIDs {
		if tableID == partition {
			return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
		}
	}
	return nil, false
}

func extractTableInfoFuncForRenameTables(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	for i, tableInfo := range event.MultipleTableInfos {
		if isPartitionTable(tableInfo) {
			for _, partitionID := range getAllPartitionIDs(tableInfo) {
				if tableID == partitionID {
					return common.WrapTableInfo(event.SchemaIDs[i], event.SchemaNames[i], tableInfo), false
				}
			}
		} else {
			if tableID == tableInfo.ID {
				return common.WrapTableInfo(event.SchemaIDs[i], event.SchemaNames[i], tableInfo), false
			}
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForCreateTables(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	for _, tableInfo := range event.MultipleTableInfos {
		if isPartitionTable(tableInfo) {
			for _, partitionID := range getAllPartitionIDs(tableInfo) {
				if tableID == partitionID {
					return common.WrapTableInfo(event.SchemaID, event.SchemaName, tableInfo), false
				}
			}
		} else {
			if tableID == tableInfo.ID {
				return common.WrapTableInfo(event.SchemaID, event.SchemaName, tableInfo), false
			}
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForAlterTablePartitioning(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if len(event.PrevPartitions) > 0 {
		for _, partitionID := range event.PrevPartitions {
			if tableID == partitionID {
				return nil, true
			}
		}
	} else {
		if tableID == event.ExtraTableID {
			return nil, true
		}
	}
	for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
		if tableID == partitionID {
			return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForRemovePartitioning(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if event.TableID == tableID {
		return common.WrapTableInfo(event.SchemaID, event.SchemaName, event.TableInfo), false
	} else {
		for _, partitionID := range event.PrevPartitions {
			if tableID == partitionID {
				return nil, true
			}
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

// =======
// buildDDLEvent begin
// =======

func buildDDLEventCommon(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tiDBOnly bool) (commonEvent.DDLEvent, bool) {
	var wrapTableInfo *common.TableInfo
	// Note: not all ddl types will respect the `filtered` result, example: create tables, rename tables
	filtered := false
	// TODO: ShouldDiscardDDL is used for old architecture, should be removed later
	if tableFilter != nil && rawEvent.SchemaName != "" && rawEvent.TableName != "" {
		filtered = tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.SchemaName, rawEvent.TableName, rawEvent.TableInfo)
		// if the ddl invovles another table name, only set filtered to true when all of them should be filtered
		if rawEvent.ExtraSchemaName != "" && rawEvent.ExtraTableName != "" {
			filtered = filtered && tableFilter.ShouldDiscardDDL(model.ActionType(rawEvent.Type), rawEvent.ExtraSchemaName, rawEvent.ExtraTableName, rawEvent.TableInfo)
		}
	}
	if rawEvent.TableInfo != nil {
		wrapTableInfo = common.WrapTableInfo(
			rawEvent.SchemaID,
			rawEvent.SchemaName,
			rawEvent.TableInfo)
	}

	return commonEvent.DDLEvent{
		Type: rawEvent.Type,
		// TODO: whether the following four fields are needed
		SchemaID:   rawEvent.SchemaID,
		TableID:    rawEvent.TableID,
		SchemaName: rawEvent.SchemaName,
		TableName:  rawEvent.TableName,

		Query:      rawEvent.Query,
		TableInfo:  wrapTableInfo,
		FinishedTs: rawEvent.FinishedTs,
		TiDBOnly:   tiDBOnly,
	}, !filtered
}

func buildDDLEventForCreateSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	return ddlEvent, true
}

func buildDDLEventForDropSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.SchemaID,
	}
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.SchemaID,
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		DropDatabaseName: rawEvent.SchemaName,
	}
	return ddlEvent, true
}

func buildDDLEventForModifySchemaCharsetAndCollate(
	rawEvent *PersistedDDLEvent, tableFilter filter.Filter,
) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.SchemaID,
	}
	return ddlEvent, true
}

func buildDDLEventForNewTableDDL(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	if isPartitionTable(rawEvent.TableInfo) {
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
		for _, id := range physicalIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.SchemaID,
				TableID:  id,
			})
		}
	} else {
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.SchemaID,
				TableID:  rawEvent.TableID,
			},
		}
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		AddName: []commonEvent.SchemaTableName{
			{
				SchemaName: rawEvent.SchemaName,
				TableName:  rawEvent.TableName,
			},
		},
	}
	return ddlEvent, true
}

func buildDDLEventForDropTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	if isPartitionTable(rawEvent.TableInfo) {
		allPhysicalTableIDs := getAllPartitionIDs(rawEvent.TableInfo)
		allPhysicalTableIDsAndDDLSpanID := make([]int64, 0, len(rawEvent.TableInfo.Partition.Definitions)+1)
		allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, allPhysicalTableIDs...)
		allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      allPhysicalTableIDsAndDDLSpanID,
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      allPhysicalTableIDs,
		}
	} else {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		DropName: []commonEvent.SchemaTableName{
			{
				SchemaName: rawEvent.SchemaName,
				TableName:  rawEvent.TableName,
			},
		},
	}
	return ddlEvent, true
}

func buildDDLEventForNormalDDLOnSingleTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{rawEvent.TableID},
	}
	return ddlEvent, true
}

func buildDDLEventForNormalDDLOnSingleTableForTiDB(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{rawEvent.TableID},
	}
	return ddlEvent, true
}

func buildDDLEventForTruncateTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	if isPartitionTable(rawEvent.TableInfo) {
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      prevPartitionsAndDDLSpanID,
		}
		// Note: for truncate table, prev partitions must all be dropped.
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      rawEvent.PrevPartitions,
		}
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
		for _, id := range physicalIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.SchemaID,
				TableID:  id,
			})
		}
	} else {
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.SchemaID,
				TableID:  rawEvent.ExtraTableID,
			},
		}
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, heartbeatpb.DDLSpan.TableID},
		}
	}
	return ddlEvent, true
}

func buildDDLEventForRenameTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.ExtraSchemaName = rawEvent.ExtraSchemaName
	ddlEvent.ExtraTableName = rawEvent.ExtraTableName
	ignorePrevTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.ExtraSchemaName, rawEvent.ExtraTableName, rawEvent.TableInfo)
	ignoreCurrentTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.SchemaName, rawEvent.TableName, rawEvent.TableInfo)
	if isPartitionTable(rawEvent.TableInfo) {
		allPhysicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		if !ignorePrevTable {
			allPhysicalIDsAndDDLSpanID := make([]int64, 0, len(allPhysicalIDs)+1)
			allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, allPhysicalIDs...)
			allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      allPhysicalIDsAndDDLSpanID,
			}
			if !ignoreCurrentTable {
				// check whether schema change
				if rawEvent.ExtraSchemaID != rawEvent.SchemaID {
					ddlEvent.UpdatedSchemas = make([]commonEvent.SchemaIDChange, 0, len(allPhysicalIDs))
					for _, id := range allPhysicalIDs {
						ddlEvent.UpdatedSchemas = append(ddlEvent.UpdatedSchemas, commonEvent.SchemaIDChange{
							TableID:     id,
							OldSchemaID: rawEvent.ExtraSchemaID,
							NewSchemaID: rawEvent.SchemaID,
						})
					}
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.SchemaName,
							TableName:  rawEvent.TableName,
						},
					},
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.ExtraSchemaName,
							TableName:  rawEvent.ExtraTableName,
						},
					},
				}
			} else {
				// the table is filtered out after rename table, we need drop the table
				ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      allPhysicalIDs,
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.ExtraSchemaName,
							TableName:  rawEvent.ExtraTableName,
						},
					},
				}
			}
		} else if !ignoreCurrentTable {
			// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
			ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query)
		} else {
			// if the table is both filtered out before and after rename table, the ddl should not be fetched
			log.Panic("should not build a ignored rename table ddl",
				zap.String("DDL", rawEvent.Query),
				zap.Int64("jobID", rawEvent.ID),
				zap.Int64("schemaID", rawEvent.SchemaID),
				zap.Int64("tableID", rawEvent.TableID))
		}
	} else {
		if !ignorePrevTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.TableID, heartbeatpb.DDLSpan.TableID},
			}
			if !ignoreCurrentTable {
				if rawEvent.ExtraSchemaID != rawEvent.SchemaID {
					ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
						{
							TableID:     rawEvent.TableID,
							OldSchemaID: rawEvent.ExtraSchemaID,
							NewSchemaID: rawEvent.SchemaID,
						},
					}
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.SchemaName,
							TableName:  rawEvent.TableName,
						},
					},
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.ExtraSchemaName,
							TableName:  rawEvent.ExtraTableName,
						},
					},
				}
			} else {
				// the table is filtered out after rename table, we need drop the table
				ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{rawEvent.TableID},
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.ExtraSchemaName,
							TableName:  rawEvent.ExtraTableName,
						},
					},
				}
			}
		} else if !ignoreCurrentTable {
			// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
			ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query)
		} else {
			// if the table is both filtered out before and after rename table, the ddl should not be fetched
			log.Panic("should not build a ignored rename table ddl",
				zap.String("DDL", rawEvent.Query),
				zap.Int64("jobID", rawEvent.ID),
				zap.Int64("schemaID", rawEvent.SchemaID),
				zap.Int64("tableID", rawEvent.TableID))
		}
	}
	return ddlEvent, true
}

func buildDDLEventForAddPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      prevPartitionsAndDDLSpanID,
	}
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(newCreatedIDs))
	for _, id := range newCreatedIDs {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID: rawEvent.SchemaID,
			TableID:  id,
		})
	}
	return ddlEvent, true
}

func buildDDLEventForDropPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      prevPartitionsAndDDLSpanID,
	}
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      droppedIDs,
	}
	return ddlEvent, true
}

func buildDDLEventForCreateView(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeAll,
	}
	return ddlEvent, true
}

func buildDDLEventForDropView(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	return ddlEvent, true
}

func buildDDLEventForTruncateAndReorganizePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, heartbeatpb.DDLSpan.TableID)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      prevPartitionsAndDDLSpanID,
	}
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
	for _, id := range newCreatedIDs {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID: rawEvent.SchemaID,
			TableID:  id,
		})
	}
	droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      droppedIDs,
	}
	return ddlEvent, true
}

func buildDDLEventForExchangeTablePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	// TODO: rawEvent.TableInfo is not correct for ignoreNormalTable
	ignoreNormalTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.SchemaName, rawEvent.TableName, rawEvent.TableInfo)
	ignorePartitionTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.ExtraSchemaName, rawEvent.ExtraTableName, rawEvent.TableInfo)
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition",
			zap.Int64s("droppedIDs", droppedIDs))
	}
	targetPartitionID := droppedIDs[0]
	if !ignoreNormalTable && !ignorePartitionTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, targetPartitionID, heartbeatpb.DDLSpan.TableID},
		}
		if rawEvent.SchemaID != rawEvent.ExtraSchemaID {
			ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
				{
					TableID:     targetPartitionID,
					OldSchemaID: rawEvent.ExtraSchemaID,
					NewSchemaID: rawEvent.SchemaID,
				},
				{
					TableID:     rawEvent.TableID,
					OldSchemaID: rawEvent.SchemaID,
					NewSchemaID: rawEvent.ExtraSchemaID,
				},
			}
		}
	} else if !ignoreNormalTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.SchemaID,
				TableID:  targetPartitionID,
			},
		}
	} else if !ignorePartitionTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{targetPartitionID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{targetPartitionID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.ExtraSchemaID,
				TableID:  rawEvent.TableID,
			},
		}
	} else {
		log.Fatal("should not happen")
	}
	return ddlEvent, true
}

func buildDDLEventForRenameTables(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, _ := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	querys, err := commonEvent.SplitQueries(rawEvent.Query)
	if err != nil {
		log.Panic("split queries failed", zap.Error(err))
	}
	var addNames, dropNames []commonEvent.SchemaTableName
	allFiltered := true
	resultQuerys := make([]string, 0)
	tableInfos := make([]*common.TableInfo, 0)
	if len(querys) != len(rawEvent.MultipleTableInfos) {
		log.Panic("rename tables length is not equal table infos", zap.Any("querys", querys), zap.Any("tableInfos", rawEvent.MultipleTableInfos))
	}
	for i, tableInfo := range rawEvent.MultipleTableInfos {
		ignorePrevTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.ExtraSchemaNames[i], rawEvent.ExtraTableNames[i], tableInfo)
		ignoreCurrentTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.SchemaNames[i], tableInfo.Name.O, tableInfo)
		if ignorePrevTable && ignoreCurrentTable {
			continue
		}
		allFiltered = false
		if isPartitionTable(rawEvent.TableInfo) {
			allPhysicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
			if !ignorePrevTable {
				resultQuerys = append(resultQuerys, querys[i])
				tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaID, rawEvent.SchemaName, tableInfo))
				ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, allPhysicalIDs...)
				if !ignoreCurrentTable {
					// check whether schema change
					if rawEvent.ExtraSchemaIDs[i] != rawEvent.SchemaIDs[i] {
						for _, id := range allPhysicalIDs {
							ddlEvent.UpdatedSchemas = append(ddlEvent.UpdatedSchemas, commonEvent.SchemaIDChange{
								TableID:     id,
								OldSchemaID: rawEvent.ExtraSchemaIDs[i],
								NewSchemaID: rawEvent.SchemaIDs[i],
							})
						}
					}
					addNames = append(addNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.SchemaNames[i],
						TableName:  tableInfo.Name.O,
					})
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				} else {
					// the table is filtered out after rename table, we need drop the table
					if ddlEvent.NeedDroppedTables == nil {
						ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
							InfluenceType: commonEvent.InfluenceTypeNormal,
						}
					}
					ddlEvent.NeedDroppedTables.TableIDs = append(ddlEvent.NeedDroppedTables.TableIDs, allPhysicalIDs...)
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				}
			} else if !ignoreCurrentTable {
				// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
				ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query)
			} else {
				// if the table is both filtered out before and after rename table, ignore
			}
		} else {
			if !ignorePrevTable {
				resultQuerys = append(resultQuerys, querys[i])
				tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaID, rawEvent.SchemaName, tableInfo))
				ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, tableInfo.ID)
				if !ignoreCurrentTable {
					if rawEvent.ExtraSchemaIDs[i] != rawEvent.SchemaIDs[i] {
						ddlEvent.UpdatedSchemas = append(ddlEvent.UpdatedSchemas, commonEvent.SchemaIDChange{
							TableID:     tableInfo.ID,
							OldSchemaID: rawEvent.ExtraSchemaIDs[i],
							NewSchemaID: rawEvent.SchemaIDs[i],
						})
					}
					addNames = append(addNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.SchemaNames[i],
						TableName:  tableInfo.Name.O,
					})
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				} else {
					// the table is filtered out after rename table, we need drop the table
					if ddlEvent.NeedDroppedTables == nil {
						ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
							InfluenceType: commonEvent.InfluenceTypeNormal,
						}
					}
					ddlEvent.NeedDroppedTables.TableIDs = append(ddlEvent.NeedDroppedTables.TableIDs, tableInfo.ID)
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				}
			} else if !ignoreCurrentTable {
				// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
				ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query)
			} else {
				// ignore
			}
		}
	}
	if allFiltered {
		return commonEvent.DDLEvent{}, false
	}
	if addNames != nil || dropNames != nil {
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			AddName:  addNames,
			DropName: dropNames,
		}
	}
	ddlEvent.Query = strings.Join(resultQuerys, "")
	ddlEvent.MultipleTableInfos = tableInfos
	return ddlEvent, true
}

func buildDDLEventForCreateTables(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	ddlEvent, _ := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	physicalTableCount := 0
	logicalTableCount := 0
	allFiltered := true
	for _, info := range rawEvent.MultipleTableInfos {
		if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.SchemaName, info.Name.O, info) {
			continue
		}
		allFiltered = false
		logicalTableCount += 1
		if isPartitionTable(info) {
			physicalTableCount += len(info.Partition.Definitions)
		} else {
			physicalTableCount += 1
		}
	}
	if allFiltered {
		return commonEvent.DDLEvent{}, false
	}
	querys, err := commonEvent.SplitQueries(rawEvent.Query)
	if err != nil {
		log.Panic("split queries failed", zap.Error(err))
	}
	if len(querys) != len(rawEvent.MultipleTableInfos) {
		log.Panic("query count not match table count",
			zap.Int("queryCount", len(querys)),
			zap.Int("tableCount", len(rawEvent.MultipleTableInfos)),
			zap.String("query", rawEvent.Query))
	}
	ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, physicalTableCount)
	addName := make([]commonEvent.SchemaTableName, 0, logicalTableCount)
	resultQuerys := make([]string, 0, logicalTableCount)
	tableInfos := make([]*common.TableInfo, 0, logicalTableCount)
	for i, info := range rawEvent.MultipleTableInfos {
		if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.SchemaName, info.Name.O, info) {
			log.Info("build ddl event for create tables filter table",
				zap.String("schemaName", rawEvent.SchemaName),
				zap.String("tableName", info.Name.O))
			continue
		}
		if isPartitionTable(info) {
			for _, partitionID := range getAllPartitionIDs(info) {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.SchemaID,
					TableID:  partitionID,
				})
			}
		} else {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.SchemaID,
				TableID:  info.ID,
			})
		}
		addName = append(addName, commonEvent.SchemaTableName{
			SchemaName: rawEvent.SchemaName,
			TableName:  info.Name.O,
		})
		resultQuerys = append(resultQuerys, querys[i])
		tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaID, rawEvent.SchemaName, info))
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		AddName: addName,
	}
	ddlEvent.Query = strings.Join(resultQuerys, "")
	ddlEvent.MultipleTableInfos = tableInfos
	if len(ddlEvent.NeedAddedTables) == 0 {
		log.Fatal("should not happen")
	}
	return ddlEvent, true
}

func buildDDLEventForAlterTablePartitioning(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	// TODO: only tidb?
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	if len(rawEvent.PrevPartitions) > 0 {
		ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.PrevPartitions...)
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      rawEvent.PrevPartitions,
		}
	} else {
		ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.ExtraTableID)
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.ExtraTableID},
		}
	}
	for _, id := range getAllPartitionIDs(rawEvent.TableInfo) {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID: rawEvent.SchemaID,
			TableID:  id,
		})
	}
	return ddlEvent, true
}

func buildDDLEventForRemovePartitioning(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) (commonEvent.DDLEvent, bool) {
	// TODO: only tidb?
	ddlEvent, ok := buildDDLEventCommon(rawEvent, tableFilter, WithTiDBOnly)
	if !ok {
		return ddlEvent, false
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.PrevPartitions...)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      rawEvent.PrevPartitions,
	}
	ddlEvent.NeedAddedTables = []commonEvent.Table{
		{
			SchemaID: rawEvent.SchemaID,
			TableID:  rawEvent.TableID,
		},
	}
	return ddlEvent, true
}
