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
	"errors"
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

const (
	WithoutTiDBOnly = false
	WithTiDBOnly    = true
	// InvalidTableID used in rename tables ddl to avoid duplicate table id
	InvalidTableID = -1
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

type updateFullTableInfoFuncArgs struct {
	event       *PersistedDDLEvent
	databaseMap map[int64]*BasicDatabaseInfo
	// logical table id -> table info
	tableInfoMap map[int64]*model.TableInfo
}

type persistStorageDDLHandler struct {
	// buildPersistedDDLEventFunc build a PersistedDDLEvent which will be write to disk from a ddl job
	buildPersistedDDLEventFunc func(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent
	// updateDDLHistoryFunc add the finished ts of ddl event to the history of table trigger and related tables
	updateDDLHistoryFunc func(args updateDDLHistoryFuncArgs) []uint64
	// updateFullTableInfoFunc update the full table info map according to the ddl event
	// Note: it must be called before updateSchemaMetadataFunc,
	// because it depends on some info which may be updated by updateSchemaMetadataFunc
	// TODO: add unit test
	updateFullTableInfoFunc func(args updateFullTableInfoFuncArgs)
	// updateSchemaMetadataFunc update database info, table info and partition info according to the ddl event
	updateSchemaMetadataFunc func(args updateSchemaMetadataFuncArgs)
	// iterateEventTablesFunc iterates through all physical table IDs affected by the DDL event
	// and calls the provided `apply` function with those IDs. For partition tables, it includes
	// all partition IDs.
	iterateEventTablesFunc func(event *PersistedDDLEvent, apply func(tableIDs ...int64))
	// extractTableInfoFunc extract (table info, deleted) for the specified `tableID` from ddl event
	extractTableInfoFunc func(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool)
	// buildDDLEvent build a DDLEvent from a PersistedDDLEvent
	// NOTE: the tableID is used in exchange table partition and rename tables DDL only,
	// see the details in buildDDLEventForExchangeTablePartition and buildDDLEventForRenameTables.
	// For other DDLs, tableID is not used and can be set to 0.
	buildDDLEventFunc func(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error)
}

var allDDLHandlers = map[model.ActionType]*persistStorageDDLHandler{
	model.ActionCreateSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
		updateFullTableInfoFunc:    updateFullTableInfoIgnore,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateSchema,
	},
	model.ActionDropSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForSchemaDDL,
		updateFullTableInfoFunc:    updateFullTableInfoForDropSchema,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForDropSchema,
	},
	model.ActionCreateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForNewTableDDL,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNewTableDDL,
	},
	model.ActionDropTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForDropTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateFullTableInfoFunc:    updateFullTableInfoForDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropTable,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForDropTable,
		buildDDLEventFunc:          buildDDLEventForDropTable,
	},
	model.ActionAddColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForTruncateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncateTable,
		updateFullTableInfoFunc:    updateFullTableInfoForTruncateTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForTruncateTable,
		iterateEventTablesFunc:     iterateEventTablesForTruncateTable,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateTable,
		buildDDLEventFunc:          buildDDLEventForTruncateTable,
	},
	model.ActionModifyColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRebaseAutoID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRenameTable,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForRenameTable,
	},
	model.ActionSetDefaultValue: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionShardRowID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionModifyTableComment: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForAddPartition,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForAddPartition,
		iterateEventTablesFunc:     iterateEventTablesForAddPartition,
		extractTableInfoFunc:       extractTableInfoFuncForAddPartition,
		buildDDLEventFunc:          buildDDLEventForAddPartition,
	},
	model.ActionDropTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForDropPartition,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropPartition,
		iterateEventTablesFunc:     iterateEventTablesForDropPartition,
		extractTableInfoFunc:       extractTableInfoFuncForDropPartition,
		buildDDLEventFunc:          buildDDLEventForDropPartition,
	},
	model.ActionCreateView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateView,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateView,
		updateFullTableInfoFunc:    updateFullTableInfoIgnore,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateView,
	},
	model.ActionModifyTableCharsetAndCollate: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncatePartition,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForTruncateTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForTruncatePartition,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateAndReorganizePartition,
		buildDDLEventFunc:          buildDDLEventForTruncateAndReorganizePartition,
	},
	model.ActionDropView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForDropView,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
		updateFullTableInfoFunc:    updateFullTableInfoIgnore,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForDropView,
	},
	model.ActionRecoverTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForNewTableDDL,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNewTableDDL,
	},
	model.ActionModifySchemaCharsetAndCollate: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForSchemaDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForSchemaDDL,
		updateFullTableInfoFunc:    updateFullTableInfoIgnore,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForModifySchemaCharsetAndCollate,
	},
	model.ActionAddPrimaryKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropPrimaryKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAlterIndexVisibility: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionExchangeTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForExchangePartition,
		updateDDLHistoryFunc:       updateDDLHistoryForExchangeTablePartition,
		updateFullTableInfoFunc:    updateFullTableInfoForExchangeTablePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForExchangeTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForExchangeTablePartition,
		extractTableInfoFunc:       extractTableInfoFuncForExchangeTablePartition,
		buildDDLEventFunc:          buildDDLEventForExchangeTablePartition,
	},
	model.ActionRenameTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTables,
		updateDDLHistoryFunc:       updateDDLHistoryForRenameTables,
		updateFullTableInfoFunc:    updateFullTableInfoForMultiTablesDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRenameTables,
		iterateEventTablesFunc:     iterateEventTablesForRenameTables,
		extractTableInfoFunc:       extractTableInfoFuncForRenameTables,
		buildDDLEventFunc:          buildDDLEventForRenameTables,
	},
	model.ActionCreateTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTables,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateTables,
		updateFullTableInfoFunc:    updateFullTableInfoForMultiTablesDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateTables,
		iterateEventTablesFunc:     iterateEventTablesForCreateTables,
		extractTableInfoFunc:       extractTableInfoFuncForCreateTables,
		buildDDLEventFunc:          buildDDLEventForCreateTables,
	},
	model.ActionMultiSchemaChange: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionReorganizePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForReorganizePartition,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForReorganizePartition,
		iterateEventTablesFunc:     iterateEventTablesForReorganizePartition,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateAndReorganizePartition,
		buildDDLEventFunc:          buildDDLEventForTruncateAndReorganizePartition,
	},
	model.ActionAlterTTLInfo: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTableTTL,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	model.ActionAlterTTLRemove: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTableTTL,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	model.ActionAlterTablePartitioning: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForAlterTablePartitioning,
		updateDDLHistoryFunc:       updateDDLHistoryForAlterTablePartitioning,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForAlterTablePartitioning,
		iterateEventTablesFunc:     iterateEventTablesForAlterTablePartitioning,
		extractTableInfoFunc:       extractTableInfoFuncForAlterTablePartitioning,
		buildDDLEventFunc:          buildDDLEventForAlterTablePartitioning,
	},
	model.ActionRemovePartitioning: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRemovePartitioning,
		updateDDLHistoryFunc:       updateDDLHistoryForRemovePartitioning,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForRemovePartitioning,
		iterateEventTablesFunc:     iterateEventTablesForRemovePartitioning,
		extractTableInfoFunc:       extractTableInfoFuncForRemovePartitioning,
		buildDDLEventFunc:          buildDDLEventForRemovePartitioning,
	},
	filter.ActionAddFullTextIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	filter.ActionCreateHybridIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateFullTableInfoFunc:    updateFullTableInfoForSingleTableDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
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

// schemaName should be "Name.O"
func findSchemaIDByName(databaseMap map[int64]*BasicDatabaseInfo, schemaName string) (int64, bool) {
	for id, info := range databaseMap {
		if strings.EqualFold(info.Name, schemaName) {
			return id, true
		}
	}
	return 0, false
}

// tableName should be "Name.O"
func findTableIDByName(tableMap map[int64]*BasicTableInfo, schemaID int64, tableName string) (int64, bool) {
	for id, info := range tableMap {
		if info.SchemaID == schemaID && strings.EqualFold(info.Name, tableName) {
			return id, true
		}
	}
	return 0, false
}

// =======
// buildPersistedDDLEventFunc start
// =======
func buildPersistedDDLEventCommon(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	var query string
	job := args.job
	// TODO: for ActionAddFullTextIndex and ActionCreateHybridIndex,
	// the parser cannot recogonize the query now, so we keep the original query in job.Query.
	// only in unit test job.Query may be empty
	if job.Type != filter.ActionAddFullTextIndex && job.Type != filter.ActionCreateHybridIndex && job.Query != "" {
		var err error
		query, err = transformDDLJobQuery(job)
		if err != nil {
			log.Error("transformDDLJobQuery failed", zap.String("query", job.Query), zap.Error(err))
			// send the original query to downstream if transform failed
			query = job.Query
		}
	} else {
		query = job.Query
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
		StartTs:        job.StartTS,
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
	event.Query = fmt.Sprintf("DROP VIEW %s", common.QuoteSchema(event.SchemaName, event.TableName))
	return event
}

func buildPersistedDDLEventForCreateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = event.TableInfo.Name.O
	setReferTableForCreateTableLike(&event, args)
	return event
}

func setReferTableForCreateTableLike(event *PersistedDDLEvent, args buildPersistedDDLEventFuncArgs) {
	if event.Query == "" {
		return
	}
	stmt, err := parser.New().ParseOneStmt(event.Query, "", "")
	if err != nil {
		log.Error("parse create table ddl failed",
			zap.String("query", event.Query),
			zap.Error(err))
		return
	}
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok || createStmt.ReferTable == nil {
		return
	}
	refTable := createStmt.ReferTable.Name.O
	refSchema := createStmt.ReferTable.Schema.O
	if refSchema == "" {
		refSchema = event.SchemaName
	}
	refSchemaID, ok := findSchemaIDByName(args.databaseMap, refSchema)
	if !ok {
		log.Warn("refer schema not found for create table like",
			zap.String("schema", refSchema),
			zap.String("table", refTable),
			zap.String("query", event.Query))
		return
	}
	refTableID, ok := findTableIDByName(args.tableMap, refSchemaID, refTable)
	if !ok {
		log.Warn("refer table not found for create table like",
			zap.String("schema", refSchema),
			zap.String("table", refTable),
			zap.String("query", event.Query))
		return
	}
	event.ExtraTableID = refTableID
	if partitions, ok := args.partitionMap[refTableID]; ok {
		event.ReferTablePartitionIDs = event.ReferTablePartitionIDs[:0]
		for id := range partitions {
			event.ReferTablePartitionIDs = append(event.ReferTablePartitionIDs, id)
		}
	}
}

func buildPersistedDDLEventForDropTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)
	event.TableName = getTableName(args.tableMap, event.TableID)
	// The query in job maybe "DROP TABLE test1.table1, test2.table2", we need rebuild it here.
	event.Query = fmt.Sprintf("DROP TABLE %s", common.QuoteSchema(event.SchemaName, event.TableName))
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

	// The old schema/table names cannot rely on ExtraSchemaName/ExtraTableName,
	// because the snapshot used by schema store may already reflect the post-rename state.
	// Example (after https://github.com/pingcap/tidb/pull/43341):
	//   table `test.t`, DDL `rename table t to test2.t;`, commit ts = 100
	//   snapshot at ts = 99 already shows `t` under `test2`
	//   => event.ExtraSchemaName becomes `test2`, which is wrong for the old name
	// SchemaStore can still use ExtraSchemaID to update internal state,
	// but the emitted event.Query must carry the correct old names.
	// Rebuild them with the following precedence:
	// 1. InvolvingSchemaInfo provides a fallback old schema/table pair, but names may be normalized.
	// 2. RenameTableArgs.OldSchemaName overrides the fallback when available.
	//    It is reliable in TiDB >= v8.5, but can be missing in older versions.
	// 3. The original query (if it specifies old schema) has the highest priority for identifier case.
	// 4. If the query omits old schema and ExtraSchemaID differs from SchemaID, use ExtraSchemaID to
	//    recover the old schema name from the schema store.
	oldSchemaName := ""
	oldTableName := ""
	oldSchemaSource := "unknown"
	if len(args.job.InvolvingSchemaInfo) > 0 {
		oldSchemaName = args.job.InvolvingSchemaInfo[0].Database
		oldTableName = args.job.InvolvingSchemaInfo[0].Table
		if oldSchemaName != "" {
			oldSchemaSource = "involving_schema_info"
		}
	}
	if args.job.Version == model.JobVersion1 || args.job.Version == model.JobVersion2 {
		if renameArgs, err := model.GetRenameTableArgs(args.job); err == nil {
			if renameArgs.OldSchemaName.O != "" {
				oldSchemaName = renameArgs.OldSchemaName.O
				oldSchemaSource = "rename_table_args"
			}
		} else {
			log.Warn("failed to get rename table args from ddl job",
				zap.Int64("jobID", args.job.ID),
				zap.String("query", event.Query),
				zap.Error(err))
		}
	}
	queryProvidedOldSchema := false
	if queryInfo, parsed := parseRenameTableQueryInfo(args.job.Query); parsed {
		if queryInfo.oldTableName != "" {
			oldTableName = queryInfo.oldTableName
		}
		if queryInfo.oldSchemaName != "" {
			oldSchemaName = queryInfo.oldSchemaName
			queryProvidedOldSchema = true
			oldSchemaSource = "query"
		}
	}
	// ExtraSchemaID can be incorrect due to snapshot timing, so only use it if the query
	// does not specify the old schema.
	if !queryProvidedOldSchema && event.ExtraSchemaID != 0 && event.ExtraSchemaID != event.SchemaID {
		if extraName := getSchemaName(args.databaseMap, event.ExtraSchemaID); extraName != "" {
			oldSchemaName = extraName
			oldSchemaSource = "extra_schema_id"
		}
	}
	if oldSchemaName != "" && oldTableName != "" {
		log.Info("rebuild rename table query",
			zap.Int64("jobID", event.ID),
			zap.String("query", event.Query),
			zap.Int64("schemaID", event.SchemaID),
			zap.String("schemaName", event.SchemaName),
			zap.String("tableName", event.TableName),
			zap.Int64("extraSchemaID", event.ExtraSchemaID),
			zap.String("extraSchemaName", event.ExtraSchemaName),
			zap.String("extraTableName", event.ExtraTableName),
			zap.String("oldSchemaName", oldSchemaName),
			zap.String("oldTableName", oldTableName),
			zap.String("oldSchemaSource", oldSchemaSource))
		event.Query = fmt.Sprintf("RENAME TABLE %s TO %s",
			common.QuoteSchema(oldSchemaName, oldTableName),
			common.QuoteSchema(event.SchemaName, event.TableName))
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

// buildPersistedDDLEventForExchangePartition build a exchange partition ddl event
// the TableID belongs to the new table(nt)
// the TableInfo belongs to the previous table(pt)
func buildPersistedDDLEventForExchangePartition(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	// these are the info of the normal table before exchange
	event.TableName = getTableName(args.tableMap, event.TableID)
	event.SchemaID = getSchemaID(args.tableMap, event.TableID)
	event.SchemaName = getSchemaName(args.databaseMap, event.SchemaID)

	// these are the info of the partition table after exchange
	event.ExtraTableID = event.TableInfo.ID
	event.ExtraTableName = getTableName(args.tableMap, event.ExtraTableID)
	event.ExtraSchemaID = getSchemaID(args.tableMap, event.ExtraTableID)
	event.ExtraSchemaName = getSchemaName(args.databaseMap, event.ExtraSchemaID)
	for id := range args.partitionMap[event.ExtraTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	// Note: event.ExtraTableInfo is set somewhere else,
	// because it is hard to get the table info of the normal table in this func.
	if event.Query != "" {
		upperQuery := strings.ToUpper(event.Query)
		idx1 := strings.Index(upperQuery, "EXCHANGE PARTITION") + len("EXCHANGE PARTITION")
		idx2 := strings.Index(upperQuery, "WITH TABLE")

		// Note that partition name should be parsed from original query, not the upperQuery.
		partName := strings.TrimSpace(event.Query[idx1:idx2])
		partName = common.UnquoteName(partName)
		event.Query = fmt.Sprintf("ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s",
			common.QuoteSchema(event.ExtraSchemaName, event.ExtraTableName),
			common.QuoteName(partName),
			common.QuoteSchema(event.SchemaName, event.TableName))

		if strings.HasSuffix(upperQuery, "WITHOUT VALIDATION") {
			event.Query += " WITHOUT VALIDATION"
		}
	} else {
		log.Warn("exchange partition query is empty, should only happen in unit tests",
			zap.Int64("jobID", event.ID))
	}
	return event
}

type renameTableQueryInfo struct {
	oldSchemaName string
	oldTableName  string
	newSchemaName string
	newTableName  string
}

func parseRenameTableQueryInfo(query string) (renameTableQueryInfo, bool) {
	if query == "" {
		return renameTableQueryInfo{}, false
	}
	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		log.Warn("parse rename table query failed",
			zap.String("query", query),
			zap.Error(err))
		return renameTableQueryInfo{}, false
	}

	switch s := stmt.(type) {
	case *ast.AlterTableStmt:
		return renameTableQueryInfo{
			oldSchemaName: s.Table.Schema.O,
			oldTableName:  s.Table.Name.O,
		}, true
	case *ast.RenameTableStmt:
		if len(s.TableToTables) == 0 {
			return renameTableQueryInfo{}, false
		}
		return renameTableQueryInfo{
			oldSchemaName: s.TableToTables[0].OldTable.Schema.O,
			oldTableName:  s.TableToTables[0].OldTable.Name.O,
			newSchemaName: s.TableToTables[0].NewTable.Schema.O,
			newTableName:  s.TableToTables[0].NewTable.Name.O,
		}, true
	default:
		return renameTableQueryInfo{}, false
	}
}

func parseRenameTablesQueryInfos(query string) ([]renameTableQueryInfo, bool) {
	if query == "" {
		return nil, false
	}
	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		log.Warn("parse rename tables query failed",
			zap.String("query", query),
			zap.Error(err))
		return nil, false
	}
	renameStmt, ok := stmt.(*ast.RenameTableStmt)
	if !ok {
		return nil, false
	}

	queryInfos := make([]renameTableQueryInfo, 0, len(renameStmt.TableToTables))
	for _, tableToTable := range renameStmt.TableToTables {
		queryInfos = append(queryInfos, renameTableQueryInfo{
			oldSchemaName: tableToTable.OldTable.Schema.O,
			oldTableName:  tableToTable.OldTable.Name.O,
			newSchemaName: tableToTable.NewTable.Schema.O,
			newTableName:  tableToTable.NewTable.Name.O,
		})
	}
	return queryInfos, true
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
	renameTableInfos := renameArgs.RenameTableInfos
	multipleTableInfos := args.job.BinlogInfo.MultipleTableInfos
	if len(renameTableInfos) != len(multipleTableInfos) {
		log.Panic("should not happen",
			zap.Int("renameArgsLen", len(renameTableInfos)),
			zap.Int("multipleTableInfosLen", len(multipleTableInfos)),
			zap.String("query", args.job.Query))
	}

	// RenameTableInfos may store normalized lower-case names,
	// while the original query can keep user-provided identifier case.
	// Prefer names parsed from the original query whenever possible.
	// See https://github.com/pingcap/ticdc/pull/2218 for background.
	queryInfos, queryParsed := parseRenameTablesQueryInfos(args.job.Query)
	if queryParsed && len(queryInfos) != len(renameTableInfos) {
		log.Panic("rename tables query info length is inconsistent with args",
			zap.Int("queryInfosLen", len(queryInfos)),
			zap.Int("renameArgsLen", len(renameTableInfos)),
			zap.String("query", args.job.Query))
	}

	// TiDB <= v8.1 may emit empty old table names for RENAME TABLES args.
	// See https://github.com/pingcap/tidb/pull/64421 for the upstream fix.
	if !queryParsed && renameTableInfos[0].OldTableName.O == "" {
		// TODO: return error instead of falling back to args once builder supports error propagation.
		log.Warn("rename tables args miss old table name and query is unavailable, keep args as-is",
			zap.Int("tableCount", len(renameTableInfos)),
			zap.String("query", args.job.Query))
	}

	var querys []string
	for i, info := range renameTableInfos {
		oldSchemaID := info.OldSchemaID
		oldSchemaName := info.OldSchemaName.O
		oldTableName := info.OldTableName.O
		newSchemaName := getSchemaName(args.databaseMap, info.NewSchemaID)
		newTableName := info.NewTableName.O
		if queryParsed {
			queryInfo := queryInfos[i]
			if queryInfo.oldSchemaName != "" {
				oldSchemaName = queryInfo.oldSchemaName
			}
			if queryInfo.oldTableName != "" {
				oldTableName = queryInfo.oldTableName
			}
			if queryInfo.newSchemaName != "" {
				newSchemaName = queryInfo.newSchemaName
			}
			if queryInfo.newTableName != "" {
				newTableName = queryInfo.newTableName
			}
		}

		event.ExtraSchemaIDs = append(event.ExtraSchemaIDs, oldSchemaID)
		event.ExtraSchemaNames = append(event.ExtraSchemaNames, oldSchemaName)
		event.ExtraTableNames = append(event.ExtraTableNames, oldTableName)
		event.SchemaIDs = append(event.SchemaIDs, info.NewSchemaID)
		event.SchemaNames = append(event.SchemaNames, newSchemaName)
		querys = append(querys, fmt.Sprintf("RENAME TABLE %s TO %s;",
			common.QuoteSchema(oldSchemaName, oldTableName),
			common.QuoteSchema(newSchemaName, newTableName)))
	}

	event.Query = strings.Join(querys, "")
	event.MultipleTableInfos = multipleTableInfos
	// we have to reverse MultipleTableInfos to get correct schema name
	// see https://github.com/pingcap/tidb/issues/63710
	//
	// If renaming tables creates cycles, some tables may not be created, and their table IDs may coincide with the latest table.
	// For example: renaming table 'a' to 'common.c', 'b' to 'a', and 'common.c' to 'b' results in table 'c' not being created,
	// with its table ID equal to that of 'b'. This leads to incorrect table info in the function `extractTableInfoFuncForRenameTables`.
	// To avoid this situation, we must use the latest table info and disregard previous entries when encountering duplicate table info.
	// Therefore, we set the table ID of the previous table info to `InvalidTableID` to ensure that each table ID is unique.
	visitTableIDs := make(map[int64]struct{})
	for i := len(event.MultipleTableInfos) - 1; i >= 0; i-- {
		id := event.MultipleTableInfos[i].ID
		if _, ok := visitTableIDs[id]; ok {
			event.MultipleTableInfos[i].ID = InvalidTableID
		} else {
			visitTableIDs[id] = struct{}{}
		}
	}
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
	// Note: for create table, this ddl event will not be sent to table dispatchers by default.
	// adding it to ddl history is just for building table info store.
	if isPartitionTable(args.ddlEvent.TableInfo) {
		// for partition table, we only care the ddl history of physical table ids.
		for _, partitionID := range getAllPartitionIDs(args.ddlEvent.TableInfo) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, partitionID)
		}
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.TableID)
	}
	if args.ddlEvent.Type == byte(model.ActionCreateTable) && args.ddlEvent.ExtraTableID != 0 {
		if len(args.ddlEvent.ReferTablePartitionIDs) > 0 {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.ReferTablePartitionIDs...)
		} else {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.ExtraTableID)
		}
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
		if info.ID == InvalidTableID {
			continue
		}
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

func updateFullTableInfoIgnore(args updateFullTableInfoFuncArgs) {}

func updateFullTableInfoForDropSchema(args updateFullTableInfoFuncArgs) {
	for tableID := range args.databaseMap[args.event.SchemaID].Tables {
		delete(args.tableInfoMap, tableID)
	}
}

func updateFullTableInfoForSingleTableDDL(args updateFullTableInfoFuncArgs) {
	args.tableInfoMap[args.event.TableID] = args.event.TableInfo
}

func updateFullTableInfoForDropTable(args updateFullTableInfoFuncArgs) {
	delete(args.tableInfoMap, args.event.TableID)
}

func updateFullTableInfoForTruncateTable(args updateFullTableInfoFuncArgs) {
	delete(args.tableInfoMap, args.event.TableID)
	args.tableInfoMap[args.event.ExtraTableID] = args.event.TableInfo
}

func updateFullTableInfoForExchangeTablePartition(args updateFullTableInfoFuncArgs) {
	physicalIDs := getAllPartitionIDs(args.event.TableInfo)
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, physicalIDs)
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition",
			zap.Int64s("droppedIDs", droppedIDs))
	}
	// set new normal table info
	targetPartitionID := droppedIDs[0]
	normalTableID := args.event.TableID
	normalTableInfo := args.tableInfoMap[normalTableID]
	normalTableInfo.ID = targetPartitionID
	args.tableInfoMap[targetPartitionID] = normalTableInfo
	delete(args.tableInfoMap, normalTableID)
	// update partition table info
	partitionTableID := args.event.ExtraTableID
	args.tableInfoMap[partitionTableID] = args.event.TableInfo
}

func updateFullTableInfoForMultiTablesDDL(args updateFullTableInfoFuncArgs) {
	if args.event.MultipleTableInfos == nil {
		log.Panic("multiple table infos should not be nil")
	}
	for _, info := range args.event.MultipleTableInfos {
		args.tableInfoMap[info.ID] = info
	}
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
		partitionInfo.AddPartitionIDs(getAllPartitionIDs(args.event.TableInfo)...)
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
		partitionInfo.AddPartitionIDs(getAllPartitionIDs(args.event.TableInfo)...)
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
		if info.ID == InvalidTableID {
			continue
		}
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
			partitionInfo.AddPartitionIDs((getAllPartitionIDs(info))...)
			args.partitionMap[info.ID] = partitionInfo
		}
	}
}

func updateSchemaMetadataForReorganizePartition(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.TableID
	physicalIDs := getAllPartitionIDs(args.event.TableInfo)
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, physicalIDs)
	args.partitionMap[tableID].RemovePartitionIDs(droppedIDs...)
	newCreatedIDs := getCreatedIDs(args.event.PrevPartitions, physicalIDs)
	args.partitionMap[tableID].AddPartitionIDs(newCreatedIDs...)
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
	args.partitionMap[newTableID].AddPartitionIDs(getAllPartitionIDs(args.event.TableInfo)...)
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
		if info.ID == InvalidTableID {
			continue
		}
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
				return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
			}
		}
	} else {
		if tableID == event.TableID {
			return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
		}
	}

	// The DDL "CREATE TABLE ... LIKE ..." may be added to the ddl history of the referenced table
	// (or its physical partition IDs) to ensure those tables are blocked while this DDL is executing.
	// It does not change the schema of the referenced table itself, so we should ignore it when
	// building the table info store for the referenced table.
	if event.Type == byte(model.ActionCreateTable) && event.ExtraTableID != 0 {
		if tableID == event.ExtraTableID {
			return nil, false
		}
		for _, id := range event.ReferTablePartitionIDs {
			if tableID == id {
				return nil, false
			}
		}
	}
	log.Panic("should not reach here",
		zap.Any("type", event.Type),
		zap.String("query", event.Query),
		zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForExchangeTablePartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if tableID == event.TableID {
		// old normal table id, return the table info of the partition table
		return common.WrapTableInfo(event.ExtraSchemaName, event.TableInfo), false
	}
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
		event.SchemaName,
		ast.NewCIStr(event.TableName).O,
		tableID,
		false,
		columnSchema,
		event.TableInfo,
	)
	return tableInfo, false
}

func extractTableInfoFuncIgnore(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	return nil, false
}

func extractTableInfoFuncForDropTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return nil, true
			}
		}
	} else {
		if event.TableID == tableID {
			return nil, true
		}
	}
	log.Panic("should not reach here",
		zap.Bool("isPartitionTable", isPartitionTable(event.TableInfo)),
		zap.Int64("eventTableID", event.TableID),
		zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForTruncateTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
			}
		}
		return nil, true
	}
	if tableID == event.ExtraTableID {
		return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
	}
	if tableID == event.TableID {
		return nil, true
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForAddPartition(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	newCreatedIDs := getCreatedIDs(event.PrevPartitions, getAllPartitionIDs(event.TableInfo))
	for _, partition := range newCreatedIDs {
		if tableID == partition {
			return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
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
			return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
		}
	}
	return nil, false
}

func extractTableInfoFuncForRenameTables(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	for i, tableInfo := range event.MultipleTableInfos {
		if tableInfo.ID == InvalidTableID {
			continue
		}
		if isPartitionTable(tableInfo) {
			for _, partitionID := range getAllPartitionIDs(tableInfo) {
				if tableID == partitionID {
					return common.WrapTableInfo(event.SchemaNames[i], tableInfo), false
				}
			}
		} else {
			if tableID == tableInfo.ID {
				return common.WrapTableInfo(event.SchemaNames[i], tableInfo), false
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
					return common.WrapTableInfo(event.SchemaName, tableInfo), false
				}
			}
		} else {
			if tableID == tableInfo.ID {
				return common.WrapTableInfo(event.SchemaName, tableInfo), false
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
			return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForRemovePartitioning(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if event.TableID == tableID {
		return common.WrapTableInfo(event.SchemaName, event.TableInfo), false
	}
	for _, partitionID := range event.PrevPartitions {
		if tableID == partitionID {
			return nil, true
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

// =======
// buildDDLEvent begin
// =======

func buildDDLEventCommon(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tiDBOnly bool) (commonEvent.DDLEvent, bool, error) {
	var wrapTableInfo *common.TableInfo
	// Note: not all ddl types will respect the `filtered` result: create tables, rename tables, rename table, exchange table partition.
	filtered, notSync := false, false
	var err error
	if tableFilter != nil {
		filtered, notSync, err = filterDDL(
			tableFilter,
			rawEvent.SchemaName,
			rawEvent.TableName,
			rawEvent.Query,
			model.ActionType(rawEvent.Type),
			rawEvent.TableInfo,
			rawEvent.StartTs,
		)
		if err != nil {
			return commonEvent.DDLEvent{}, false, err
		}
		if rawEvent.ExtraSchemaName != "" && rawEvent.ExtraTableName != "" {
			filteredByExtraTable, notSyncByExtraTable := false, false
			filteredByExtraTable, notSyncByExtraTable, err = filterDDL(
				tableFilter,
				rawEvent.ExtraSchemaName,
				rawEvent.ExtraTableName,
				rawEvent.Query,
				model.ActionType(rawEvent.Type),
				rawEvent.TableInfo,
				rawEvent.StartTs,
			)
			if err != nil {
				return commonEvent.DDLEvent{}, false, err
			}
			filtered = filtered && filteredByExtraTable
			notSync = notSync || notSyncByExtraTable
		}
	}

	if filtered {
		log.Info("discard DDL by filter(ShouldDiscardDDL)",
			zap.String("schema", rawEvent.SchemaName),
			zap.String("extraSchema", rawEvent.ExtraSchemaName),
			zap.String("table", rawEvent.TableName),
			zap.String("extraTable", rawEvent.ExtraTableName),
			zap.String("query", rawEvent.Query),
			zap.Any("ddlType", model.ActionType(rawEvent.Type)),
			zap.Uint64("startTs", rawEvent.StartTs),
		)
	}
	if notSync {
		log.Info("ignore DDL by event filter(ShouldIgnoreDDL), it will be skipped in dispatcher",
			zap.String("schema", rawEvent.SchemaName),
			zap.String("extraSchema", rawEvent.ExtraSchemaName),
			zap.String("table", rawEvent.TableName),
			zap.String("extraTable", rawEvent.ExtraTableName),
			zap.String("query", rawEvent.Query),
			zap.Any("ddlType", model.ActionType(rawEvent.Type)),
			zap.Uint64("startTs", rawEvent.StartTs),
		)
	}

	if rawEvent.TableInfo != nil {
		wrapTableInfo = common.WrapTableInfo(rawEvent.SchemaName, rawEvent.TableInfo)
	}

	return commonEvent.DDLEvent{
		Version: commonEvent.DDLEventVersion1, // Must set version explicitly
		Type:    rawEvent.Type,
		// TODO: whether the following four fields are needed
		SchemaID:   rawEvent.SchemaID,
		SchemaName: rawEvent.SchemaName,
		TableName:  rawEvent.TableName,

		Query:      rawEvent.Query,
		TableInfo:  wrapTableInfo,
		StartTs:    rawEvent.StartTs,
		FinishedTs: rawEvent.FinishedTs,
		TiDBOnly:   tiDBOnly,
		BDRMode:    rawEvent.BDRRole,

		NotSync: notSync,
	}, !filtered, nil
}

func filterDDL(tableFilter filter.Filter, schema, table, query string, ddlType model.ActionType, tableInfo *model.TableInfo, startTs uint64) (bool, bool, error) {
	filtered, notSync := false, false
	if tableFilter != nil {
		filtered = tableFilter.ShouldDiscardDDL(schema, table, ddlType, common.WrapTableInfo(schema, tableInfo))
	}
	if !filtered {
		// If the DDL is not filtered, we need to check whether the DDL should be synced to downstream.
		// For example, if a `TRUNCATE TABLE` DDL is filtered by event filter,
		// and we don't need to sync it to downstream, but the DML events of the new truncated table
		// should be sent to downstream.
		// So we should send the `TRUNCATE TABLE` DDL event to table trigger,
		// to ensure the new truncated table can be handled correctly.
		if tableFilter != nil {
			var err error
			// The core of whether `NotSync` is set to true is whether the DML events should be sent to downstream.
			// If the table is filtered, we don't need to send the DML events to downstream.
			// So we can just ignore the `TRUNCATE TABLE` DDL in here.
			// Thus, we set `NotSync` to true.
			// If the table is not filtered, we should send the DML events to downstream.
			// So we set `NotSync` to false, and this corresponding DDL can be applied to log service.
			notSync, err = tableFilter.ShouldIgnoreDDL(schema, table, query, ddlType, startTs)
			if err != nil {
				return false, false, err
			}
		}
	}
	return filtered, notSync, nil
}

func buildDDLEventForCreateSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	return ddlEvent, true, err
}

func buildDDLEventForDropSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
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
	return ddlEvent, true, err
}

func buildDDLEventForModifySchemaCharsetAndCollate(
	rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64,
) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.SchemaID,
	}
	return ddlEvent, true, err
}

func ignoreParseErrorForActiveActiveSyntax(err error, query string) bool {
	// ignore the syntax error caused by `ACTIVE ACTIVE or SOFTDELETE` Related syntax
	if errors.Is(err, parser.ErrSyntax) && (strings.Contains(strings.ToUpper(query), "ACTIVE_ACTIVE") || strings.Contains(strings.ToUpper(query), "SOFTDELETE")) {
		return true
	}
	return false
}

func buildDDLEventForNewTableDDL(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	if rawEvent.ExtraTableID != 0 {
		if len(rawEvent.ReferTablePartitionIDs) > 0 {
			ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.ReferTablePartitionIDs...)
		} else {
			ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.ExtraTableID)
		}
	}
	if isPartitionTable(rawEvent.TableInfo) {
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
		splitable := isSplitable(rawEvent.TableInfo)
		for _, id := range physicalIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID:  rawEvent.SchemaID,
				TableID:   id,
				Splitable: splitable,
			})
		}
	} else {
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID:  rawEvent.SchemaID,
				TableID:   rawEvent.TableID,
				Splitable: isSplitable(rawEvent.TableInfo),
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
	if rawEvent.Query != "" {
		stmt, err := parser.New().ParseOneStmt(rawEvent.Query, "", "")
		if err != nil {
			log.Error("parse create table ddl failed",
				zap.String("query", rawEvent.Query),
				zap.Error(err))

			if ignoreParseErrorForActiveActiveSyntax(err, rawEvent.Query) {
				return ddlEvent, true, nil
			} else {
				return ddlEvent, false, err
			}
		}

		if createStmt, ok := stmt.(*ast.CreateTableStmt); ok && createStmt.ReferTable != nil {
			refTable := createStmt.ReferTable.Name.O
			refSchema := createStmt.ReferTable.Schema.O
			if refSchema == "" {
				refSchema = rawEvent.SchemaName
			}
			ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{
				{
					SchemaName: refSchema,
					TableName:  refTable,
				},
			}
		}

	}
	return ddlEvent, true, err
}

func buildDDLEventForDropTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	if isPartitionTable(rawEvent.TableInfo) {
		allPhysicalTableIDs := getAllPartitionIDs(rawEvent.TableInfo)
		allPhysicalTableIDsAndDDLSpanID := make([]int64, 0, len(rawEvent.TableInfo.Partition.Definitions)+1)
		allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, allPhysicalTableIDs...)
		allPhysicalTableIDsAndDDLSpanID = append(allPhysicalTableIDsAndDDLSpanID, common.DDLSpanTableID)
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
			TableIDs:      []int64{rawEvent.TableID, common.DDLSpanTableID},
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
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForNormalDDLOnSingleTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	// means it is filtered
	if !ok {
		return ddlEvent, false, err
	}

	// the event is related to the partition table
	if rawEvent.TableInfo.Partition != nil {
		partitionTableIDs := make([]int64, 0)
		for _, partition := range rawEvent.TableInfo.Partition.Definitions {
			partitionTableIDs = append(partitionTableIDs, partition.ID)
		}

		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      partitionTableIDs,
		}
	} else {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForNormalDDLOnSingleTableForTiDB(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	// the event is related to the partition table
	if rawEvent.TableInfo.Partition != nil {
		partitionTableIDs := make([]int64, 0)
		for _, partition := range rawEvent.TableInfo.Partition.Definitions {
			partitionTableIDs = append(partitionTableIDs, partition.ID)
		}

		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      partitionTableIDs,
		}
	} else {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForTruncateTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	if isPartitionTable(rawEvent.TableInfo) {
		prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
		prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, common.DDLSpanTableID)
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
		splitable := isSplitable(rawEvent.TableInfo)
		for _, id := range physicalIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID:  rawEvent.SchemaID,
				TableID:   id,
				Splitable: splitable,
			})
		}
	} else {
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID:  rawEvent.SchemaID,
				TableID:   rawEvent.ExtraTableID,
				Splitable: isSplitable(rawEvent.TableInfo),
			},
		}
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, common.DDLSpanTableID},
		}
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForRenameTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.ExtraSchemaName = rawEvent.ExtraSchemaName
	ddlEvent.ExtraTableName = rawEvent.ExtraTableName
	ignorePrevTable, ignoreCurrentTable := false, false
	notSyncPrevTable := false
	if tableFilter != nil {
		ignorePrevTable, notSyncPrevTable, err = filterDDL(
			tableFilter,
			rawEvent.ExtraSchemaName,
			rawEvent.ExtraTableName,
			rawEvent.Query,
			model.ActionRenameTable,
			rawEvent.TableInfo,
			rawEvent.StartTs,
		)
		if err != nil {
			return commonEvent.DDLEvent{}, false, err
		}
		ignoreCurrentTable, _, err = filterDDL(
			tableFilter,
			rawEvent.SchemaName,
			rawEvent.TableName,
			rawEvent.Query,
			model.ActionRenameTable,
			rawEvent.TableInfo,
			rawEvent.StartTs,
		)
		if err != nil {
			return commonEvent.DDLEvent{}, false, err
		}
	}
	if isPartitionTable(rawEvent.TableInfo) {
		allPhysicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		if !ignorePrevTable {
			allPhysicalIDsAndDDLSpanID := make([]int64, 0, len(allPhysicalIDs)+1)
			allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, allPhysicalIDs...)
			allPhysicalIDsAndDDLSpanID = append(allPhysicalIDsAndDDLSpanID, common.DDLSpanTableID)
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      allPhysicalIDsAndDDLSpanID,
			}
			ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.ExtraSchemaName, TableName: rawEvent.ExtraTableName}}
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
			ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query).Error()
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
				TableIDs:      []int64{rawEvent.TableID, common.DDLSpanTableID},
			}
			// Set the old schema and table names for rename table.
			ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.ExtraSchemaName, TableName: rawEvent.ExtraTableName}}
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
			ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query).Error()
		} else {
			// if the table is both filtered out before and after rename table, the ddl should not be fetched
			log.Panic("should not build a ignored rename table ddl",
				zap.String("DDL", rawEvent.Query),
				zap.Int64("jobID", rawEvent.ID),
				zap.Int64("schemaID", rawEvent.SchemaID),
				zap.Int64("tableID", rawEvent.TableID))
		}
	}
	// For rename table, we only set NotSync to true when the previous table is filtered.
	ddlEvent.NotSync = notSyncPrevTable
	return ddlEvent, true, err
}

func buildDDLEventForAddPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, common.DDLSpanTableID)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      prevPartitionsAndDDLSpanID,
	}
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(newCreatedIDs))
	splitable := isSplitable(rawEvent.TableInfo)
	for _, id := range newCreatedIDs {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID:  rawEvent.SchemaID,
			TableID:   id,
			Splitable: splitable,
		})
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForDropPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, common.DDLSpanTableID)
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
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForCreateView(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeAll,
	}
	return ddlEvent, true, err
}

func buildDDLEventForDropView(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	return ddlEvent, true, err
}

func buildDDLEventForTruncateAndReorganizePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	prevPartitionsAndDDLSpanID := make([]int64, 0, len(rawEvent.PrevPartitions)+1)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, rawEvent.PrevPartitions...)
	prevPartitionsAndDDLSpanID = append(prevPartitionsAndDDLSpanID, common.DDLSpanTableID)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      prevPartitionsAndDDLSpanID,
	}
	physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
	newCreatedIDs := getCreatedIDs(rawEvent.PrevPartitions, physicalIDs)
	splitable := isSplitable(rawEvent.TableInfo)
	for _, id := range newCreatedIDs {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID:  rawEvent.SchemaID,
			TableID:   id,
			Splitable: splitable,
		})
	}
	droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      droppedIDs,
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForExchangeTablePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.ExtraSchemaName = rawEvent.ExtraSchemaName
	ddlEvent.ExtraTableName = rawEvent.ExtraTableName
	// TODO: rawEvent.TableInfo is not correct for ignoreNormalTable
	// ignoreNormalTable and ignorePartitionTable are the table info before exchange
	ignoreNormalTable, ignorePartitionTable := false, false
	notSyncPartitionTable := false
	if tableFilter != nil {
		ignoreNormalTable, _, err = filterDDL(
			tableFilter,
			rawEvent.SchemaName,
			rawEvent.TableName,
			rawEvent.Query,
			model.ActionExchangeTablePartition,
			// rawEvent.ExtraTableInfo is the normal table info before exchange.
			rawEvent.ExtraTableInfo.ToTiDBTableInfo(),
			rawEvent.StartTs,
		)
		if err != nil {
			return commonEvent.DDLEvent{}, false, err
		}
		ignorePartitionTable, notSyncPartitionTable, err = filterDDL(
			tableFilter,
			rawEvent.ExtraSchemaName,
			rawEvent.ExtraTableName,
			rawEvent.Query,
			model.ActionExchangeTablePartition,
			// rawEvent.TableInfo is the partition table info after exchange,
			// typically, we should use the table info before exchange to do filtering,
			// but we don't have the table info before exchange here,
			// so we use the partition table info after exchange instead,
			// because the difference between this two table info is just one partition table id changed.
			rawEvent.TableInfo,
			rawEvent.StartTs,
		)
		if err != nil {
			return commonEvent.DDLEvent{}, false, err
		}
	}
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
			TableIDs:      []int64{rawEvent.TableID, targetPartitionID, common.DDLSpanTableID},
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
		ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{
			{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName},
			{SchemaName: rawEvent.ExtraSchemaName, TableName: rawEvent.ExtraTableName},
		}
	} else if !ignoreNormalTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID, common.DDLSpanTableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.TableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID:  rawEvent.SchemaID,
				TableID:   targetPartitionID,
				Splitable: isSplitable(rawEvent.TableInfo),
			},
		}
		ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{
			{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName},
		}
	} else if !ignorePartitionTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{targetPartitionID, common.DDLSpanTableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{targetPartitionID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID:  rawEvent.ExtraSchemaID,
				TableID:   rawEvent.TableID,
				Splitable: isSplitable(rawEvent.TableInfo),
			},
		}
		ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{
			{SchemaName: rawEvent.ExtraSchemaName, TableName: rawEvent.ExtraTableName},
		}
	} else {
		log.Fatal("should not happen")
	}
	// For exchange table partition, we only set NotSync to true when the partition table is filtered.
	ddlEvent.NotSync = notSyncPartitionTable
	ddlEvent.MultipleTableInfos = []*common.TableInfo{
		common.WrapTableInfo(rawEvent.SchemaName, rawEvent.TableInfo),
		rawEvent.ExtraTableInfo,
	}
	if tableID != 0 {
		// Here we set TableInfo to the table info of tableID.
		// First, check whether the tableID is a normal table after exchange.
		// If false, set TableInfo to rawEvent.TableInfo, because the rawEvent.TableInfo is the partition table info after exchange.
		// NOTE: ddlEvent.TableInfo is already the rawEvent.TableInfo in buildDDLEventCommon. So we don't need to set it again if false.
		// If true, set TableInfo to rawEvent.ExtraTableInfo,
		// because the rawEvent.ExtraTableInfo is the normal table info before exchange,
		// but the tableID is the normal table after exchange, so we need to get a new TableInfo for it.
		// NOTE: We can't just check tableID == rawEvent.ExtraTableInfo.TableName.TableID,
		// because rawEvent.ExtraTableInfo is the table info before exchange,
		// and the tableID is the table id after exchange.
		isNormalTableAfterExchange := true
		for _, id := range physicalIDs {
			if id == tableID {
				isNormalTableAfterExchange = false
				break
			}
		}
		if isNormalTableAfterExchange {
			ddlEvent.TableInfo = common.NewTableInfo(
				rawEvent.ExtraSchemaName,
				ast.NewCIStr(rawEvent.ExtraTableName).O,
				tableID,
				false,
				rawEvent.ExtraTableInfo.ShadowCopyColumnSchema(),
				rawEvent.ExtraTableInfo.ToTiDBTableInfo(),
			)
		}
	}
	return ddlEvent, true, err
}

func buildDDLEventForRenameTables(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, _, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	querys, err := commonEvent.SplitQueries(rawEvent.Query)
	if err != nil {
		log.Panic("split queries failed", zap.Error(err))
	}
	var addNames, dropNames []commonEvent.SchemaTableName
	allFiltered := true
	resultQuerys := make([]string, 0)
	tableInfos := make([]*common.TableInfo, 0)
	blockedTableNames := make([]commonEvent.SchemaTableName, 0)
	if len(querys) != len(rawEvent.MultipleTableInfos) {
		log.Panic("rename tables length is not equal table infos", zap.Any("querys", querys), zap.Any("tableInfos", rawEvent.MultipleTableInfos))
	}
	for i, tableInfo := range rawEvent.MultipleTableInfos {
		ignorePrevTable, ignoreCurrentTable := false, false
		notSyncPrevTable := false
		if tableInfo != nil {
			ignorePrevTable, notSyncPrevTable, err = filterDDL(
				tableFilter, rawEvent.ExtraSchemaNames[i], rawEvent.ExtraTableNames[i], rawEvent.Query, model.ActionType(rawEvent.Type), tableInfo, rawEvent.StartTs)
			if err != nil {
				return commonEvent.DDLEvent{}, false, err
			}
			ignoreCurrentTable, _, err = filterDDL(
				tableFilter, rawEvent.SchemaNames[i], tableInfo.Name.O, rawEvent.Query, model.ActionType(rawEvent.Type), tableInfo, rawEvent.StartTs)
			if err != nil {
				return commonEvent.DDLEvent{}, false, err
			}
		}
		if ignorePrevTable && ignoreCurrentTable {
			continue
		}
		allFiltered = false
		if isPartitionTable(rawEvent.TableInfo) {
			allPhysicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
			if !ignorePrevTable {
				if !notSyncPrevTable {
					// only when the previous table is not filtered and not NotSync, we add the query and table info
					// to the DDLEvent. otherwise, we just skip it.
					resultQuerys = append(resultQuerys, querys[i])
					tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaNames[i], tableInfo))
				}
				blockedTableNames = append(blockedTableNames, commonEvent.SchemaTableName{
					SchemaName: rawEvent.ExtraSchemaNames[i],
					TableName:  rawEvent.ExtraTableNames[i],
				})
				if tableInfo.ID != InvalidTableID {
					ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, allPhysicalIDs...)
				}
				if !ignoreCurrentTable {
					// check whether schema change
					if rawEvent.ExtraSchemaIDs[i] != rawEvent.SchemaIDs[i] && tableInfo.ID != InvalidTableID {
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
					if tableInfo.ID != InvalidTableID {
						ddlEvent.NeedDroppedTables.TableIDs = append(ddlEvent.NeedDroppedTables.TableIDs, allPhysicalIDs...)
					}
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				}
			} else if !ignoreCurrentTable {
				// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
				ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query).Error()
			} else {
				// if the table is both filtered out before and after rename table, ignore
			}
			if tableID != 0 {
				// This ddl event is for the dispatcher corresponding to tableID,
				// so we should set the table info in DDLEvent to the table info of tableID.
				// Otherwise, the table info in DDLEvent is always the first table info in rename tables.
				// And dispatcher only checks TableInfo instead of MultipleTableInfos for now, even it's a multi-table DDL.
				// tableID will be a physical table ID if the table is a partition table.
				// So here we need to check whether tableID is in allPhysicalIDs.
				// If we find it, we set the TableInfo in DDLEvent to the table info of tableID.
				for _, id := range allPhysicalIDs {
					if id == tableID {
						ddlEvent.TableInfo = common.WrapTableInfo(rawEvent.SchemaNames[i], tableInfo)
						break
					}
				}
			}
		} else {
			if !ignorePrevTable {
				if !notSyncPrevTable {
					// only when the previous table is not filtered and not NotSync, we add the query and table info
					// to the DDLEvent. otherwise, we just skip it.
					resultQuerys = append(resultQuerys, querys[i])
					tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaNames[i], tableInfo))
				}
				blockedTableNames = append(blockedTableNames, commonEvent.SchemaTableName{
					SchemaName: rawEvent.ExtraSchemaNames[i],
					TableName:  rawEvent.ExtraTableNames[i],
				})
				if tableInfo.ID != InvalidTableID {
					ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, tableInfo.ID)
				}
				if !ignoreCurrentTable {
					if rawEvent.ExtraSchemaIDs[i] != rawEvent.SchemaIDs[i] && tableInfo.ID != InvalidTableID {
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
					if tableInfo.ID != InvalidTableID {
						ddlEvent.NeedDroppedTables.TableIDs = append(ddlEvent.NeedDroppedTables.TableIDs, tableInfo.ID)
					}
					dropNames = append(dropNames, commonEvent.SchemaTableName{
						SchemaName: rawEvent.ExtraSchemaNames[i],
						TableName:  rawEvent.ExtraTableNames[i],
					})
				}
			} else if !ignoreCurrentTable {
				// ignorePrevTable & !ignoreCurrentTable is not allowed as in: https://docs.pingcap.com/tidb/dev/ticdc-ddl
				ddlEvent.Err = cerror.ErrSyncRenameTableFailed.GenWithStackByArgs(rawEvent.TableID, rawEvent.Query).Error()
			} else {
				// ignore
			}
			if tableID != 0 {
				// Here we set TableInfo to the table info of tableID.
				// tableID is a normal table ID if the table is not a partition table.
				if tableInfo.ID == tableID {
					ddlEvent.TableInfo = common.WrapTableInfo(rawEvent.SchemaNames[i], tableInfo)
				}
			}
		}
	}
	if allFiltered {
		return commonEvent.DDLEvent{}, false, err
	}
	if addNames != nil || dropNames != nil {
		ddlEvent.TableNameChange = &commonEvent.TableNameChange{
			AddName:  addNames,
			DropName: dropNames,
		}
	}
	ddlEvent.Query = strings.Join(resultQuerys, "")
	ddlEvent.BlockedTableNames = blockedTableNames
	ddlEvent.MultipleTableInfos = tableInfos
	// For rename tables, we don't set NotSync, because we have already filter out the querys and multiple table infos which are NotSync.
	// So here we just set NotSync to false.
	ddlEvent.NotSync = false
	return ddlEvent, true, err
}

func buildDDLEventForCreateTables(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, _, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	physicalTableCount := 0
	logicalTableCount := 0
	allFiltered := true
	for _, info := range rawEvent.MultipleTableInfos {
		if tableFilter != nil && tableFilter.ShouldDiscardDDL(
			rawEvent.SchemaName, info.Name.O, model.ActionType(rawEvent.Type), common.WrapTableInfo(rawEvent.SchemaName, info)) {
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
		return commonEvent.DDLEvent{}, false, err
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
		filtered, notSync := false, false
		if tableFilter != nil {
			filtered, notSync, err = filterDDL(
				tableFilter, rawEvent.SchemaName, info.Name.O, rawEvent.Query, model.ActionType(rawEvent.Type), info, rawEvent.StartTs)
			if err != nil {
				return commonEvent.DDLEvent{}, false, err
			}
			if filtered {
				log.Info("discard DDL by filter in create tables", zap.String("schemaName", rawEvent.SchemaName), zap.String("tableName", info.Name.O))
				continue
			}
		}
		if isPartitionTable(info) {
			splitable := isSplitable(info)
			for _, partitionID := range getAllPartitionIDs(info) {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID:  rawEvent.SchemaID,
					TableID:   partitionID,
					Splitable: splitable,
				})
			}
		} else {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID:  rawEvent.SchemaID,
				TableID:   info.ID,
				Splitable: isSplitable(info),
			})
		}
		addName = append(addName, commonEvent.SchemaTableName{
			SchemaName: rawEvent.SchemaName,
			TableName:  info.Name.O,
		})
		if !notSync {
			// only when the table is not NotSync, we add the query and table info
			// to the DDLEvent. otherwise, we just skip it.
			resultQuerys = append(resultQuerys, querys[i])
			tableInfos = append(tableInfos, common.WrapTableInfo(rawEvent.SchemaName, info))
		}
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		AddName: addName,
	}
	ddlEvent.Query = strings.Join(resultQuerys, "")
	ddlEvent.MultipleTableInfos = tableInfos
	// For create tables, we don't set NotSync, because we have already filter out the querys and multiple table infos which are NotSync.
	// So here we just set NotSync to false.
	ddlEvent.NotSync = false
	if len(ddlEvent.NeedAddedTables) == 0 {
		log.Fatal("should not happen")
	}
	return ddlEvent, true, err
}

func buildDDLEventForAlterTablePartitioning(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
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
	splitable := isSplitable(rawEvent.TableInfo)
	for _, id := range getAllPartitionIDs(rawEvent.TableInfo) {
		ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
			SchemaID:  rawEvent.SchemaID,
			TableID:   id,
			Splitable: splitable,
		})
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}

func buildDDLEventForRemovePartitioning(rawEvent *PersistedDDLEvent, tableFilter filter.Filter, tableID int64) (commonEvent.DDLEvent, bool, error) {
	ddlEvent, ok, err := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	if err != nil {
		return commonEvent.DDLEvent{}, false, err
	}
	if !ok {
		return ddlEvent, false, err
	}
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{common.DDLSpanTableID},
	}
	ddlEvent.BlockedTables.TableIDs = append(ddlEvent.BlockedTables.TableIDs, rawEvent.PrevPartitions...)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      rawEvent.PrevPartitions,
	}
	ddlEvent.NeedAddedTables = []commonEvent.Table{
		{
			SchemaID:  rawEvent.SchemaID,
			TableID:   rawEvent.TableID,
			Splitable: isSplitable(rawEvent.TableInfo),
		},
	}
	ddlEvent.BlockedTableNames = []commonEvent.SchemaTableName{{SchemaName: rawEvent.SchemaName, TableName: rawEvent.TableName}}
	return ddlEvent, true, err
}
