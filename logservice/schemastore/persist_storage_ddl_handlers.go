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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
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
	// iteratehEventTablesFunc call `apply` for all tables related to the ddl event
	iterateEventTablesFunc func(event *PersistedDDLEvent, apply func(tableIDs ...int64))
	// extractTableInfoFunc extract (table info, deleted) for the specified `tableID` from ddl event
	extractTableInfoFunc func(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool)
	// buildDDLEvent build a DDLEvent from a PersistedDDLEvent
	buildDDLEventFunc func(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent
}

var allDDLHandlers = map[model.ActionType]*persistStorageDDLHandler{
	model.ActionCreateSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateSchema,
	},
	model.ActionDropSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
		updateDDLHistoryFunc:       updateDDLHistoryForDropSchema,
		updateSchemaMetadataFunc:   updateSchemaMetadataForDropSchema,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForDropSchema,
	},
	model.ActionModifyTableCharsetAndCollate: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
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
	model.ActionAlterIndexVisibility: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
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
	model.ActionSetTiFlashReplica: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
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
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropView,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateDropView,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesIgnore,
		extractTableInfoFunc:       extractTableInfoFuncIgnore,
		buildDDLEventFunc:          buildDDLEventForCreateDropView,
	},
	model.ActionTruncateTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncatePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForTruncateTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForTruncatePartition,
		extractTableInfoFunc:       extractTableInfoFuncForTruncateAndReorganizePartition,
		buildDDLEventFunc:          buildDDLEventForTruncateAndReorganizePartition,
	},

	model.ActionRecoverTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataForNewTableDDL,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNewTableDDL,
	},

	model.ActionExchangeTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForExchangePartition,
		updateDDLHistoryFunc:       updateDDLHistoryForExchangeTablePartition,
		updateSchemaMetadataFunc:   updateSchemaMetadataForExchangeTablePartition,
		iterateEventTablesFunc:     iterateEventTablesForExchangeTablePartition,
		extractTableInfoFunc:       nil, // this func should not be called
		buildDDLEventFunc:          buildDDLEventForExchangeTablePartition,
	},

	model.ActionCreateTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTables,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateTables,
		updateSchemaMetadataFunc:   updateSchemaMetadataForCreateTables,
		iterateEventTablesFunc:     iterateEventTablesForCreateTables,
		extractTableInfoFunc:       extractTableInfoFuncForCreateTables,
		buildDDLEventFunc:          buildDDLEventForCreateTables,
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
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},
	model.ActionAlterTTLRemove: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTableForTiDB,
	},

	model.ActionMultiSchemaChange: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
		updateSchemaMetadataFunc:   updateSchemaMetadataIgnore,
		iterateEventTablesFunc:     iterateEventTablesForSingleTableDDL,
		extractTableInfoFunc:       extractTableInfoFuncForSingleTableDDL,
		buildDDLEventFunc:          buildDDLEventForNormalDDLOnSingleTable,
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

// transform ddl query based on sql mode.
func transformDDLJobQuery(job *model.Job) (string, error) {
	p := parser.New()
	// We need to use the correct SQL mode to parse the DDL query.
	// Otherwise, the parser may fail to parse the DDL query.
	// For example, it is needed to parse the following DDL query:
	//  `alter table "t" add column "c" int default 1;`
	// by adding `ANSI_QUOTES` to the SQL mode.
	p.SetSQLMode(job.SQLMode)
	stmts, _, err := p.Parse(job.Query, job.Charset, job.Collate)
	if err != nil {
		return "", errors.Trace(err)
	}
	var result string
	buildQuery := func(stmt ast.StmtNode) (string, error) {
		var sb strings.Builder
		// translate TiDB feature to special comment
		restoreFlags := format.RestoreTiDBSpecialComment
		// escape the keyword
		restoreFlags |= format.RestoreNameBackQuotes
		// upper case keyword
		restoreFlags |= format.RestoreKeyWordUppercase
		// wrap string with single quote
		restoreFlags |= format.RestoreStringSingleQuotes
		// remove placement rule
		restoreFlags |= format.SkipPlacementRuleForRestore
		// force disable ttl
		restoreFlags |= format.RestoreWithTTLEnableOff
		if err = stmt.Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
			return "", errors.Trace(err)
		}
		return sb.String(), nil
	}
	if len(stmts) > 1 {
		results := make([]string, 0, len(stmts))
		for _, stmt := range stmts {
			query, err := buildQuery(stmt)
			if err != nil {
				return "", errors.Trace(err)
			}
			results = append(results, query)
		}
		result = strings.Join(results, ";")
	} else {
		result, err = buildQuery(stmts[0])
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	log.Info("transform ddl query to result",
		zap.String("DDL", job.Query),
		zap.String("charset", job.Charset),
		zap.String("collate", job.Collate),
		zap.String("result", result))

	return result, nil
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

	event := PersistedDDLEvent{
		ID:              job.ID,
		Type:            byte(job.Type),
		CurrentSchemaID: job.SchemaID,
		CurrentTableID:  job.TableID,
		Query:           query,
		SchemaVersion:   job.BinlogInfo.SchemaVersion,
		DBInfo:          job.BinlogInfo.DBInfo,
		TableInfo:       job.BinlogInfo.TableInfo,
		FinishedTs:      job.BinlogInfo.FinishedTS,
		BDRRole:         job.BDRRole,
		CDCWriteSource:  job.CDCWriteSource,
	}
	return event
}

func buildPersistedDDLEventForCreateDropSchema(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	log.Info("buildPersistedDDLEvent for create/drop schema",
		zap.Any("type", event.Type),
		zap.Int64("schemaID", event.CurrentSchemaID),
		zap.String("schemaName", event.DBInfo.Name.O))
	event.CurrentSchemaName = event.DBInfo.Name.O
	return event
}

func buildPersistedDDLEventForCreateDropView(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	return event
}

func buildPersistedDDLEventForCreateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.CurrentTableName = event.TableInfo.Name.O
	return event
}

func buildPersistedDDLEventForDropTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.CurrentTableName = getTableName(args.tableMap, event.CurrentTableID)
	if event.Query != "" {
		event.Query = fmt.Sprintf("DROP TABLE `%s`.`%s`", event.CurrentSchemaName, event.CurrentTableName)
	}
	return event
}

func buildPersistedDDLEventForNormalDDLOnSingleTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.CurrentTableName = getTableName(args.tableMap, event.CurrentTableID)
	return event
}

func buildPersistedDDLEventForTruncateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	// only table id change after truncate
	event.PrevTableID = event.CurrentTableID
	event.CurrentTableID = event.TableInfo.ID
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.CurrentTableName = getTableName(args.tableMap, event.PrevTableID)
	if isPartitionTable(event.TableInfo) {
		for id := range args.partitionMap[event.PrevTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	}
	return event
}

func buildPersistedDDLEventForRenameTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	// Note: schema id/schema name/table name may be changed or not
	// table id does not change, we use it to get the table's prev schema id/name and table name
	event.PrevSchemaID = getSchemaID(args.tableMap, event.CurrentTableID)
	event.PrevTableName = getTableName(args.tableMap, event.CurrentTableID)
	event.PrevSchemaName = getSchemaName(args.databaseMap, event.PrevSchemaID)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	// get the table's current table name from the ddl job
	event.CurrentTableName = event.TableInfo.Name.O
	// Note: event.PrevSchemaID may not be accurate for rename table in some case.
	// after pr: https://github.com/pingcap/tidb/pull/43341,
	// assume there is a table `test.t` and a ddl: `rename table t to test2.t;`, and its commit ts is `100`.
	// if you get a ddl snapshot at ts `99`, table `t` is already in `test2`.
	// so event.PrevSchemaName will also be `test2`.
	// And because SchemaStore is the source of truth of cdc,
	// we can use event.PrevSchemaID(even it is wrong) to update the internal state of the cdc.
	// TODO: not sure whether kafka sink will use event.PrevSchemaName and event.PrevTableName
	// But event.Query will be emit to downstream(out of cdc), we must make it correct.
	if args.job.Query != "" {
		event.Query = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`",
			args.job.InvolvingSchemaInfo[0].Database, args.job.InvolvingSchemaInfo[0].Table,
			event.CurrentSchemaName, event.CurrentTableName)
	}
	return event
}

func buildPersistedDDLEventForNormalPartitionDDL(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventForNormalDDLOnSingleTable(args)
	for id := range args.partitionMap[event.CurrentTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

func buildPersistedDDLEventForExchangePartition(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.PrevSchemaID = event.CurrentSchemaID
	event.PrevTableID = event.CurrentTableID
	event.PrevSchemaName = getSchemaName(args.databaseMap, event.PrevSchemaID)
	event.PrevTableName = getTableName(args.tableMap, event.PrevTableID)
	event.CurrentTableID = event.TableInfo.ID
	event.CurrentSchemaID = getSchemaID(args.tableMap, event.TableInfo.ID)
	event.CurrentTableName = getTableName(args.tableMap, event.TableInfo.ID)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	for id := range args.partitionMap[event.CurrentTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

func buildPersistedDDLEventForCreateTables(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.MultipleTableInfos = args.job.BinlogInfo.MultipleTableInfos
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

func updateDDLHistoryForDropSchema(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	for tableID := range args.databaseMap[args.ddlEvent.CurrentSchemaID].Tables {
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
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.CurrentTableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForNormalDDLOnSingleTable(args updateDDLHistoryFuncArgs) []uint64 {
	if isPartitionTable(args.ddlEvent.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(args.ddlEvent.TableInfo) {
			args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, partitionID)
		}
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.CurrentTableID)
	}
	return args.tableTriggerDDLHistory
}

func updateDDLHistoryForTruncateTable(args updateDDLHistoryFuncArgs) []uint64 {
	args.appendTableTriggerDDLHistory(args.ddlEvent.FinishedTs)
	if isPartitionTable(args.ddlEvent.TableInfo) {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, getAllPartitionIDs(args.ddlEvent.TableInfo)...)
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.PrevPartitions...)
	} else {
		args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, args.ddlEvent.CurrentTableID, args.ddlEvent.PrevTableID)
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

func updateDDLHistoryForCreateDropView(args updateDDLHistoryFuncArgs) []uint64 {
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
	args.appendTablesDDLHistory(args.ddlEvent.FinishedTs, droppedIDs[0], args.ddlEvent.PrevTableID)
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

// =======
// updateDatabaseInfoAndTableInfoFunc begin
// =======
func updateSchemaMetadataForCreateSchema(args updateSchemaMetadataFuncArgs) {
	args.databaseMap[args.event.CurrentSchemaID] = &BasicDatabaseInfo{
		Name:   args.event.CurrentSchemaName,
		Tables: make(map[int64]bool),
	}
}

func updateSchemaMetadataForDropSchema(args updateSchemaMetadataFuncArgs) {
	schemaID := args.event.CurrentSchemaID
	for tableID := range args.databaseMap[schemaID].Tables {
		delete(args.tableMap, tableID)
		delete(args.partitionMap, tableID)
	}
	delete(args.databaseMap, schemaID)
}

func updateSchemaMetadataForNewTableDDL(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.CurrentTableID
	schemaID := args.event.CurrentSchemaID
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
	tableID := args.event.CurrentTableID
	schemaID := args.event.CurrentSchemaID
	args.removeTableFromDB(tableID, schemaID)
	delete(args.tableMap, tableID)
	if isPartitionTable(args.event.TableInfo) {
		delete(args.partitionMap, tableID)
	}
}

func updateSchemaMetadataIgnore(args updateSchemaMetadataFuncArgs) {}

func updateSchemaMetadataForTruncateTable(args updateSchemaMetadataFuncArgs) {
	oldTableID := args.event.PrevTableID
	newTableID := args.event.CurrentTableID
	schemaID := args.event.CurrentSchemaID
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
	tableID := args.event.CurrentTableID
	if args.event.PrevSchemaID != args.event.CurrentSchemaID {
		args.tableMap[tableID].SchemaID = args.event.CurrentSchemaID
		args.removeTableFromDB(tableID, args.event.PrevSchemaID)
		args.addTableToDB(tableID, args.event.CurrentSchemaID)
	}
	args.tableMap[tableID].Name = args.event.CurrentTableName
}

func updateSchemaMetadataForAddPartition(args updateSchemaMetadataFuncArgs) {
	newCreatedIDs := getCreatedIDs(args.event.PrevPartitions, getAllPartitionIDs(args.event.TableInfo))
	for _, id := range newCreatedIDs {
		args.partitionMap[args.event.CurrentTableID][id] = nil
	}
}

func updateSchemaMetadataForDropPartition(args updateSchemaMetadataFuncArgs) {
	droppedIDs := getDroppedIDs(args.event.PrevPartitions, getAllPartitionIDs(args.event.TableInfo))
	for _, id := range droppedIDs {
		delete(args.partitionMap[args.event.CurrentTableID], id)
	}
}

func updateSchemaMetadataForTruncateTablePartition(args updateSchemaMetadataFuncArgs) {
	tableID := args.event.CurrentTableID
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
	normalTableID := args.event.PrevTableID
	normalSchemaID := args.event.PrevSchemaID
	partitionID := droppedIDs[0]
	normalTableName := getTableName(args.tableMap, normalTableID)
	args.removeTableFromDB(normalTableID, normalSchemaID)
	delete(args.tableMap, normalTableID)
	args.addTableToDB(partitionID, normalSchemaID)
	args.tableMap[partitionID] = &BasicTableInfo{
		SchemaID: normalSchemaID,
		Name:     normalTableName,
	}
	partitionTableID := args.event.CurrentTableID
	delete(args.partitionMap[partitionTableID], partitionID)
	args.partitionMap[partitionTableID][normalTableID] = nil
}

func updateSchemaMetadataForCreateTables(args updateSchemaMetadataFuncArgs) {
	if args.event.MultipleTableInfos == nil {
		log.Panic("multiple table infos should not be nil")
	}
	for _, info := range args.event.MultipleTableInfos {
		args.addTableToDB(info.ID, args.event.CurrentSchemaID)
		args.tableMap[info.ID] = &BasicTableInfo{
			SchemaID: args.event.CurrentSchemaID,
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
	tableID := args.event.CurrentTableID
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

// =======
// iterateEventTablesFunc begin
// =======

func iterateEventTablesIgnore(event *PersistedDDLEvent, apply func(tableId ...int64)) {}

func iterateEventTablesForSingleTableDDL(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	if isPartitionTable(event.TableInfo) {
		apply(getAllPartitionIDs(event.TableInfo)...)
	} else {
		apply(event.CurrentTableID)
	}
}

func iterateEventTablesForTruncateTable(event *PersistedDDLEvent, apply func(tableId ...int64)) {
	if isPartitionTable(event.TableInfo) {
		apply(event.PrevPartitions...)
		apply(getAllPartitionIDs(event.TableInfo)...)
	} else {
		apply(event.PrevTableID, event.CurrentTableID)
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
	apply(targetPartitionID, event.PrevTableID)
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

// =======
// extractTableInfoFunc begin
// =======

func extractTableInfoFuncForSingleTableDDL(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
			}
		}
	} else {
		if tableID == event.CurrentTableID {
			return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
		}
	}
	log.Panic("should not reach here", zap.Any("event", event), zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncIgnore(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	return nil, false
}

func extractTableInfoFuncForDropTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if event.CurrentTableID == tableID {
		return nil, true
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

func extractTableInfoFuncForTruncateTable(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	if isPartitionTable(event.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(event.TableInfo) {
			if tableID == partitionID {
				return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
			}
		}
		return nil, true
	} else {
		if tableID == event.CurrentTableID {
			return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
		} else if tableID == event.PrevTableID {
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
			return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
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
			return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, event.TableInfo), false
		}
	}
	return nil, false
}

func extractTableInfoFuncForCreateTables(event *PersistedDDLEvent, tableID int64) (*common.TableInfo, bool) {
	for _, tableInfo := range event.MultipleTableInfos {
		if isPartitionTable(tableInfo) {
			for _, partitionID := range getAllPartitionIDs(tableInfo) {
				if tableID == partitionID {
					return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, tableInfo), false
				}
			}
		} else {
			if tableID == tableInfo.ID {
				return common.WrapTableInfo(event.CurrentSchemaID, event.CurrentSchemaName, tableInfo), false
			}
		}
	}
	log.Panic("should not reach here", zap.Int64("tableID", tableID))
	return nil, false
}

// =======
// buildDDLEvent begin
// =======

func buildDDLEventCommon(rawEvent *PersistedDDLEvent, _ filter.Filter, tiDBOnly bool) commonEvent.DDLEvent {
	var wrapTableInfo *common.TableInfo
	if rawEvent.TableInfo != nil {
		wrapTableInfo = common.WrapTableInfo(
			rawEvent.CurrentSchemaID,
			rawEvent.CurrentSchemaName,
			rawEvent.TableInfo)
	}

	return commonEvent.DDLEvent{
		Type: rawEvent.Type,
		// TODO: whether the following four fields are needed
		SchemaID:   rawEvent.CurrentSchemaID,
		TableID:    rawEvent.CurrentTableID,
		SchemaName: rawEvent.CurrentSchemaName,
		TableName:  rawEvent.CurrentTableName,

		Query:      rawEvent.Query,
		TableInfo:  wrapTableInfo,
		FinishedTs: rawEvent.FinishedTs,
		TiDBOnly:   tiDBOnly,
	}
}

func buildDDLEventForCreateSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	return ddlEvent
}

func buildDDLEventForDropSchema(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.CurrentSchemaID,
	}
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      rawEvent.CurrentSchemaID,
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		DropDatabaseName: rawEvent.CurrentSchemaName,
	}
	return ddlEvent
}

func buildDDLEventForNewTableDDL(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	if isPartitionTable(rawEvent.TableInfo) {
		physicalIDs := getAllPartitionIDs(rawEvent.TableInfo)
		ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(physicalIDs))
		for _, id := range physicalIDs {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  id,
			})
		}
	} else {
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  rawEvent.CurrentTableID,
			},
		}
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		AddName: []commonEvent.SchemaTableName{
			{
				SchemaName: rawEvent.CurrentSchemaName,
				TableName:  rawEvent.CurrentTableName,
			},
		},
	}
	return ddlEvent
}

func buildDDLEventForDropTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
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
			TableIDs:      []int64{rawEvent.CurrentTableID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.CurrentTableID},
		}
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		DropName: []commonEvent.SchemaTableName{
			{
				SchemaName: rawEvent.CurrentSchemaName,
				TableName:  rawEvent.CurrentTableName,
			},
		},
	}
	return ddlEvent
}

func buildDDLEventForNormalDDLOnSingleTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{rawEvent.CurrentTableID},
	}
	return ddlEvent
}

func buildDDLEventForNormalDDLOnSingleTableForTiDB(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{rawEvent.CurrentTableID},
	}
	return ddlEvent
}

func buildDDLEventForTruncateTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
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
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  id,
			})
		}
	} else {
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.PrevTableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  rawEvent.CurrentTableID,
			},
		}
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.PrevTableID, heartbeatpb.DDLSpan.TableID},
		}
	}
	return ddlEvent
}

func buildDDLEventForRenameTable(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.PrevSchemaName = rawEvent.PrevSchemaName
	ddlEvent.PrevTableName = rawEvent.PrevTableName
	ignorePrevTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.PrevSchemaName, rawEvent.PrevTableName)
	ignoreCurrentTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, rawEvent.CurrentTableName)
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
				if rawEvent.PrevSchemaID != rawEvent.CurrentSchemaID {
					ddlEvent.UpdatedSchemas = make([]commonEvent.SchemaIDChange, 0, len(allPhysicalIDs))
					for _, id := range allPhysicalIDs {
						ddlEvent.UpdatedSchemas = append(ddlEvent.UpdatedSchemas, commonEvent.SchemaIDChange{
							TableID:     id,
							OldSchemaID: rawEvent.PrevSchemaID,
							NewSchemaID: rawEvent.CurrentSchemaID,
						})
					}
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					AddName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.CurrentSchemaName,
							TableName:  rawEvent.CurrentTableName,
						},
					},
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.PrevSchemaName,
							TableName:  rawEvent.PrevTableName,
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
							SchemaName: rawEvent.PrevSchemaName,
							TableName:  rawEvent.PrevTableName,
						},
					},
				}
			}
		} else if !ignoreCurrentTable {
			// TODO: this should report an error as old cdc behaviour
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
			}
			// the table is filtered out before rename table, we need add table here
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  rawEvent.CurrentTableID,
				},
			}
			ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, len(allPhysicalIDs))
			for _, id := range allPhysicalIDs {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  id,
				})
			}
			ddlEvent.TableNameChange = &commonEvent.TableNameChange{
				AddName: []commonEvent.SchemaTableName{
					{
						SchemaName: rawEvent.CurrentSchemaName,
						TableName:  rawEvent.CurrentTableName,
					},
				},
			}
		} else {
			// if the table is both filtered out before and after rename table, the ddl should not be fetched
			log.Panic("should not build a ignored rename table ddl",
				zap.String("DDL", rawEvent.Query),
				zap.Int64("jobID", rawEvent.ID),
				zap.Int64("schemaID", rawEvent.CurrentSchemaID),
				zap.Int64("tableID", rawEvent.CurrentTableID))
		}
	} else {
		if !ignorePrevTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{rawEvent.CurrentTableID, heartbeatpb.DDLSpan.TableID},
			}
			if !ignoreCurrentTable {
				if rawEvent.PrevSchemaID != rawEvent.CurrentSchemaID {
					ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
						{
							TableID:     rawEvent.CurrentTableID,
							OldSchemaID: rawEvent.PrevSchemaID,
							NewSchemaID: rawEvent.CurrentSchemaID,
						},
					}
					ddlEvent.TableNameChange = &commonEvent.TableNameChange{
						AddName: []commonEvent.SchemaTableName{
							{
								SchemaName: rawEvent.CurrentSchemaName,
								TableName:  rawEvent.CurrentTableName,
							},
						},
						DropName: []commonEvent.SchemaTableName{
							{
								SchemaName: rawEvent.PrevSchemaName,
								TableName:  rawEvent.PrevTableName,
							},
						},
					}
				}
			} else {
				// the table is filtered out after rename table, we need drop the table
				ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
					InfluenceType: commonEvent.InfluenceTypeNormal,
					TableIDs:      []int64{rawEvent.CurrentTableID},
				}
				ddlEvent.TableNameChange = &commonEvent.TableNameChange{
					DropName: []commonEvent.SchemaTableName{
						{
							SchemaName: rawEvent.PrevSchemaName,
							TableName:  rawEvent.PrevTableName,
						},
					},
				}
			}
		} else if !ignoreCurrentTable {
			ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
			}
			// the table is filtered out before rename table, we need add table here
			ddlEvent.NeedAddedTables = []commonEvent.Table{
				{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  rawEvent.CurrentTableID,
				},
			}
			ddlEvent.TableNameChange = &commonEvent.TableNameChange{
				AddName: []commonEvent.SchemaTableName{
					{
						SchemaName: rawEvent.CurrentSchemaName,
						TableName:  rawEvent.CurrentTableName,
					},
				},
			}
		} else {
			// if the table is both filtered out before and after rename table, the ddl should not be fetched
			log.Panic("should not build a ignored rename table ddl",
				zap.String("DDL", rawEvent.Query),
				zap.Int64("jobID", rawEvent.ID),
				zap.Int64("schemaID", rawEvent.CurrentSchemaID),
				zap.Int64("tableID", rawEvent.CurrentTableID))
		}
	}
	return ddlEvent
}

func buildDDLEventForAddPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
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
			SchemaID: rawEvent.CurrentSchemaID,
			TableID:  id,
		})
	}
	return ddlEvent
}

func buildDDLEventForDropPartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
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
	return ddlEvent
}

func buildDDLEventForCreateDropView(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeAll,
	}
	return ddlEvent
}

func buildDDLEventForTruncateAndReorganizePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
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
			SchemaID: rawEvent.CurrentSchemaID,
			TableID:  id,
		})
	}
	droppedIDs := getDroppedIDs(rawEvent.PrevPartitions, physicalIDs)
	ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      droppedIDs,
	}
	return ddlEvent
}

func buildDDLEventForExchangeTablePartition(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ignoreNormalTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.PrevSchemaName, rawEvent.PrevTableName)
	ignorePartitionTable := tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, rawEvent.CurrentTableName)
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
			TableIDs:      []int64{rawEvent.PrevTableID, targetPartitionID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.UpdatedSchemas = []commonEvent.SchemaIDChange{
			{
				TableID:     targetPartitionID,
				OldSchemaID: rawEvent.CurrentSchemaID,
				NewSchemaID: rawEvent.PrevSchemaID,
			},
			{
				TableID:     rawEvent.PrevTableID,
				OldSchemaID: rawEvent.PrevSchemaID,
				NewSchemaID: rawEvent.CurrentSchemaID,
			},
		}
	} else if !ignoreNormalTable {
		ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.PrevTableID, heartbeatpb.DDLSpan.TableID},
		}
		ddlEvent.NeedDroppedTables = &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{rawEvent.PrevTableID},
		}
		ddlEvent.NeedAddedTables = []commonEvent.Table{
			{
				SchemaID: rawEvent.PrevSchemaID,
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
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  rawEvent.PrevTableID,
			},
		}
	} else {
		log.Fatal("should not happen")
	}
	return ddlEvent
}

func buildDDLEventForCreateTables(rawEvent *PersistedDDLEvent, tableFilter filter.Filter) commonEvent.DDLEvent {
	ddlEvent := buildDDLEventCommon(rawEvent, tableFilter, WithoutTiDBOnly)
	ddlEvent.BlockedTables = &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{heartbeatpb.DDLSpan.TableID},
	}
	physicalTableCount := 0
	logicalTableCount := 0
	for _, info := range rawEvent.MultipleTableInfos {
		if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, info.Name.O) {
			continue
		}
		logicalTableCount += 1
		if isPartitionTable(info) {
			physicalTableCount += len(info.Partition.Definitions)
		} else {
			physicalTableCount += 1
		}
	}
	querys := strings.Split(rawEvent.Query, ";")
	ddlEvent.NeedAddedTables = make([]commonEvent.Table, 0, physicalTableCount)
	addName := make([]commonEvent.SchemaTableName, 0, logicalTableCount)
	resultQuerys := make([]string, 0, logicalTableCount)
	for i, info := range rawEvent.MultipleTableInfos {
		if tableFilter != nil && tableFilter.ShouldIgnoreTable(rawEvent.CurrentSchemaName, info.Name.O) {
			continue
		}
		if isPartitionTable(info) {
			for _, partitionID := range getAllPartitionIDs(info) {
				ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
					SchemaID: rawEvent.CurrentSchemaID,
					TableID:  partitionID,
				})
			}
		} else {
			ddlEvent.NeedAddedTables = append(ddlEvent.NeedAddedTables, commonEvent.Table{
				SchemaID: rawEvent.CurrentSchemaID,
				TableID:  info.ID,
			})
		}
		addName = append(addName, commonEvent.SchemaTableName{
			SchemaName: rawEvent.CurrentSchemaName,
			TableName:  info.Name.O,
		})
		resultQuerys = append(resultQuerys, querys[i])
	}
	ddlEvent.TableNameChange = &commonEvent.TableNameChange{
		AddName: addName,
	}
	ddlEvent.Query = strings.Join(resultQuerys, ";")
	if len(ddlEvent.NeedAddedTables) == 0 {
		log.Fatal("should not happen")
	}
	return ddlEvent
}
