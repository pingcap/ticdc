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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"go.uber.org/zap"
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

type persistStorageDDLHandler struct {
	buildPersistedDDLEventFunc func(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent
	updateDDLHistoryFunc       func(args updateDDLHistoryFuncArgs) []uint64
}

var allDDLHandlers = map[model.ActionType]*persistStorageDDLHandler{
	model.ActionCreateSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
		updateDDLHistoryFunc:       updateDDLHistoryForTableTriggerOnlyDDL,
	},
	model.ActionDropSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
		updateDDLHistoryFunc:       updateDDLHistoryForDropSchema,
	},
	model.ActionCreateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
	},
	model.ActionDropTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
	},
	model.ActionAddColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionDropColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionAddIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionDropIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionAddForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionDropForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForTruncateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncateTable,
	},
	model.ActionModifyColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionRebaseAutoID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionRenameTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
	},
	model.ActionSetDefaultValue: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionShardRowID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionModifyTableComment: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionRenameIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
		updateDDLHistoryFunc:       updateDDLHistoryForNormalDDLOnSingleTable,
	},
	model.ActionAddTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForAddPartition,
	},
	model.ActionDropTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForDropPartition,
	},
	model.ActionCreateView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventCommon,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateView,
	},
	model.ActionTruncateTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForTruncatePartition,
	},

	model.ActionRecoverTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
		updateDDLHistoryFunc:       updateDDLHistoryForAddDropTable,
	},

	model.ActionExchangeTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForExchangePartition,
		updateDDLHistoryFunc:       updateDDLHistoryForExchangeTablePartition,
	},

	model.ActionCreateTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTables,
		updateDDLHistoryFunc:       updateDDLHistoryForCreateTables,
	},

	model.ActionReorganizePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
		updateDDLHistoryFunc:       updateDDLHistoryForReorganizePartition,
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

// ==== buildPersistedDDLEventFunc start ====
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

func buildPersistedDDLEventForCreateTable(args buildPersistedDDLEventFuncArgs) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(args)
	event.CurrentSchemaName = getSchemaName(args.databaseMap, event.CurrentSchemaID)
	event.CurrentTableName = event.TableInfo.Name.O
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

// ==== updateDDLHistoryFunc begin ====
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
