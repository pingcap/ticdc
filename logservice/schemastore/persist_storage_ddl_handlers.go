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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type persistStorageDDLHandler struct {
	buildPersistedDDLEventFunc func(
		job *model.Job,
		databaseMap map[int64]*BasicDatabaseInfo,
		tableMap map[int64]*BasicTableInfo,
		partitionMap map[int64]BasicPartitionInfo,
	) PersistedDDLEvent
	updateDDLHistoryFunc func(
		ddlEvent *PersistedDDLEvent,
		databaseMap map[int64]*BasicDatabaseInfo,
		tableMap map[int64]*BasicTableInfo,
		partitionMap map[int64]BasicPartitionInfo,
		tablesDDLHistory map[int64][]uint64,
		tableTriggerDDLHistory []uint64,
	) []uint64
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

// ==== buildPersistedDDLEventFunc start ====
func buildPersistedDDLEventCommon(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	var query string
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

func buildPersistedDDLEventForCreateDropSchema(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	log.Info("buildPersistedDDLEvent for create/drop schema",
		zap.Any("type", event.Type),
		zap.Int64("schemaID", event.CurrentSchemaID),
		zap.String("schemaName", event.DBInfo.Name.O))
	event.CurrentSchemaName = event.DBInfo.Name.O
	return event
}

func buildPersistedDDLEventForCreateTable(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	databaseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = databaseInfo.Name
	event.CurrentTableName = event.TableInfo.Name.O
	return event
}

func buildPersistedDDLEventForNormalDDLOnSingleTable(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	databaseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = databaseInfo.Name
	tableInfo, ok := tableMap[event.CurrentTableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", event.CurrentTableID))
	}
	event.CurrentTableName = tableInfo.Name
	return event
}

func buildPersistedDDLEventForTruncateTable(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	// only table id change after truncate
	event.PrevTableID = event.CurrentTableID
	event.CurrentTableID = event.TableInfo.ID
	databaseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = databaseInfo.Name
	tableInfo, ok := tableMap[event.PrevTableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", event.PrevTableID))
	}
	event.CurrentTableName = tableInfo.Name
	if isPartitionTable(event.TableInfo) {
		for id := range partitionMap[event.PrevTableID] {
			event.PrevPartitions = append(event.PrevPartitions, id)
		}
	}
	return event
}

func buildPersistedDDLEventForRenameTable(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	// Note: schema id/schema name/table name may be changed or not
	// table id does not change, we use it to get the table's prev schema id/name and table name
	tableInfo, ok := tableMap[event.CurrentTableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", event.CurrentTableID))
	}
	event.PrevSchemaID = tableInfo.SchemaID
	event.PrevTableName = tableInfo.Name
	prevDatabaseInfo, ok := databaseMap[event.PrevSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.PrevSchemaID))
	}
	event.PrevSchemaName = prevDatabaseInfo.Name
	currentDatabaseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = currentDatabaseInfo.Name
	// get the table's current table name from the ddl job
	event.CurrentTableName = event.TableInfo.Name.O
	return event
}

func buildPersistedDDLEventForNormalPartitionDDL(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventForNormalDDLOnSingleTable(job, databaseMap, tableMap, partitionMap)
	for id := range partitionMap[event.CurrentTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

func buildPersistedDDLEventForExchangePartition(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	event.PrevSchemaID = event.CurrentSchemaID
	event.PrevTableID = event.CurrentTableID
	prevDatabaseInfo, ok := databaseMap[event.PrevSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.PrevSchemaID))
	}
	event.PrevSchemaName = prevDatabaseInfo.Name
	prevTableInfo, ok := tableMap[event.PrevTableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", event.PrevTableID))
	}
	event.PrevTableName = prevTableInfo.Name
	event.CurrentTableID = event.TableInfo.ID
	currentTableInfo, ok := tableMap[event.CurrentTableID]
	if !ok {
		log.Panic("table not found", zap.Int64("tableID", event.CurrentTableID))
	}
	// set CurrentSchemaID before CurrentSchemaName
	event.CurrentSchemaID = currentTableInfo.SchemaID
	event.CurrentTableName = currentTableInfo.Name
	currentDatabseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = currentDatabseInfo.Name
	for id := range partitionMap[event.CurrentTableID] {
		event.PrevPartitions = append(event.PrevPartitions, id)
	}
	return event
}

func buildPersistedDDLEventForCreateTables(
	job *model.Job,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
) PersistedDDLEvent {
	event := buildPersistedDDLEventCommon(job, databaseMap, tableMap, partitionMap)
	currentDatabseInfo, ok := databaseMap[event.CurrentSchemaID]
	if !ok {
		log.Panic("database not found", zap.Int64("schemaID", event.CurrentSchemaID))
	}
	event.CurrentSchemaName = currentDatabseInfo.Name
	event.MultipleTableInfos = job.BinlogInfo.MultipleTableInfos
	return event
}

// ==== updateDDLHistoryFunc begin ====
func updateDDLHistoryForTableTriggerOnlyDDL(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	return append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
}

func updateDDLHistoryForDropSchema(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for tableID := range databaseMap[ddlEvent.CurrentSchemaID].Tables {
		if partitionInfo, ok := partitionMap[tableID]; ok {
			for id := range partitionInfo {
				tablesDDLHistory[id] = append(tablesDDLHistory[id], ddlEvent.FinishedTs)
			}
		} else {
			tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
		}
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForAddDropTable(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	// Note: for create table, this ddl event will not be sent to table dispatchers.
	// add it to ddl history is just for building table info store.
	if isPartitionTable(ddlEvent.TableInfo) {
		// for partition table, we only care the ddl history of physical table ids.
		for _, partitionID := range getAllPartitionIDs(ddlEvent.TableInfo) {
			tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
		}
	} else {
		tablesDDLHistory[ddlEvent.CurrentTableID] = append(tablesDDLHistory[ddlEvent.CurrentTableID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForNormalDDLOnSingleTable(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	if isPartitionTable(ddlEvent.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(ddlEvent.TableInfo) {
			tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
		}
	} else {
		tablesDDLHistory[ddlEvent.CurrentTableID] = append(tablesDDLHistory[ddlEvent.CurrentTableID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForTruncateTable(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	if isPartitionTable(ddlEvent.TableInfo) {
		for _, partitionID := range getAllPartitionIDs(ddlEvent.TableInfo) {
			tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
		}
		for _, partitionID := range ddlEvent.PrevPartitions {
			tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
		}
	} else {
		tablesDDLHistory[ddlEvent.CurrentTableID] = append(tablesDDLHistory[ddlEvent.CurrentTableID], ddlEvent.FinishedTs)
		tablesDDLHistory[ddlEvent.PrevTableID] = append(tablesDDLHistory[ddlEvent.PrevTableID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForAddPartition(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for _, partitionID := range getAllPartitionIDs(ddlEvent.TableInfo) {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForDropPartition(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for _, partitionID := range ddlEvent.PrevPartitions {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForCreateView(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for tableID := range tableMap {
		tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForTruncatePartition(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for _, partitionID := range ddlEvent.PrevPartitions {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	newCreateIDs := getCreatedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
	for _, partitionID := range newCreateIDs {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForExchangeTablePartition(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	droppedIDs := getDroppedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
	if len(droppedIDs) != 1 {
		log.Panic("exchange table partition should only drop one partition",
			zap.Int64s("droppedIDs", droppedIDs))
	}
	tablesDDLHistory[ddlEvent.PrevTableID] = append(tablesDDLHistory[ddlEvent.PrevTableID], ddlEvent.FinishedTs)
	for _, tableID := range droppedIDs {
		tablesDDLHistory[tableID] = append(tablesDDLHistory[tableID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForCreateTables(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	// it won't be send to table dispatchers, just for build version store
	for _, info := range ddlEvent.MultipleTableInfos {
		if isPartitionTable(info) {
			for _, partitionID := range getAllPartitionIDs(info) {
				tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
			}
		} else {
			tablesDDLHistory[info.ID] = append(tablesDDLHistory[info.ID], ddlEvent.FinishedTs)
		}
	}
	return tableTriggerDDLHistory
}

func updateDDLHistoryForReorganizePartition(
	ddlEvent *PersistedDDLEvent,
	databaseMap map[int64]*BasicDatabaseInfo,
	tableMap map[int64]*BasicTableInfo,
	partitionMap map[int64]BasicPartitionInfo,
	tablesDDLHistory map[int64][]uint64,
	tableTriggerDDLHistory []uint64,
) []uint64 {
	tableTriggerDDLHistory = append(tableTriggerDDLHistory, ddlEvent.FinishedTs)
	for _, partitionID := range ddlEvent.PrevPartitions {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	newCreateIDs := getCreatedIDs(ddlEvent.PrevPartitions, getAllPartitionIDs(ddlEvent.TableInfo))
	for _, partitionID := range newCreateIDs {
		tablesDDLHistory[partitionID] = append(tablesDDLHistory[partitionID], ddlEvent.FinishedTs)
	}
	return tableTriggerDDLHistory
}
