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
}

var allDDLHandlers = map[model.ActionType]*persistStorageDDLHandler{
	model.ActionCreateSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
	},
	model.ActionDropSchema: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateDropSchema,
	},
	model.ActionCreateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
	},
	model.ActionDropTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionDropForeignKey: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionTruncateTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForTruncateTable,
	},
	model.ActionModifyColumn: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRebaseAutoID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForRenameTable,
	},
	model.ActionSetDefaultValue: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionShardRowID: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionModifyTableComment: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionRenameIndex: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalDDLOnSingleTable,
	},
	model.ActionAddTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
	},
	model.ActionDropTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
	},
	model.ActionCreateView: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventCommon,
	},
	model.ActionTruncateTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
	},

	model.ActionRecoverTable: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTable,
	},

	model.ActionExchangeTablePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForExchangePartition,
	},

	model.ActionCreateTables: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForCreateTables,
	},

	model.ActionReorganizePartition: {
		buildPersistedDDLEventFunc: buildPersistedDDLEventForNormalPartitionDDL,
	},
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

// ==== buildPersistedDDLEventFunc end ====
