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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

func loadPersistentStorageForTest(db *pebble.DB, gcTs uint64, upperBound UpperBoundMeta) *persistentStorage {
	p := &persistentStorage{
		pdCli:                  nil,
		kvStorage:              nil,
		db:                     db,
		gcTs:                   gcTs,
		upperBound:             upperBound,
		tableMap:               make(map[int64]*BasicTableInfo),
		partitionMap:           make(map[int64]BasicPartitionInfo),
		databaseMap:            make(map[int64]*BasicDatabaseInfo),
		tablesDDLHistory:       make(map[int64][]uint64),
		tableTriggerDDLHistory: make([]uint64, 0),
		tableInfoStoreMap:      make(map[int64]*versionedTableInfoStore),
		tableRegisteredCount:   make(map[int64]int),
	}
	p.initializeFromDisk()
	ctx := context.Background()
	p.ctx, p.cancel = context.WithCancel(ctx)
	return p
}

// create a persistent storage at dbPath with initailDBInfos
func newPersistentStorageForTest(dbPath string, initialDBInfos []mockDBInfo) *persistentStorage {
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("remove path fail", zap.Error(err))
	}
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail", zap.Error(err))
	}
	gcTs := uint64(0)
	if len(initialDBInfos) > 0 {
		mockWriteKVSnapOnDisk(db, gcTs, initialDBInfos)
	}
	upperBound := UpperBoundMeta{
		FinishedDDLTs: gcTs,
		ResolvedTs:    gcTs,
	}
	writeUpperBoundMeta(db, upperBound)
	return loadPersistentStorageForTest(db, gcTs, upperBound)
}

// load a persistent storage from dbPath
func loadPersistentStorageFromPathForTest(dbPath string, maxFinishedDDLTs uint64) *persistentStorage {
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail", zap.Error(err))
	}
	gcTs := uint64(0)
	upperBound := UpperBoundMeta{
		FinishedDDLTs: maxFinishedDDLTs,
		ResolvedTs:    maxFinishedDDLTs,
	}
	writeUpperBoundMeta(db, upperBound)
	return loadPersistentStorageForTest(db, gcTs, upperBound)
}

func formatDDLJobsForTest(jobs []*model.Job) string {
	var res []string
	for _, job := range jobs {
		res = append(res, fmt.Sprintf("type: %s, finishedTs: %d, schemaID: %d, tableID: %d", job.Type, job.BinlogInfo.FinishedTS, job.SchemaID, job.TableID))
	}
	return strings.Join(res, "; ")
}

func formatDDLEventsForTest(events []commonEvent.DDLEvent) string {
	var res []string
	for _, event := range events {
		var blockedTableIDs string
		if event.BlockedTables != nil {
			blockedTableIDs = fmt.Sprintf("type: %v, schemaID: %d, tableIDs: %v", event.BlockedTables.InfluenceType, event.BlockedTables.SchemaID, event.BlockedTables.TableIDs)
		}
		var needDroppedTableIDs string
		if event.NeedDroppedTables != nil {
			needDroppedTableIDs = fmt.Sprintf("type: %v, schemaID: %d, tableIDs: %v", event.NeedDroppedTables.InfluenceType, event.NeedDroppedTables.SchemaID, event.NeedDroppedTables.TableIDs)
		}
		res = append(res, fmt.Sprintf("type: %s, finishedTs: %d, query %s, blocked tables: %s, updated schemas %v, need dropped tables: %s, need added tables: %v, table name change %v",
			model.ActionType(event.Type),
			event.FinishedTs,
			event.Query,
			blockedTableIDs,
			event.UpdatedSchemas,
			needDroppedTableIDs,
			event.NeedAddedTables,
			event.TableNameChange))
	}
	return strings.Join(res, "; ")
}

type mockDBInfo struct {
	dbInfo *model.DBInfo
	tables []*model.TableInfo
}

func mockWriteKVSnapOnDisk(db *pebble.DB, snapTs uint64, dbInfos []mockDBInfo) {
	batch := db.NewBatch()
	defer batch.Close()
	for _, dbInfo := range dbInfos {
		addSchemaInfoToBatch(batch, snapTs, dbInfo.dbInfo)
		for _, tableInfo := range dbInfo.tables {
			tableInfoValue, err := json.Marshal(tableInfo)
			if err != nil {
				log.Panic("marshal table info fail", zap.Error(err))
			}
			addTableInfoToBatch(batch, snapTs, dbInfo.dbInfo, tableInfoValue)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Panic("commit batch fail", zap.Error(err))
	}
	writeGcTs(db, snapTs)
}

func buildTableFilterByNameForTest(schemaName, tableName string) filter.Filter {
	filterRule := fmt.Sprintf("%s.%s", schemaName, tableName)
	filterConfig := &config.FilterConfig{
		Rules: []string{filterRule},
	}
	tableFilter, err := filter.NewFilter(filterConfig, "", false, false)
	if err != nil {
		log.Panic("build filter failed", zap.Error(err))
	}
	return tableFilter
}

func newEligibleTableInfoForTest(tableID int64, tableName string) *model.TableInfo {
	// add a mock pk column
	tableInfo := &model.TableInfo{
		ID:   tableID,
		Name: ast.NewCIStr(tableName),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("a"),
				Offset:    0,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("b"),
				Offset:    1,
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
		},
		Indices: []*model.IndexInfo{
			{
				Name:    ast.NewCIStr("PRIMARY"),
				Primary: true,
				Unique:  true,
				Columns: []*model.IndexColumn{
					{Name: ast.NewCIStr("a"), Offset: 0},
				},
			},
		},
		PKIsHandle: true,
	}
	tableInfo.Columns[0].AddFlag(mysql.PriKeyFlag)
	// safety check, it's necessary
	if tableInfo.GetPkColInfo() == nil {
		log.Panic("table should have pk column")
	}
	tableInfoAfterWrap := common.WrapTableInfo("test", tableInfo)
	if len(tableInfoAfterWrap.GetColumns()) == 0 {
		log.Panic("table should have at least one column after wrap")
	}
	if !tableInfoAfterWrap.IsEligible(false) {
		log.Panic("table should be eligible after wrap")
	}
	tidbTableInfo := tableInfoAfterWrap.ToTiDBTableInfo()
	if !common.OriginalHasPKOrNotNullUK(tidbTableInfo) {
		log.Panic("table should be eligible after wrap and convert to TiDB TableInfo")
	}
	tidbTableInfoWrap := common.WrapTableInfo("test", tidbTableInfo)
	if len(tidbTableInfoWrap.GetColumns()) == 0 {
		log.Panic("table should have at least one column after convert to TiDB TableInfo")
	}
	if !tidbTableInfoWrap.IsEligible(false) {
		log.Panic("table should be eligible after wrap and convert to TiDB TableInfo and wrap again")
	}
	return tableInfo
}

func newEligiblePartitionTableInfoForTest(tableID int64, tableName string, partitions []model.PartitionDefinition) *model.TableInfo {
	tableInfo := newEligibleTableInfoForTest(tableID, tableName)
	tableInfo.Partition = &model.PartitionInfo{
		Definitions: partitions,
		Enable:      true,
	}
	return tableInfo
}

func buildCreateSchemaJobForTest(schemaID int64, schemaName string, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionCreateSchema,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: ast.NewCIStr(schemaName),
			},
			FinishedTS: finishedTs,
		},
	}
}

func buildDropSchemaJobForTest(schemaID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionDropSchema,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			DBInfo: &model.DBInfo{
				ID: schemaID,
			},
			FinishedTS: finishedTs,
		},
	}
}

func buildCreateTableJobForTest(schemaID, tableID int64, tableName string, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionCreateTable,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, tableName),
			FinishedTS: finishedTs,
		},
	}
}

func buildCreateTablesJobForTest(schemaID int64, tableIDs []int64, tableNames []string, finishedTs uint64) *model.Job {
	querys := make([]string, 0, len(tableIDs))
	for i := range tableIDs {
		querys = append(querys, fmt.Sprintf("create table %s(a int primary key);", tableNames[i]))
	}
	return buildCreateTablesJobWithQueryForTest(schemaID, tableIDs, tableNames, querys, finishedTs)
}

func buildCreateTablesJobWithQueryForTest(schemaID int64, tableIDs []int64, tableNames []string, querys []string, finishedTs uint64) *model.Job {
	multiTableInfos := make([]*model.TableInfo, 0, len(tableIDs))
	for i, id := range tableIDs {
		multiTableInfos = append(multiTableInfos, newEligibleTableInfoForTest(id, tableNames[i]))
	}
	return &model.Job{
		Type:     model.ActionCreateTables,
		SchemaID: schemaID,
		Query:    strings.Join(querys, ""),
		BinlogInfo: &model.HistoryInfo{
			MultipleTableInfos: multiTableInfos,
			FinishedTS:         finishedTs,
		},
	}
}

func buildCreatePartitionTablesJobForTest(schemaID int64, tableIDs []int64, tableNames []string, partitionIDLists [][]int64, finishedTs uint64) *model.Job {
	multiTableInfos := make([]*model.TableInfo, 0, len(tableIDs))
	querys := make([]string, 0, len(tableIDs))
	for i, id := range tableIDs {
		partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDLists[i]))
		for _, partitionID := range partitionIDLists[i] {
			partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
				ID: partitionID,
			})
		}
		multiTableInfos = append(multiTableInfos, newEligiblePartitionTableInfoForTest(id, tableNames[i], partitionDefinitions))
		querys = append(querys, fmt.Sprintf("create table %s(a int primary key);", tableNames[i]))
	}
	return &model.Job{
		Type:     model.ActionCreateTables,
		SchemaID: schemaID,
		Query:    strings.Join(querys, ""),
		BinlogInfo: &model.HistoryInfo{
			MultipleTableInfos: multiTableInfos,
			FinishedTS:         finishedTs,
		},
	}
}

func buildRenameTableJobForTest(schemaID, tableID int64, tableName string, finishedTs uint64, prevInfo *model.InvolvingSchemaInfo) *model.Job {
	job := &model.Job{
		Type:     model.ActionRenameTable,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, tableName),
			FinishedTS: finishedTs,
		},
	}
	if prevInfo != nil {
		job.InvolvingSchemaInfo = []model.InvolvingSchemaInfo{
			{
				Database: prevInfo.Database,
				Table:    prevInfo.Table,
			},
		}
	}
	return job
}

func buildRenameTablesJobForTest(
	oldSchemaIDs, newSchemaIDs, tableIDs []int64,
	oldSchemaNames, oldTableNames, newTableNames []string,
	finishedTs uint64,
) *model.Job {
	args := &model.RenameTablesArgs{
		RenameTableInfos: make([]*model.RenameTableArgs, 0, len(tableIDs)),
	}
	multiTableInfos := make([]*model.TableInfo, 0, len(tableIDs))
	for i := 0; i < len(tableIDs); i++ {
		args.RenameTableInfos = append(args.RenameTableInfos, &model.RenameTableArgs{
			OldSchemaID:   oldSchemaIDs[i],
			NewSchemaID:   newSchemaIDs[i],
			TableID:       tableIDs[i],
			NewTableName:  ast.NewCIStr(newTableNames[i]),
			OldSchemaName: ast.NewCIStr(oldSchemaNames[i]),
			OldTableName:  ast.NewCIStr(oldTableNames[i]),
		})
		multiTableInfos = append(multiTableInfos, newEligibleTableInfoForTest(tableIDs[i], newTableNames[i]))
	}
	job := &model.Job{
		Type: model.ActionRenameTables,
		BinlogInfo: &model.HistoryInfo{
			MultipleTableInfos: multiTableInfos,
			FinishedTS:         finishedTs,
		},
		Version: model.JobVersion2,
	}
	job.FillArgs(args)
	return job
}

func buildRenamePartitionTableJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionRenameTable, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

// most partition table related job have the same structure
func buildPartitionTableRelatedJobForTest(jobType model.ActionType, schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     jobType,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligiblePartitionTableInfoForTest(tableID, tableName, partitionDefinitions),
			FinishedTS: finishedTs,
		},
	}
}

func buildCreatePartitionTableJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionCreateTable, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

func buildDropTableJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionDropTable,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
		},
	}
}

// Note: `partitionIDs` must include all partition IDs of the original table.
func buildDropPartitionTableJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionDropTable, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

func buildTruncateTableJobForTest(schemaID, oldTableID, newTableID int64, tableName string, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionTruncateTable,
		SchemaID: schemaID,
		TableID:  oldTableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(newTableID, tableName),
			FinishedTS: finishedTs,
		},
	}
}

func buildTruncatePartitionTableJobForTest(schemaID, oldTableID, newTableID int64, tableName string, newPartitionIDs []int64, finishedTs uint64) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(newPartitionIDs))
	for _, partitionID := range newPartitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionTruncateTable,
		SchemaID: schemaID,
		TableID:  oldTableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligiblePartitionTableInfoForTest(newTableID, tableName, partitionDefinitions),
			FinishedTS: finishedTs,
		},
	}
}

// Note: `partitionIDs` must include all partition IDs of the table after add partition.
func buildAddPartitionJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionAddTablePartition, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

// Note: `partitionIDs` must include all partition IDs of the table after drop partition.
func buildDropPartitionJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionDropTablePartition, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

// Note: `partitionIDs` must include all partition IDs of the table after truncate partition.
func buildTruncatePartitionJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	return buildPartitionTableRelatedJobForTest(model.ActionTruncateTablePartition, schemaID, tableID, tableName, partitionIDs, finishedTs)
}

// Note: `partitionIDs` must include all partition IDs of the table after exchange partition.
func buildExchangePartitionJobForTest(
	normalSchemaID int64,
	normalTableID int64,
	partitionTableID int64,
	partitionTableName string,
	partitionIDs []int64,
	finishedTs uint64,
) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionExchangeTablePartition,
		SchemaID: normalSchemaID,
		TableID:  normalTableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligiblePartitionTableInfoForTest(partitionTableID, partitionTableName, partitionDefinitions),
			FinishedTS: finishedTs,
		},
	}
}

func buildAddPrimaryKeyJobForTest(schemaID, tableID int64, finishedTs uint64, indexes ...*model.IndexInfo) *model.Job {
	tableInfo := newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID))
	tableInfo.Indices = indexes
	return &model.Job{
		Type:     model.ActionAddPrimaryKey,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  tableInfo,
		},
	}
}

func buildAlterIndexVisibilityJobForTest(schemaID, tableID int64, finishedTs uint64, indexes ...*model.IndexInfo) *model.Job {
	tableInfo := newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID))
	tableInfo.Indices = indexes
	return &model.Job{
		Type:     model.ActionAlterIndexVisibility,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  tableInfo,
		},
	}
}

func buildAddFulltextIndexJobForTest(schemaID, tableID int64, finishedTs uint64, indexes ...*model.IndexInfo) *model.Job {
	tableInfo := newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID))
	tableInfo.Indices = indexes
	return &model.Job{
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  tableInfo,
		},
		Query: "ALTER TABLE t2 ADD FULLTEXT INDEX (b) WITH PARSER standard;",
	}
}

func buildCreateHybridIndexJobForTest(schemaID, tableID int64, finishedTs uint64, indexes ...*model.IndexInfo) *model.Job {
	tableInfo := newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID))
	tableInfo.Indices = indexes
	return &model.Job{
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  tableInfo,
		},
		Query: "CREATE HYBRID INDEX i_idx ON t(b, c, d, e, g) PARAMETER 'hybrid_index_param';",
	}
}

func buildDropPrimaryKeyJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionDropPrimaryKey,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
			FinishedTS: finishedTs,
		},
	}
}

func buildModifyTableCharsetJobForTest(schemaID, tableID int64, finishedTs uint64, charset string) *model.Job {
	tableInfo := newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID))
	tableInfo.Charset = charset
	return &model.Job{
		Type:     model.ActionModifyTableCharsetAndCollate,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  tableInfo,
		},
	}
}

func buildAlterTTLJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionAlterTTLInfo,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
			FinishedTS: finishedTs,
		},
	}
}

func buildRemoveTTLJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionAlterTTLRemove,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
			FinishedTS: finishedTs,
		},
	}
}

func buildMultiSchemaChangeJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionMultiSchemaChange,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
			FinishedTS: finishedTs,
		},
	}
}

func buildAddColumnJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionMultiSchemaChange,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
			FinishedTS: finishedTs,
		},
	}
}

func buildDropColumnJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionMultiSchemaChange,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
		},
	}
}

func buildCreateViewJobForTest(schemaID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionCreateView,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
		},
	}
}

func buildDropViewJobForTest(schemaID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionDropView,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
		},
	}
}

func buildAddForeignKeyJobForTest(schemaID, tableID int64, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionAddForeignKey,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			FinishedTS: finishedTs,
			TableInfo:  newEligibleTableInfoForTest(tableID, fmt.Sprintf("t_%d", tableID)),
		},
	}
}

// old table can be a normal table or a partition table
// `tableName` args is just to pass some safety check for the ddl handler
func buildAlterTablePartitioningJobForTest(
	schemaID, oldTableID, newTableID int64, newPartitions []int64,
	tableName string, finishedTs uint64,
) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(newPartitions))
	for _, partitionID := range newPartitions {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionAlterTablePartitioning,
		SchemaID: schemaID,
		TableID:  oldTableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligiblePartitionTableInfoForTest(newTableID, tableName, partitionDefinitions),
			FinishedTS: finishedTs,
		},
	}
}

// `tableName` args is just to pass some safety check for the ddl handler
func buildRemovePartitioningJobForTest(
	schemaID, oldTableID, newTableID int64,
	tableName string, finishedTs uint64,
) *model.Job {
	return &model.Job{
		Type:     model.ActionRemovePartitioning,
		SchemaID: schemaID,
		TableID:  oldTableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo:  newEligibleTableInfoForTest(newTableID, tableName),
			FinishedTS: finishedTs,
		},
	}
}
