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
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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
	return p
}

// create a persistent storage at dbPath with initailDBInfos
func newPersistentStorageForTest(dbPath string, initailDBInfos []mockDBInfo) *persistentStorage {
	if err := os.RemoveAll(dbPath); err != nil {
		log.Panic("remove path fail", zap.Error(err))
	}
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Panic("create database fail", zap.Error(err))
	}
	gcTs := uint64(0)
	if len(initailDBInfos) > 0 {
		mockWriteKVSnapOnDisk(db, gcTs, initailDBInfos)
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

type mockDBInfo struct {
	dbInfo *model.DBInfo
	tables []*model.TableInfo
}

func mockWriteKVSnapOnDisk(db *pebble.DB, snapTs uint64, dbInfos []mockDBInfo) {
	batch := db.NewBatch()
	defer batch.Close()
	for _, dbInfo := range dbInfos {
		writeSchemaInfoToBatch(batch, snapTs, dbInfo.dbInfo)
		for _, tableInfo := range dbInfo.tables {
			tableInfoValue, err := json.Marshal(tableInfo)
			if err != nil {
				log.Panic("marshal table info fail", zap.Error(err))
			}
			writeTableInfoToBatch(batch, snapTs, dbInfo.dbInfo, tableInfoValue)
		}
	}
	if err := batch.Commit(pebble.NoSync); err != nil {
		log.Panic("commit batch fail", zap.Error(err))
	}
	writeGcTs(db, snapTs)
}

func compareUnorderedTableSlices(slice1, slice2 []commonEvent.Table) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	sort.Slice(slice1, func(i, j int) bool {
		if slice1[i].SchemaID == slice1[j].SchemaID {
			return slice1[i].TableID < slice1[j].TableID
		}
		return slice1[i].SchemaID < slice1[j].SchemaID
	})

	sort.Slice(slice2, func(i, j int) bool {
		if slice2[i].SchemaID == slice2[j].SchemaID {
			return slice2[i].TableID < slice2[j].TableID
		}
		return slice2[i].SchemaID < slice2[j].SchemaID
	})

	for i := range slice1 {
		if slice1[i].SchemaID != slice2[i].SchemaID ||
			slice1[i].TableID != slice2[i].TableID ||
			!reflect.DeepEqual(slice1[i].SchemaTableName, slice2[i].SchemaTableName) {
			return false
		}
	}

	return true
}

func buildTableFilterByNameForTest(schemaName, tableName string) filter.Filter {
	filterRule := fmt.Sprintf("%s.%s", schemaName, tableName)
	log.Info("filterRule", zap.String("filterRule", filterRule))
	filterConfig := &config.FilterConfig{
		Rules: []string{filterRule},
	}
	tableFilter, err := filter.NewFilter(filterConfig, "", false)
	if err != nil {
		log.Panic("build filter failed", zap.Error(err))
	}
	return tableFilter
}

func buildCreateSchemaJobForTest(schemaID int64, schemaName string, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionCreateSchema,
		SchemaID: schemaID,
		BinlogInfo: &model.HistoryInfo{
			DBInfo: &model.DBInfo{
				ID:   schemaID,
				Name: pmodel.NewCIStr(schemaName),
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
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: pmodel.NewCIStr(tableName),
			},
			FinishedTS: finishedTs,
		},
	}
}

func buildCreateTablesJobForTest(schemaID int64, tableIDs []int64, tableNames []string, finishedTs uint64) *model.Job {
	multiTableInfos := make([]*model.TableInfo, 0, len(tableIDs))
	querys := make([]string, 0, len(tableIDs))
	for i, id := range tableIDs {
		multiTableInfos = append(multiTableInfos, &model.TableInfo{
			ID:   id,
			Name: pmodel.NewCIStr(tableNames[i]),
		})
		querys = append(querys, fmt.Sprintf("create table %s(a int primary key)", tableNames[i]))
	}
	return &model.Job{
		Type:     model.ActionCreateTables,
		SchemaID: schemaID,
		Query:    strings.Join(querys, ";"),
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
		multiTableInfos = append(multiTableInfos, &model.TableInfo{
			ID:   id,
			Name: pmodel.NewCIStr(tableNames[i]),
			Partition: &model.PartitionInfo{
				Definitions: partitionDefinitions,
			},
		})
		querys = append(querys, fmt.Sprintf("create table %s(a int primary key)", tableNames[i]))
	}
	return &model.Job{
		Type:     model.ActionCreateTables,
		SchemaID: schemaID,
		Query:    strings.Join(querys, ";"),
		BinlogInfo: &model.HistoryInfo{
			MultipleTableInfos: multiTableInfos,
			FinishedTS:         finishedTs,
		},
	}
}

func buildRenameTableJobForTest(schemaID, tableID int64, tableName string, finishedTs uint64) *model.Job {
	return &model.Job{
		Type:     model.ActionRenameTable,
		SchemaID: schemaID,
		TableID:  tableID,
		BinlogInfo: &model.HistoryInfo{
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: pmodel.NewCIStr(tableName),
			},
			FinishedTS: finishedTs,
		},
	}
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
			TableInfo: &model.TableInfo{
				ID:   tableID,
				Name: pmodel.NewCIStr(tableName),
				Partition: &model.PartitionInfo{
					Definitions: partitionDefinitions,
				},
			},
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
			TableInfo: &model.TableInfo{
				ID:   newTableID,
				Name: pmodel.NewCIStr(tableName),
			},
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
			TableInfo: &model.TableInfo{
				ID:   newTableID,
				Name: pmodel.NewCIStr(tableName),
				Partition: &model.PartitionInfo{
					Definitions: partitionDefinitions,
				},
			},
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
			TableInfo: &model.TableInfo{
				ID:   partitionTableID,
				Name: pmodel.NewCIStr(partitionTableName),
				Partition: &model.PartitionInfo{
					Definitions: partitionDefinitions,
				},
			},
			FinishedTS: finishedTs,
		},
	}
}
