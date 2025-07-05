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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func buildCreateTableEventForTest(schemaID, tableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: ast.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildDropTableEventForTest(schemaID, tableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:       byte(model.ActionDropTable),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		FinishedTs: finishedTs,
	}
}

func buildRecoverTableEventForTest(schemaID, tableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:       byte(model.ActionRecoverTable),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: ast.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildCreatePartitionTableEventForTest(schemaID, tableID int64, schemaName, tableName string, partitionIDs []int64, finishedTs uint64) *PersistedDDLEvent {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &PersistedDDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:        tableID,
			Name:      ast.NewCIStr(tableName),
			Partition: buildPartitionDefinitionsForTest(partitionIDs),
		},
		FinishedTs: finishedTs,
	}
}

func buildDropPartitionTableEventForTest(schemaID, tableID int64, schemaName, tableName string, partitionIDs []int64, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:       byte(model.ActionDropTable),
		SchemaID:   schemaID,
		TableID:    tableID,
		SchemaName: schemaName,
		TableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:        tableID,
			Name:      ast.NewCIStr(tableName),
			Partition: buildPartitionDefinitionsForTest(partitionIDs),
		},
		FinishedTs: finishedTs,
	}
}

func buildTruncateTableEventForTest(schemaID, oldTableID, newTableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:         byte(model.ActionTruncateTable),
		SchemaID:     schemaID,
		TableID:      oldTableID,
		SchemaName:   schemaName,
		TableName:    tableName,
		ExtraTableID: newTableID,
		TableInfo: &model.TableInfo{
			ID:   newTableID,
			Name: ast.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildTruncatePartitionTableEventForTest(
	schemaID, oldTableID int64, newTableID int64,
	schemaName, tableName string,
	newPartitionIDs []int64, finishedTs uint64,
) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:       byte(model.ActionTruncateTable),
		SchemaID:   schemaID,
		TableID:    oldTableID,
		SchemaName: schemaName,
		TableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:        newTableID,
			Name:      ast.NewCIStr(tableName),
			Partition: buildPartitionDefinitionsForTest(newPartitionIDs),
		},
		FinishedTs: finishedTs,
	}
}

func buildRenameTableEventForTest(extraSchemaID, schemaID, tableID int64, extraSchemaName, extraTableName, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:            byte(model.ActionRenameTable),
		SchemaID:        schemaID,
		TableID:         tableID,
		SchemaName:      schemaName,
		TableName:       tableName,
		ExtraSchemaID:   extraSchemaID,
		ExtraSchemaName: extraSchemaName,
		ExtraTableName:  extraTableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: ast.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildExchangePartitionTableEventForTest(
	normalSchemaID, normalTableID, partitionSchemaID, partitionTableID int64,
	normalSchemaName, normalTableName, partitionSchemaName, partitionTableName string,
	oldPartitionIDs, newPartitionIDs []int64, finishedTs uint64,
) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:            byte(model.ActionExchangeTablePartition),
		SchemaID:        normalSchemaID,
		TableID:         normalTableID,
		SchemaName:      normalSchemaName,
		TableName:       normalTableName,
		ExtraSchemaID:   partitionSchemaID,
		ExtraTableID:    partitionTableID,
		ExtraSchemaName: partitionSchemaName,
		ExtraTableName:  partitionTableName,
		TableInfo: &model.TableInfo{
			ID:        partitionTableID,
			Name:      ast.NewCIStr(partitionTableName),
			Partition: buildPartitionDefinitionsForTest(newPartitionIDs),
		},
		ExtraTableInfo: common.WrapTableInfo(normalSchemaName, &model.TableInfo{
			ID:   normalTableID,
			Name: ast.NewCIStr(normalTableName),
		}),
		PrevPartitions: oldPartitionIDs,
		FinishedTs:     finishedTs,
	}
}

func buildPartitionDefinitionsForTest(partitionIDs []int64) *model.PartitionInfo {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.PartitionInfo{
		Definitions: partitionDefinitions,
		Enable:      true,
	}
}
