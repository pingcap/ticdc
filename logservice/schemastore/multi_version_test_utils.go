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
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

func buildCreateTableEventForTest(schemaID, tableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:              byte(model.ActionCreateTable),
		CurrentSchemaID:   schemaID,
		CurrentTableID:    tableID,
		CurrentSchemaName: schemaName,
		CurrentTableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: pmodel.NewCIStr(tableName),
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
		Type:              byte(model.ActionCreateTable),
		CurrentSchemaID:   schemaID,
		CurrentTableID:    tableID,
		CurrentSchemaName: schemaName,
		CurrentTableName:  tableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: pmodel.NewCIStr(tableName),
			Partition: &model.PartitionInfo{
				Definitions: partitionDefinitions,
				Enable:      true,
			},
		},
		FinishedTs: finishedTs,
	}
}

func buildTruncateTableEventForTest(schemaID, oldTableID, newTableID int64, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:              byte(model.ActionTruncateTable),
		CurrentSchemaID:   schemaID,
		CurrentTableID:    newTableID,
		CurrentSchemaName: schemaName,
		CurrentTableName:  tableName,
		PrevTableID:       oldTableID,
		TableInfo: &model.TableInfo{
			ID:   newTableID,
			Name: pmodel.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildRenameTableEventForTest(prevSchemaID, schemaID, tableID int64, prevSchemaName, prevTableName, schemaName, tableName string, finishedTs uint64) *PersistedDDLEvent {
	return &PersistedDDLEvent{
		Type:              byte(model.ActionRenameTable),
		CurrentSchemaID:   schemaID,
		CurrentTableID:    tableID,
		CurrentSchemaName: schemaName,
		CurrentTableName:  tableName,
		PrevSchemaID:      prevSchemaID,
		PrevSchemaName:    prevSchemaName,
		PrevTableName:     prevTableName,
		TableInfo: &model.TableInfo{
			ID:   tableID,
			Name: pmodel.NewCIStr(tableName),
		},
		FinishedTs: finishedTs,
	}
}

func buildExchangePartitionTableEventForTest(
	normalSchemaID, normalTableID, partitionSchemaID, partitionTableID int64,
	normalSchemaName, normalTableName, partitionSchemaName, partitionTableName string,
	oldPartitionIDs, newPartitionIDs []int64, finishedTs uint64,
) *PersistedDDLEvent {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(newPartitionIDs))
	for _, partitionID := range newPartitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &PersistedDDLEvent{
		Type:              byte(model.ActionExchangeTablePartition),
		CurrentSchemaID:   partitionSchemaID,
		CurrentTableID:    partitionTableID,
		CurrentSchemaName: partitionSchemaName,
		CurrentTableName:  partitionTableName,
		PrevSchemaID:      normalSchemaID,
		PrevTableID:       normalTableID,
		PrevSchemaName:    normalSchemaName,
		PrevTableName:     normalTableName,
		TableInfo: &model.TableInfo{
			ID:   partitionTableID,
			Name: pmodel.NewCIStr(partitionTableName),
			Partition: &model.PartitionInfo{
				Definitions: partitionDefinitions,
				Enable:      true,
			},
		},
		PreTableInfo: common.WrapTableInfo(normalSchemaID, normalSchemaName, &model.TableInfo{
			ID:   normalTableID,
			Name: pmodel.NewCIStr(normalTableName),
		}),
		PrevPartitions: oldPartitionIDs,
		FinishedTs:     finishedTs,
	}
}
