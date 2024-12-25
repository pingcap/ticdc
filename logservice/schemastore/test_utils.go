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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

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

func buildCreatePartitionTableJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionCreateTable,
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
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionDropTable,
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
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionAddTablePartition,
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

// Note: `partitionIDs` must include all partition IDs of the table after drop partition.
func buildDropPartitionJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionDropTablePartition,
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

// Note: `partitionIDs` must include all partition IDs of the table after truncate partition.
func buildTruncatePartitionJobForTest(schemaID, tableID int64, tableName string, partitionIDs []int64, finishedTs uint64) *model.Job {
	partitionDefinitions := make([]model.PartitionDefinition, 0, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionDefinitions = append(partitionDefinitions, model.PartitionDefinition{
			ID: partitionID,
		})
	}
	return &model.Job{
		Type:     model.ActionTruncateTablePartition,
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
