// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"strings"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

// GetDDLActionType return DDL ActionType by the prefix
// see https://github.com/pingcap/tidb/blob/master/pkg/meta/model/job.go
func GetDDLActionType(query string) timodel.ActionType {
	query = strings.ToLower(query)
	if strings.HasPrefix(query, "create schema") || strings.HasPrefix(query, "create database") {
		return timodel.ActionCreateSchema
	}
	if strings.HasPrefix(query, "drop schema") || strings.HasPrefix(query, "drop database") {
		return timodel.ActionDropSchema
	}
	if strings.HasPrefix(query, "create table") {
		return timodel.ActionCreateTable
	}
	if strings.HasPrefix(query, "drop table") {
		return timodel.ActionDropTable
	}
	if strings.HasPrefix(query, "add column") {
		return timodel.ActionAddColumn
	}
	if strings.HasPrefix(query, "drop column") {
		return timodel.ActionDropColumn
	}
	if strings.Contains(query, "add index") {
		return timodel.ActionAddIndex
	}
	if strings.Contains(query, "drop index") {
		return timodel.ActionDropIndex
	}
	if strings.Contains(query, "add foreign key") {
		return timodel.ActionAddForeignKey
	}
	if strings.Contains(query, "drop foreign key") {
		return timodel.ActionDropForeignKey
	}
	if strings.HasPrefix(query, "truncate table") {
		return timodel.ActionTruncateTable
	}
	if strings.Contains(query, "modify column") {
		return timodel.ActionModifyColumn
	}
	if strings.Contains(query, "rebase autoid") {
		return timodel.ActionRebaseAutoID
	}
	if strings.Contains(query, "rename table") {
		return timodel.ActionRenameTable
	}
	if strings.Contains(query, "set default value") {
		return timodel.ActionSetDefaultValue
	}
	if strings.Contains(query, "rename index") {
		return timodel.ActionRenameIndex
	}
	if strings.Contains(query, "add table partition") {
		return timodel.ActionAddTablePartition
	}
	if strings.Contains(query, "drop table partition") {
		return timodel.ActionDropTablePartition
	}
	if strings.Contains(query, "truncate table partition") {
		return timodel.ActionTruncateTablePartition
	}
	if strings.Contains(query, "reorganize partition") {
		return timodel.ActionReorganizePartition
	}
	if strings.Contains(query, "exchange partition") {
		return timodel.ActionExchangeTablePartition
	}
	if strings.Contains(query, "alter table partitioning") {
		return timodel.ActionAlterTablePartitioning
	}
	if strings.Contains(query, "remove table partition") {
		return timodel.ActionRemovePartitioning
	}
	if strings.Contains(query, "modify table charset") {
		return timodel.ActionModifyTableCharsetAndCollate
	}
	if strings.Contains(query, "modify schema charset") {
		return timodel.ActionModifySchemaCharsetAndCollate
	}
	if strings.Contains(query, "add primary key") {
		return timodel.ActionAddPrimaryKey
	}
	if strings.Contains(query, "drop primary key") {
		return timodel.ActionDropPrimaryKey
	}
	return timodel.ActionNone
}

func GetInfluenceTables(action timodel.ActionType, schemaID int64, tableID int64) *commonEvent.InfluencedTables {
	switch action {
	case timodel.ActionCreateSchema:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		}
	case timodel.ActionDropSchema:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
			SchemaID:      schemaID,
		}
	case timodel.ActionCreateTable:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		}
	case timodel.ActionDropTable:
		// todo: how to handle the partition tables, all partitions should be blocked
		// only consider normal table now.
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{tableID},
		}
	case timodel.ActionAddColumn, timodel.ActionDropColumn,
		timodel.ActionAddIndex, timodel.ActionDropIndex,
		timodel.ActionAddForeignKey, timodel.ActionDropForeignKey,
		timodel.ActionModifyColumn, timodel.ActionRebaseAutoID,
		timodel.ActionSetDefaultValue, timodel.ActionRenameIndex,
		timodel.ActionAddPrimaryKey, timodel.ActionDropPrimaryKey,
		timodel.ActionModifyTableCharsetAndCollate:
		// todo: how to handle the partition tables, all partitions should be blocked
		// only consider normal table now.
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{tableID},
		}
	case timodel.ActionTruncateTable:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionRenameTable:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionAddTablePartition:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionDropTablePartition:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionTruncateTablePartition:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionReorganizePartition:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionExchangeTablePartition:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionAlterTablePartitioning:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionRemovePartitioning:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	case timodel.ActionModifySchemaCharsetAndCollate:
		log.Panic("unsupported DDL action, influence tables not set", zap.String("action", action.String()))
	default:
		log.Panic("unsupported DDL action", zap.String("action", action.String()))
	}
	return nil
}
