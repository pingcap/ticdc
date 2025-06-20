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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// GetDDLActionType return DDL ActionType by the prefix
// see https://github.com/pingcap/tidb/blob/master/pkg/meta/model/job.go
func GetDDLActionType(query string) timodel.ActionType {
	query = strings.ToLower(query)
	// DDL related to the Database
	if strings.HasPrefix(query, "create schema") || strings.HasPrefix(query, "create database") {
		return timodel.ActionCreateSchema
	}
	if strings.HasPrefix(query, "drop schema") || strings.HasPrefix(query, "drop database") {
		return timodel.ActionDropSchema
	}
	// DDL related to the Table
	if strings.HasPrefix(query, "create table") {
		return timodel.ActionCreateTable
	}
	if strings.HasPrefix(query, "drop table") {
		return timodel.ActionDropTable
	}
	if strings.HasPrefix(query, "recover table") {
		return timodel.ActionRecoverTable
	}
	if strings.HasPrefix(query, "truncate") {
		return timodel.ActionTruncateTable
	}
	if strings.HasPrefix(query, "rename table") {
		if strings.Contains(query, ",") {
			return timodel.ActionRenameTables
		}
		return timodel.ActionRenameTable
	}

	if strings.Contains(query, "add partition") {
		return timodel.ActionAddTablePartition
	}
	if strings.Contains(query, "drop partition") {
		return timodel.ActionDropTablePartition
	}
	if strings.Contains(query, "truncate partition") {
		return timodel.ActionTruncateTablePartition
	}
	if strings.Contains(query, "reorganize partition") {
		return timodel.ActionReorganizePartition
	}

	// ALTER TABLE partitioned_table EXCHANGE PARTITION p1 WITH TABLE non_partitioned_table
	if strings.Contains(query, "exchange partition") {
		return timodel.ActionExchangeTablePartition
	}
	if strings.Contains(query, "partition by") {
		return timodel.ActionAlterTablePartitioning
	}
	if strings.Contains(query, "remove partitioning") {
		return timodel.ActionRemovePartitioning
	}

	if strings.Contains(query, "character set") {
		if strings.HasPrefix(query, "alter table") {
			return timodel.ActionModifyTableCharsetAndCollate
		}
		if strings.HasPrefix(query, "alter database") {
			return timodel.ActionModifySchemaCharsetAndCollate
		}
		log.Panic("how to set action for the DDL", zap.String("query", query))
	}

	if strings.Contains(query, "add primary key") {
		return timodel.ActionAddPrimaryKey
	}
	if strings.Contains(query, "add index") ||
		strings.Contains(query, "add key") ||
		strings.Contains(query, "add unique index") ||
		strings.Contains(query, "add unique key") ||
		strings.Contains(query, "add fulltext index") ||
		strings.Contains(query, "add fulltext key") ||
		strings.HasPrefix(query, "create index") {
		return timodel.ActionAddIndex
	}
	// todo: add unit test to verify this
	if strings.Contains(query, "drop primary key") {
		return timodel.ActionDropPrimaryKey
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
	if strings.Contains(query, "rename index") {
		return timodel.ActionRenameIndex
	}

	if strings.Contains(query, "set default") || strings.Contains(query, "drop default") {
		return timodel.ActionSetDefaultValue
	}

	// DDL related to column
	if strings.Contains(query, "add column") {
		return timodel.ActionAddColumn
	}
	if strings.Contains(query, "drop column") {
		return timodel.ActionDropColumn
	}
	if strings.Contains(query, "modify") {
		return timodel.ActionModifyColumn
	}
	if strings.Contains(query, "change") {
		return timodel.ActionModifyColumn
	}

	if strings.Contains(query, "auto_increment") {
		return timodel.ActionRebaseAutoID
	}

	if strings.Contains(query, "invisible") {
		return timodel.ActionAlterIndexVisibility
	}
	if strings.Contains(query, "create view") {
		return timodel.ActionCreateView
	}

	log.Panic("how to set action for the DDL ?", zap.String("query", query))
	return timodel.ActionNone
}

func GetBlockedTables(
	accessor blockedTableProvider,
	ddl *commonEvent.DDLEvent,
) *commonEvent.InfluencedTables {
	var (
		schemaName = ddl.SchemaName
		tableName  = ddl.TableName
		action     = timodel.ActionType(ddl.Type)
	)
	if action == timodel.ActionRenameTable {
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			log.Panic("parse statement failed", zap.Any("DDL", ddl), zap.Error(err))
		}
		schemaName = stmt.(*ast.RenameTableStmt).TableToTables[0].OldTable.Schema.O
		tableName = stmt.(*ast.RenameTableStmt).TableToTables[0].OldTable.Name.O

		ddl.ExtraSchemaName = schemaName
		ddl.ExtraTableName = tableName
	}
	blockedTableIDs := accessor.GetBlockedTables(schemaName, tableName)

	if action == timodel.ActionExchangeTablePartition {
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			log.Panic("parse statement failed", zap.Any("DDL", ddl), zap.Error(err))
		}
		sourceSchemaName := stmt.(*ast.AlterTableStmt).Table.Schema.O
		if sourceSchemaName == "" {
			sourceSchemaName = ddl.SchemaName
		}
		sourceTableName := stmt.(*ast.AlterTableStmt).Table.Name.O
		blockedTableIDs = accessor.GetBlockedTables(sourceSchemaName, sourceTableName)

		exchangedSchemaName := stmt.(*ast.AlterTableStmt).Specs[0].NewTable.Schema.O
		if exchangedSchemaName == "" {
			exchangedSchemaName = ddl.SchemaName
		}
		exchangedTableName := stmt.(*ast.AlterTableStmt).Specs[0].NewTable.Name.O
		exchangedTableID := accessor.GetBlockedTables(exchangedSchemaName, exchangedTableName)
		blockedTableIDs = append(blockedTableIDs, exchangedTableID...)
	}

	switch action {
	// create schema means the database not exist yet, so should not block tables.
	case timodel.ActionCreateSchema, timodel.ActionCreateTable:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
		}
	case timodel.ActionDropSchema, timodel.ActionModifySchemaCharsetAndCollate:
		// schemaID is not set now, can be set if only block the table belongs to the schema.
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeDB,
		}
	case timodel.ActionCreateView:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeAll,
		}
	case timodel.ActionTruncateTable, timodel.ActionRenameTable, timodel.ActionDropTable, timodel.ActionRecoverTable,
		timodel.ActionAddColumn, timodel.ActionDropColumn,
		timodel.ActionModifyColumn, timodel.ActionSetDefaultValue,
		timodel.ActionAddIndex, timodel.ActionDropIndex, timodel.ActionRenameIndex,
		timodel.ActionAddForeignKey, timodel.ActionDropForeignKey,
		timodel.ActionAddPrimaryKey, timodel.ActionDropPrimaryKey,
		timodel.ActionModifyTableCharsetAndCollate, timodel.ActionAlterIndexVisibility,
		timodel.ActionRebaseAutoID, timodel.ActionMultiSchemaChange, timodel.ActionDropView:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      blockedTableIDs,
		}
	case timodel.ActionAddTablePartition, timodel.ActionDropTablePartition,
		timodel.ActionTruncateTablePartition, timodel.ActionReorganizePartition,
		timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning,
		timodel.ActionExchangeTablePartition:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      blockedTableIDs,
		}
	default:
		log.Panic("unsupported DDL action", zap.String("DDL", ddl.Query), zap.String("action", action.String()))
	}
	return nil
}
