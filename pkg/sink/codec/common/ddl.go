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
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/zap"
)

// GetDDLActionType return DDL ActionType by the DDL query
// see https://github.com/pingcap/tidb/blob/master/pkg/meta/model/job.go,
// https://github.com/pingcap/tidb/blob/9accc3cfa3de44130537fcf9767623c436869aa6/pkg/ddl/executor.go#L1710
// and https://github.com/pingcap/tidb/blob/9accc3cfa3de44130537fcf9767623c436869aa6/pkg/executor/ddl.go#L96
func GetDDLActionType(query string) timodel.ActionType {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		log.Panic("parse ddl query failed", zap.String("query", query), zap.Error(err))
		return timodel.ActionNone
	}
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		return timodel.ActionCreateTable
	case *ast.DropTableStmt:
		if s.IsView {
			return timodel.ActionDropView
		} else {
			return timodel.ActionDropTable
		}
	case *ast.AlterDatabaseStmt:
		return timodel.ActionModifySchemaCharsetAndCollate
	case *ast.AlterTableStmt:
		if len(s.Specs) > 1 {
			return timodel.ActionMultiSchemaChange
		} else if len(s.Specs) == 0 {
			// The TTL_ENABLE attribute in the downstream will be automatically set to OFF
			// `ALTER TABLE TTL_ENABLE='ON'` will transform into `ALTER TABLE`
			// and the table specs are empty.
			return timodel.ActionAlterTTLInfo
		}
		spec := s.Specs[0]
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			return timodel.ActionAddColumn
		case ast.AlterTableAttributes:
			return timodel.ActionAlterTableAttributes
		case ast.AlterTableOption:
			if len(spec.Options) > 1 {
				return timodel.ActionMultiSchemaChange
			}
			option := spec.Options[0]
			switch option.Tp {
			case ast.TableOptionAutoIncrement, ast.TableOptionAutoRandomBase:
				return timodel.ActionRebaseAutoID
			case ast.TableOptionComment:
				return timodel.ActionModifyTableComment
			case ast.TableOptionCharset, ast.TableOptionCollate:
				return timodel.ActionModifyTableCharsetAndCollate
			case ast.TableOptionTTL, ast.TableOptionTTLEnable, ast.TableOptionTTLJobInterval:
				return timodel.ActionAlterTTLInfo
			}
		case ast.AlterTableAddPartitions, ast.AlterTableAddLastPartition:
			return timodel.ActionAddTablePartition
		case ast.AlterTableDropPartition, ast.AlterTableDropFirstPartition:
			return timodel.ActionDropTablePartition
		case ast.AlterTableSetTiFlashReplica:
			return timodel.ActionSetTiFlashReplica
		case ast.AlterTableAddConstraint:
			switch spec.Constraint.Tp {
			case ast.ConstraintKey, ast.ConstraintIndex, ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
				return timodel.ActionAddIndex
			case ast.ConstraintForeignKey:
				return timodel.ActionAddForeignKey
			case ast.ConstraintPrimaryKey:
				return timodel.ActionAddPrimaryKey
			}
			return timodel.ActionAddCheckConstraint
		case ast.AlterTableDropColumn:
			return timodel.ActionDropColumn
		case ast.AlterTableDropPrimaryKey:
			return timodel.ActionDropPrimaryKey
		case ast.AlterTableDropIndex:
			return timodel.ActionDropIndex
		case ast.AlterTableDropForeignKey:
			return timodel.ActionDropForeignKey
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			return timodel.ActionModifyColumn
		case ast.AlterTableAlterColumn:
			return timodel.ActionSetDefaultValue
		case ast.AlterTableRenameTable:
			return timodel.ActionRenameTable
		case ast.AlterTableRenameIndex:
			return timodel.ActionRenameIndex
		case ast.AlterTableTruncatePartition:
			return timodel.ActionTruncateTablePartition
		case ast.AlterTablePartition:
			return timodel.ActionAlterTablePartitioning
		case ast.AlterTableRemovePartitioning:
			return timodel.ActionRemovePartitioning
		case ast.AlterTableReorganizePartition, ast.AlterTableReorganizeLastPartition, ast.AlterTableReorganizeFirstPartition:
			return timodel.ActionReorganizePartition
		case ast.AlterTableExchangePartition:
			return timodel.ActionExchangeTablePartition
		case ast.AlterTableIndexInvisible:
			return timodel.ActionAlterIndexVisibility
		case ast.AlterTableRemoveTTL:
			return timodel.ActionAlterTTLRemove
		default:
			log.Panic("AlterTableType is not supported", zap.Any("tp", spec.Tp))
		}
	case *ast.CreateIndexStmt:
		return timodel.ActionAddIndex
	case *ast.CreateDatabaseStmt:
		return timodel.ActionCreateSchema
	case *ast.CreateViewStmt:
		return timodel.ActionCreateView
	case *ast.DropIndexStmt:
		return timodel.ActionDropIndex
	case *ast.DropDatabaseStmt:
		return timodel.ActionDropSchema
	case *ast.FlashBackTableStmt, *ast.RecoverTableStmt:
		return timodel.ActionRecoverTable
	case *ast.RenameTableStmt:
		if len(s.TableToTables) > 1 {
			return timodel.ActionRenameTables
		}
		return timodel.ActionRenameTable
	case *ast.TruncateTableStmt:
		if len(s.Table.PartitionNames) > 0 {
			return timodel.ActionTruncateTablePartition
		}
		return timodel.ActionTruncateTable
	}

	log.Panic("unsupport ddl type", zap.String("query", query))
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
	blockedTableIDs := accessor.GetBlockedTables(schemaName, tableName)
	if action == timodel.ActionRenameTable {
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			log.Panic("parse statement failed", zap.Any("DDL", ddl), zap.Error(err))
		}
		// The query in job maybe "RENAME TABLE table1 to table2"
		oldSchemaName := stmt.(*ast.RenameTableStmt).TableToTables[0].OldTable.Schema.O
		if oldSchemaName != "" {
			schemaName = oldSchemaName
		}
		tableName = stmt.(*ast.RenameTableStmt).TableToTables[0].OldTable.Name.O

		ddl.ExtraSchemaName = schemaName
		ddl.ExtraTableName = tableName
		extraTableIDs := accessor.GetBlockedTables(schemaName, tableName)
		blockedTableIDs = append(blockedTableIDs, extraTableIDs...)
	}

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
		timodel.ActionExchangeTablePartition, timodel.ActionAlterTTLInfo, timodel.ActionAlterTTLRemove:
		return &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      blockedTableIDs,
		}
	default:
		log.Panic("unsupported DDL action", zap.String("DDL", ddl.Query), zap.String("action", action.String()))
	}
	return nil
}
