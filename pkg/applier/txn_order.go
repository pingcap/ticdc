// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package applier

import (
	"context"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// verifyRedoTable never infers a source name from a lossy routing expression.
func (ra *RedoApplier) verifyRedoTable(source, target *common.TableName) error {
	if ra.txnOrderFilter == nil {
		return nil
	}
	if source != nil {
		return ra.verifyTxnOrderTable(source)
	}
	if target != nil && ra.hasTableRoute && !ra.txnOrderFilter.MatchTable(target.Schema, target.Table) {
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
			"routed redo log lacks source table identity; use txn-order-tables matching the recorded target names and omit routing rules")
	}
	return ra.verifyTxnOrderTable(target)
}

func (ra *RedoApplier) verifyTxnOrderDDL(ctx context.Context, ddl *commonEvent.RedoDDLEvent) error {
	if ra.txnOrderFilter == nil {
		return nil
	}
	if len(ddl.Tables) != 0 {
		for _, table := range ddl.Tables {
			if err := ra.verifyRedoTable(&table.Source, &table.Target); err != nil {
				return err
			}
		}
		return nil
	}
	if ddl.TableName.Schema != "" && ddl.TableName.Table != "" {
		if err := ra.verifyRedoTable(nil, &ddl.TableName); err != nil {
			return err
		}
	}
	switch model.ActionType(ddl.Type) {
	case model.ActionCreateTables, model.ActionRenameTables:
		return ra.verifyLegacyMultiTableDDL(ctx, ddl)
	case model.ActionRecoverSchema:
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
			"redo log lacks per-table partition metadata for recover schema with txn-order-tables")
	}
	return nil
}

// Legacy multi-table DDLs did not retain all table schemas. CREATE carries its
// partition clause; RENAME must check each old table before any rename executes.
func (ra *RedoApplier) verifyLegacyMultiTableDDL(ctx context.Context, ddl *commonEvent.RedoDDLEvent) error {
	stmts, _, err := parser.New().ParseSQL(ddl.DDL.Query)
	if err != nil {
		return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "cannot validate legacy redo DDL")
	}
	tableName := func(table *ast.TableName) common.TableName {
		schema := table.Schema.O
		if schema == "" {
			schema = ddl.TableName.Schema
		}
		return common.TableName{Schema: schema, Table: table.Name.O}
	}
	key := func(table common.TableName) commonEvent.SchemaTableName {
		return commonEvent.SchemaTableName{SchemaName: strings.ToLower(table.Schema), TableName: strings.ToLower(table.Table)}
	}
	createdPartitions := make(map[commonEvent.SchemaTableName]bool)
	for _, stmt := range stmts {
		switch stmt := stmt.(type) {
		case *ast.CreateTableStmt:
			target := tableName(stmt.Table)
			target.IsPartition = stmt.Partition != nil
			if stmt.ReferTable != nil {
				refer := tableName(stmt.ReferTable)
				partitioned, known := createdPartitions[key(refer)]
				if !known {
					partitioned, err = ra.mysqlSink.IsTablePartitioned(ctx, refer.Schema, refer.Table)
					if err != nil {
						return err
					}
				}
				target.IsPartition = partitioned
			}
			createdPartitions[key(target)] = target.IsPartition
			if err := ra.verifyRedoTable(nil, &target); err != nil {
				return err
			}
		case *ast.RenameTableStmt:
			// Track intermediate names without applying any rename downstream.
			// Only the final names determine which tables require ordering.
			origins := make(map[commonEvent.SchemaTableName]common.TableName)
			targets := make([]common.TableName, 0, len(stmt.TableToTables))
			for _, pair := range stmt.TableToTables {
				old := tableName(pair.OldTable)
				target := tableName(pair.NewTable)
				origin, known := origins[key(old)]
				if !known {
					origin = old
				}
				delete(origins, key(old))
				origins[key(target)] = origin
				targets = append(targets, target)
			}
			for _, target := range targets {
				old, final := origins[key(target)]
				if !final {
					continue
				}
				delete(origins, key(target))
				if err := ra.verifyRedoTable(nil, &target); err != nil {
					return err
				}
				if !ra.txnOrderFilter.MatchTable(target.Schema, target.Table) {
					continue
				}
				target.IsPartition, err = ra.mysqlSink.IsTablePartitioned(ctx, old.Schema, old.Table)
				if err != nil {
					return err
				}
				if err := ra.verifyTxnOrderTable(&target); err != nil {
					return err
				}
			}
		default:
			return errors.ErrInvalidReplicaConfig.GenWithStackByArgs(
				"redo log lacks per-table partition metadata for multi-table DDL with txn-order-tables")
		}
	}
	return nil
}
