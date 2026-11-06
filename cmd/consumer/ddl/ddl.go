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

// Package ddl contains the DDL identity and blocking rules shared by the
// scheduler and the writer.
package ddl

import (
	"slices"

	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// Key identifies one DDL independently of the connection that delivered it.
// Split DDLs can share a timestamp and table, so Query is part of the identity.
type Key struct {
	CommitTs uint64
	Schema   string
	Table    string
	Query    string
}

// KeyOf returns the replay identity of a DDL event.
func KeyOf(ddl *event.DDLEvent) Key {
	return Key{
		CommitTs: ddl.GetCommitTs(),
		Schema:   ddl.SchemaName,
		Table:    ddl.TableName,
		Query:    ddl.Query,
	}
}

// BlocksTable reports whether ddl must be written before dml.
func BlocksTable(ddl *event.DDLEvent, dml *event.DMLEvent) bool {
	if dml.TableInfo == nil {
		return true
	}
	return BlocksIdentity(ddl, dml.PhysicalTableID, dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName())
}

// BlocksIdentity reports whether ddl changes the given table identity.
func BlocksIdentity(ddl *event.DDLEvent, tableID int64, schema, table string) bool {
	// Incomplete multi-table metadata must retain a global barrier.
	if ddl.SchemaName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeAll) {
		return true
	}
	if (ddl.Type == byte(model.ActionRenameTables) || ddl.Type == byte(model.ActionExchangeTablePartition)) && len(ddl.BlockedTableNames) == 0 && len(ddl.MultipleTableInfos) == 0 {
		return true
	}
	if (schema == ddl.SchemaName && table == ddl.TableName) ||
		(schema == ddl.ExtraSchemaName && table == ddl.ExtraTableName) {
		return true
	}
	for _, name := range ddl.BlockedTableNames {
		if schema == name.SchemaName && table == name.TableName {
			return true
		}
	}
	for _, info := range ddl.MultipleTableInfos {
		if info != nil && schema == info.GetSchemaName() && table == info.GetTableName() {
			return true
		}
	}
	if ddl.BlockedTables != nil && slices.Contains(ddl.BlockedTables.TableIDs, tableID) {
		return true
	}
	return (ddl.TableName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeDB)) && schema == ddl.SchemaName
}
