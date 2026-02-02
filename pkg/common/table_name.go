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
	"fmt"
)

//go:generate msgp

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema string `toml:"db-name" msg:"db-name"`
	Table  string `toml:"tbl-name" msg:"tbl-name"`
	// TableID is the logic table ID
	TableID     int64 `toml:"tbl-id" msg:"tbl-id"`
	IsPartition bool  `toml:"is-partition" msg:"is-partition"`
	// TargetSchema is the target schema name for routing.
	// If empty, Schema is used as the target.
	TargetSchema string `toml:"target-db-name" msg:"target-db-name"`
	// TargetTable is the target table name for routing.
	// If empty, Table is used as the target.
	TargetTable string `toml:"target-tbl-name" msg:"target-tbl-name"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// QuoteString returns quoted full table name
func (t TableName) QuoteString() string {
	return QuoteSchema(t.Schema, t.Table)
}

// GetTargetSchema returns the target schema name for routing.
// If TargetSchema is empty, returns Schema.
func (t *TableName) GetTargetSchema() string {
	if t.TargetSchema != "" {
		return t.TargetSchema
	}
	return t.Schema
}

// GetTargetTable returns the target table name for routing.
// If TargetTable is empty, returns Table.
func (t *TableName) GetTargetTable() string {
	if t.TargetTable != "" {
		return t.TargetTable
	}
	return t.Table
}

// TargetString returns the target schema.table string for routing.
func (t TableName) TargetString() string {
	return fmt.Sprintf("%s.%s", t.GetTargetSchema(), t.GetTargetTable())
}

// QuoteTargetString returns quoted full target table name for routing.
func (t TableName) QuoteTargetString() string {
	return QuoteSchema(t.GetTargetSchema(), t.GetTargetTable())
}
