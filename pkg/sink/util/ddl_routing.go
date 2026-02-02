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

package util

import (
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
	"go.uber.org/zap"
)

// DDLRoutingResult contains the result of DDL routing rewriting.
type DDLRoutingResult struct {
	// NewQuery is the rewritten DDL query with target table names.
	NewQuery string
	// TargetSchemaName is the target schema to use for the USE statement (if needed).
	// This is set to the first target table's schema.
	TargetSchemaName string
	// WasRewritten indicates whether the query was actually rewritten (had routing applied).
	WasRewritten bool
}

// RewriteDDLQueryWithRouting rewrites a DDL query by applying routing rules
// to transform source table names to target table names.
//
// This function is used by both MySQL and Redo sinks to ensure DDL statements
// use the correct target table names when sink routing is configured.
//
// Parameters:
//   - router: The Router instance containing routing rules. May be nil.
//   - ddl: The DDL event containing the query to rewrite.
//   - changefeedID: The changefeed ID for logging purposes.
//
// Returns:
//   - *DDLRoutingResult: Contains the rewritten query and metadata.
//   - error: Returns an error if DDL parsing or rewriting fails. Silent fallback
//     is not acceptable as it could lead to data going to wrong tables.
//
// IMPORTANT: This function does NOT mutate ddl.Query. Callers are responsible
// for applying the returned NewQuery to the DDL event if WasRewritten is true.
func RewriteDDLQueryWithRouting(router *Router, ddl *commonEvent.DDLEvent, changefeedID string) (*DDLRoutingResult, error) {
	result := &DDLRoutingResult{
		NewQuery:     ddl.Query,
		WasRewritten: false,
	}

	// Early returns for no-op cases
	if router == nil || ddl.Query == "" {
		return result, nil
	}

	// Get the default schema for parsing. If TableInfo is nil (e.g., for
	// database-level DDLs like CREATE DATABASE), FetchDDLTables will extract
	// the schema name directly from the DDL statement itself.
	defaultSchema := ""
	if ddl.TableInfo != nil {
		defaultSchema = ddl.TableInfo.GetSchemaName()
	}

	// Parse the DDL query using TiDB parser
	p := parser.New()
	stmt, err := p.ParseOneStmt(ddl.Query, "", "")
	if err != nil {
		return nil, errors.Errorf("failed to parse DDL query for routing: %v (query: %s)", err, ddl.Query)
	}

	// Fetch source tables from the DDL
	sourceTables, err := FetchDDLTables(defaultSchema, stmt)
	if err != nil {
		return nil, errors.Errorf("failed to fetch tables from DDL for routing: %v (query: %s)", err, ddl.Query)
	}

	if len(sourceTables) == 0 {
		return result, nil
	}

	// Build target tables by applying routing rules
	targetTables := make([]*filter.Table, 0, len(sourceTables))
	hasRouting := false
	for _, srcTable := range sourceTables {
		targetSchema, targetTable := router.Route(srcTable.Schema, srcTable.Name)
		if targetSchema != srcTable.Schema || targetTable != srcTable.Name {
			hasRouting = true
		}
		targetTables = append(targetTables, &filter.Table{
			Schema: targetSchema,
			Name:   targetTable,
		})
	}

	if !hasRouting {
		return result, nil
	}

	// Rewrite the DDL with target tables
	newQuery, err := RenameDDLTable(stmt, targetTables)
	if err != nil {
		return nil, errors.Errorf("failed to rewrite DDL query with routing: %v (query: %s)", err, ddl.Query)
	}

	if newQuery != ddl.Query {
		log.Info("DDL query rewritten with routing",
			zap.String("changefeed", changefeedID),
			zap.String("originalQuery", ddl.Query),
			zap.String("newQuery", newQuery))
		result.NewQuery = newQuery
		result.WasRewritten = true
	}

	// Set the target schema for the USE command when executing the DDL.
	// We use the first target table's schema because:
	// 1. For single-table DDLs, this is the correct target schema
	// 2. For multi-table DDLs (e.g., RENAME TABLE), all table references in the
	//    rewritten query are fully qualified, so the USE command just needs to
	//    switch to any database the user has access to - the first target schema
	//    is guaranteed to be accessible since routing was configured for it.
	if len(targetTables) > 0 {
		result.TargetSchemaName = targetTables[0].Schema
	}

	return result, nil
}
