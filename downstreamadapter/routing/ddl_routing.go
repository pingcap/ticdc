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
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
	"go.uber.org/zap"
)

// rewriteDDLQueryWithRouting rewrites a DDL query by applying routing rules
// to transform source table names to target table names.
//
// It only returns the query string and whether the query text changed.
// Canonical DDL schema/table fields are rewritten separately by ApplyToDDLEvent.
func rewriteDDLQueryWithRouting(
	router *Router, ddl *commonEvent.DDLEvent, changefeedID common.ChangeFeedID,
) (string, bool, error) {
	if router == nil || ddl.Query == "" {
		return ddl.Query, false, nil
	}

	// Get the default schema for parsing. If TableInfo is nil (e.g., for
	// database-level DDLs like CREATE DATABASE), FetchDDLTables will extract
	// the schema name directly from the DDL statement itself.
	var originSchema string
	if ddl.TableInfo != nil {
		originSchema = ddl.TableInfo.GetSchemaName()
	}

	// Parse the DDL query using TiDB parser
	p := parser.New()
	stmt, err := p.ParseOneStmt(ddl.Query, "", "")
	if err != nil {
		log.Error("rewrite ddl failed due to parse ddl query",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", ddl.Query), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	// Fetch source tables from the DDL
	sourceTables, err := fetchDDLTables(originSchema, stmt)
	if err != nil {
		log.Error("rewrite ddl failed due to fetch ddl tables",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", ddl.Query), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	if len(sourceTables) == 0 {
		return ddl.Query, false, nil
	}

	// Build target tables by applying routing rules
	var (
		routed       bool
		targetTables = make([]*filter.Table, 0, len(sourceTables))
	)
	for _, srcTable := range sourceTables {
		targetSchema, targetTable := router.Route(srcTable.Schema, srcTable.Name)
		if targetSchema != srcTable.Schema || targetTable != srcTable.Name {
			routed = true
		}
		targetTables = append(targetTables, &filter.Table{
			Schema: targetSchema,
			Name:   targetTable,
		})
	}

	if !routed {
		return ddl.Query, false, nil
	}

	// Rewrite the DDL with target tables
	newQuery, err := rewriteDDLQuery(stmt, targetTables)
	if err != nil {
		log.Error("rewrite ddl failed due to rewrite ddl query",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", ddl.Query), zap.Any("targetTables", targetTables), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	if newQuery != ddl.Query {
		log.Info("DDL query rewritten with routing",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("originalQuery", ddl.Query),
			zap.String("newQuery", newQuery))
	}

	return newQuery, newQuery != ddl.Query, nil
}
