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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

// parseCreateTableQueries splits a TiDB CREATE batch without rewriting its statements.
// TiDB retains only the first job's SQL mode, so each statement may need different quote modes.
func parseCreateTableQueries(job *model.Job) ([]ast.StmtNode, error) {
	p := parser.New()
	modes := [...]mysql.SQLMode{
		job.SQLMode,
		job.SQLMode ^ mysql.ModeANSIQuotes,
		job.SQLMode ^ mysql.ModeNoBackslashEscapes,
		job.SQLMode ^ (mysql.ModeANSIQuotes | mysql.ModeNoBackslashEscapes),
	}
	var result []ast.StmtNode
	remaining := strings.TrimSpace(job.Query)
	for remaining != "" {
		end := strings.IndexByte(remaining, ';')
		for {
			query, expectedCount := remaining, 1
			if end >= 0 {
				// A second statement proves the semicolon is a separator, even
				// when a quoted value or a line comment contains semicolons.
				query, expectedCount = remaining[:end+1]+" SELECT 1", 2
			}
			var stmts []ast.StmtNode
			var err error
			for _, mode := range modes {
				p.SetSQLMode(mode)
				stmts, _, err = p.Parse(query, job.Charset, job.Collate)
				if err == nil && (len(stmts) == expectedCount || end < 0 && len(stmts) == 0) {
					break
				}
			}
			if err == nil && len(stmts) == expectedCount {
				result = append(result, stmts[0])
				if end < 0 {
					return result, nil
				}
				remaining = strings.TrimSpace(remaining[end+1:])
				break
			}
			if end < 0 {
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrTiDBUnexpectedJobMeta, err)
				}
				if len(stmts) == 0 {
					return result, nil
				}
				return nil, cerror.ErrTiDBUnexpectedJobMeta.GenWithStack("cannot separate batch CREATE statements")
			}
			next := strings.IndexByte(remaining[end+1:], ';')
			if next < 0 {
				end = -1
			} else {
				end += next + 1
			}
		}
	}
	return result, nil
}

// transform ddl query based on sql mode.
func transformDDLJobQuery(job *model.Job) (string, error) {
	p := parser.New()
	// We need to use the correct SQL mode to parse the DDL query.
	// Otherwise, the parser may fail to parse the DDL query.
	// For example, it is needed to parse the following DDL query:
	//  `alter table "t" add column "c" int default 1;`
	// by adding `ANSI_QUOTES` to the SQL mode.
	p.SetSQLMode(job.SQLMode)
	stmts, _, err := p.Parse(job.Query, job.Charset, job.Collate)
	if err != nil {
		return "", errors.Trace(err)
	}
	var result string

	if len(stmts) > 1 {
		results := make([]string, 0, len(stmts))
		for _, stmt := range stmts {
			query, err := commonEvent.Restore(stmt)
			if err != nil {
				return "", errors.Trace(err)
			}
			results = append(results, query)
		}
		result = strings.Join(results, ";")
	} else {
		result, err = commonEvent.Restore(stmts[0])
		if err != nil {
			return "", errors.Trace(err)
		}
	}

	log.Info("transform ddl query to result", zap.String("charset", job.Charset),
		zap.String("collate", job.Collate), zap.String("result", result))
	return result, nil
}

// isSplitable returns whether the table is eligible for split in all sinks
// Only tables with a clustered primary key and no other unique key can be split in all sinks.
func isSplitable(tableInfo *model.TableInfo) bool {
	return common.IsSplitable(tableInfo.PKIsHandle || tableInfo.IsCommonHandle, tableInfo.Indices)
}

func getIndexIDs(job *model.Job) []int64 {
	if job == nil {
		return nil
	}

	// Anonymous index rewrite only needs IDs for ADD INDEX clauses, and it
	// consumes them in SQL order. Other modify-index subjobs such as DROP INDEX,
	// RENAME INDEX, or ADD PRIMARY KEY would shift that positional mapping and
	// make the downstream rewrite pick the wrong upstream name.
	if job.Type == model.ActionAddIndex {
		return extractAddIndexIDs(job)
	}

	if job.MultiSchemaInfo == nil {
		return nil
	}

	res := make([]int64, 0)
	for idx, subJob := range job.MultiSchemaInfo.SubJobs {
		if subJob.Type != model.ActionAddIndex {
			continue
		}
		proxyJob := subJob.ToProxyJob(job, idx)
		res = append(res, extractAddIndexIDs(&proxyJob)...)
	}
	return res
}

func extractAddIndexIDs(job *model.Job) []int64 {
	idxArgs, err := model.GetModifyIndexArgs(job)
	if idxArgs == nil || err != nil {
		return nil
	}

	res := make([]int64, 0, len(idxArgs.IndexArgs))
	for _, indexArg := range idxArgs.IndexArgs {
		res = append(res, indexArg.IndexID)
	}
	return res
}
