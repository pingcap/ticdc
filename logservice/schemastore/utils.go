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
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

// parseCreateTableQueries preserves the text of independently submitted CREATEs.
// TiDB retains only the first job's SQL mode. Ambiguous later statements must
// agree with the table metadata before their AST can be used downstream.
func parseCreateTableQueries(job *model.Job) ([]ast.StmtNode, error) {
	p := parser.New()
	encoding := charset.FindEncoding(job.Charset)
	modes := [...]mysql.SQLMode{
		job.SQLMode,
		job.SQLMode ^ mysql.ModeANSIQuotes,
		job.SQLMode ^ mysql.ModeNoBackslashEscapes,
		job.SQLMode ^ (mysql.ModeANSIQuotes | mysql.ModeNoBackslashEscapes),
	}
	type candidate struct {
		stmt     *ast.CreateTableStmt
		mode     mysql.SQLMode
		end      int
		query    string
		metadata map[string]any
	}
	type position struct{ remaining, table int }
	failed := make(map[position]bool)
	var result []ast.StmtNode
	var parseErr error
	var parseRemaining func(string) bool
	parseRemaining = func(remaining string) bool {
		remaining = strings.TrimSpace(remaining)
		if remaining == "" {
			return true
		}
		pos := position{len(remaining), len(result)}
		if failed[pos] {
			return false
		}
		var candidates []candidate
		for _, mode := range modes {
			end := createTableStatementEnd(remaining, mode, encoding)
			p.SetSQLMode(mode)
			stmts, _, err := p.Parse(remaining[:end], job.Charset, job.Collate)
			if err != nil {
				parseErr = errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
				continue
			}
			if len(stmts) == 0 {
				if parseRemaining(remaining[end:]) {
					return true
				}
				continue
			}
			if len(stmts) != 1 {
				continue
			}
			stmt, ok := stmts[0].(*ast.CreateTableStmt)
			if !ok {
				continue
			}
			query, err := commonEvent.Restore(stmt)
			if err != nil {
				parseErr = errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
				continue
			}
			duplicate := false
			for _, c := range candidates {
				if c.end == end && c.query == query {
					duplicate = true
					break
				}
			}
			if !duplicate {
				candidates = append(candidates, candidate{stmt: stmt, mode: mode, end: end, query: query})
			}
		}
		var actualMetadata map[string]any
		if len(candidates) > 1 && len(result) > 0 {
			if job.BinlogInfo == nil || len(result) >= len(job.BinlogInfo.MultipleTableInfos) {
				failed[pos] = true
				return false
			}
			actualMetadata = createTableLiteralMetadata(job.BinlogInfo.MultipleTableInfos[len(result)])
			for i := range candidates {
				c := &candidates[i]
				ctx := metabuild.NewContext(metabuild.WithExprCtx(exprstatic.NewExprContext(
					exprstatic.WithEvalCtx(exprstatic.NewEvalContext(exprstatic.WithSQLMode(c.mode))),
				)))
				info := job.BinlogInfo.MultipleTableInfos[len(result)]
				if info == nil {
					continue
				}
				parsed, err := ddl.BuildTableInfoWithStmt(ctx, c.stmt, info.Charset, info.Collate, nil)
				if err != nil {
					parseErr = errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
					continue
				}
				c.metadata = createTableLiteralMetadata(parsed)
			}
		}
	candidateLoop:
		for _, c := range candidates {
			if len(candidates) > 1 {
				if len(result) == 0 {
					// Only the first statement's mode survives TiDB's job merge.
					if c.mode != job.SQLMode {
						continue
					}
				} else {
					if c.metadata == nil {
						continue
					}
					// Compare only fields that vary with SQL mode. Unrelated
					// fields, such as TIMESTAMP defaults, can depend on session
					// context that the merged job also did not retain.
					for key, value := range c.metadata {
						for _, other := range candidates {
							if other.metadata != nil && !reflect.DeepEqual(value, other.metadata[key]) &&
								!reflect.DeepEqual(value, actualMetadata[key]) {
								continue candidateLoop
							}
						}
					}
				}
			}
			result = append(result, c.stmt)
			// A prefix alone cannot prove a boundary: an alternate quote mode
			// might have terminated a string early. Validate the entire suffix.
			if parseRemaining(remaining[c.end:]) {
				return true
			}
			result = result[:len(result)-1]
		}
		failed[pos] = true
		return false
	}
	if parseRemaining(job.Query) {
		return result, nil
	}
	if parseErr != nil {
		return nil, parseErr
	}
	return nil, errors.ErrTiDBUnexpectedJobMeta.GenWithStack("cannot separate batch CREATE statements with matching SQL mode semantics")
}

// createTableStatementEnd scans one statement without parsing every quoted
// semicolon. The SQL parser still validates the statement and executable comments.
func createTableStatementEnd(query string, mode mysql.SQLMode, encoding charset.Encoding) int {
	var quote byte
	for i := 0; i < len(query); i++ {
		if size := encoding.MbLen(query[i:]); size > 1 {
			i += size - 1
			continue
		}
		ch := query[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(query) && query[i+1] == quote {
					i++
				} else {
					quote = 0
				}
			} else if ch == '\\' && quote != '`' && !mode.HasNoBackslashEscapesMode() {
				i++
			}
			continue
		}
		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '#':
			if end := strings.IndexByte(query[i:], '\n'); end >= 0 {
				i += end
			} else {
				return len(query)
			}
		case '-':
			if i+2 < len(query) && query[i+1] == '-' && unicode.IsSpace(rune(query[i+2])) {
				if end := strings.IndexByte(query[i+3:], '\n'); end >= 0 {
					i += 3 + end
				} else {
					return len(query)
				}
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				if end := strings.Index(query[i+2:], "*/"); end >= 0 {
					i += end + 3
				} else {
					return len(query)
				}
			}
		case ';':
			return i + 1
		}
	}
	return len(query)
}

// createTableLiteralMetadata records fields affected by quoting and string
// escapes, leaving physical IDs and session-dependent storage options aside.
func createTableLiteralMetadata(info *model.TableInfo) map[string]any {
	if info == nil {
		return nil
	}
	values := map[string]any{
		"name":            info.Name.L,
		"comment":         info.Comment,
		"compression":     info.Compression,
		"engineAttribute": info.EngineAttribute,
		"columnCount":     len(info.Columns),
	}
	for i, col := range info.Columns {
		prefix := "column/" + strconv.Itoa(i) + "/"
		values[prefix+"name"] = col.Name.L
		values[prefix+"comment"] = col.Comment
		values[prefix+"default"] = col.GetDefaultValue()
		values[prefix+"defaultIsExpr"] = col.DefaultIsExpr
		values[prefix+"generated"] = col.GeneratedExprString
		values[prefix+"elems"] = col.GetElems()
	}
	for _, index := range info.Indices {
		values["indexComment/"+index.Name.L] = index.Comment
		values["indexCondition/"+index.Name.L] = index.ConditionExprString
	}
	for i, constraint := range info.Constraints {
		values["constraint/"+strconv.Itoa(i)] = constraint.ExprString
	}
	if info.Partition != nil {
		values["partitionExpr"] = info.Partition.Expr
		values["partitionCount"] = len(info.Partition.Definitions)
		for i, partition := range info.Partition.Definitions {
			prefix := "partition/" + strconv.Itoa(i) + "/"
			values[prefix+"name"] = partition.Name.L
			values[prefix+"comment"] = partition.Comment
			values[prefix+"lessThan"] = partition.LessThan
			values[prefix+"inValues"] = partition.InValues
		}
	}
	if info.TTLInfo != nil {
		values["ttlInterval"] = info.TTLInfo.IntervalExprStr
		values["ttlJobInterval"] = info.TTLInfo.JobInterval
	}
	return values
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
