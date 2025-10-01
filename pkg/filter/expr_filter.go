// Copyright 2022 PingCAP, Inc.
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

package filter

import (
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// dmlExprFilterRule only be used by dmlExprFilter.
// This struct is mostly a duplicate of `ExprFilterGroup` in dm/pkg/syncer,
// but have slightly changed to fit the usage of cdc.
type dmlExprFilterRule struct {
	mu sync.Mutex
	// Cache tableInfos to check if the table was changed.
	// table id -> table info version
	tables map[int64]uint64

	insertExprs    map[string]expression.Expression // tableName -> expr
	updateOldExprs map[string]expression.Expression // tableName -> expr
	updateNewExprs map[string]expression.Expression // tableName -> expr
	deleteExprs    map[string]expression.Expression // tableName -> expr

	tableMatcher tfilter.Filter
	// All tables in this rule share the same config.
	config *config.EventFilterRule

	sessCtx sessionctx.Context
}

func newExprFilterRule(
	sessCtx sessionctx.Context,
	cfg *config.EventFilterRule,
) (*dmlExprFilterRule, error) {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg.Matcher)
	}

	ret := &dmlExprFilterRule{
		tables:         make(map[int64]uint64),
		insertExprs:    make(map[string]expression.Expression),
		updateOldExprs: make(map[string]expression.Expression),
		updateNewExprs: make(map[string]expression.Expression),
		deleteExprs:    make(map[string]expression.Expression),
		config:         cfg,
		tableMatcher:   tf,
		sessCtx:        sessCtx,
	}
	return ret, nil
}

// verifyAndInitRule will verify and init the rule.
// It should only be called in dmlExprFilter's verify method.
// We ask users to set these expr only in default sql mode,
// so we just need to  verify each expr in default sql mode
// func (r *dmlExprFilterRule) verify(tableInfos []*model.TableInfo) error {
// 	// verify expression filter rule syntax.
// 	p := parser.New()
// 	_, _, err := p.ParseSQL(completeExpression(r.config.IgnoreInsertValueExpr))
// 	if err != nil {
// 		log.Error("failed to parse expression", zap.Error(err))
// 		return cerror.ErrExpressionParseFailed.
// 			FastGenByArgs(r.config.IgnoreInsertValueExpr)
// 	}
// 	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreUpdateNewValueExpr))
// 	if err != nil {
// 		log.Error("failed to parse expression", zap.Error(err))
// 		return cerror.ErrExpressionParseFailed.
// 			FastGenByArgs(r.config.IgnoreUpdateNewValueExpr)
// 	}
// 	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreUpdateOldValueExpr))
// 	if err != nil {
// 		log.Error("failed to parse expression", zap.Error(err))
// 		return cerror.ErrExpressionParseFailed.
// 			FastGenByArgs(r.config.IgnoreUpdateOldValueExpr)
// 	}
// 	_, _, err = p.ParseSQL(completeExpression(r.config.IgnoreDeleteValueExpr))
// 	if err != nil {
// 		log.Error("failed to parse expression", zap.Error(err))
// 		return cerror.ErrExpressionParseFailed.
// 			FastGenByArgs(r.config.IgnoreDeleteValueExpr)
// 	}
// verify expression filter rule.
// for _, tableInfo := range tableInfos {
// 	tableName := tableInfo.TableName.String()
// 	if !r.tableMatcher.MatchTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
// 		continue
// 	}
// 	if r.config.IgnoreInsertValueExpr != "" {
// 		e, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, tableInfo)
// 		if err != nil {
// 			return err
// 		}
// 		r.insertExprs[tableName] = e
// 	}
// 	if r.config.IgnoreUpdateOldValueExpr != "" {
// 		e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, tableInfo)
// 		if err != nil {
// 			return err
// 		}
// 		r.updateOldExprs[tableName] = e
// 	}
// 	if r.config.IgnoreUpdateNewValueExpr != "" {
// 		e, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, tableInfo)
// 		if err != nil {
// 			return err
// 		}
// 		r.updateNewExprs[tableName] = e
// 	}
// 	if r.config.IgnoreDeleteValueExpr != "" {
// 		e, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, tableInfo)
// 		if err != nil {
// 			return err
// 		}
// 		r.deleteExprs[tableName] = e
// 	}
// }
// 	return nil
// }

// The caller must hold r.mu.Lock() before calling this function.
func (r *dmlExprFilterRule) resetExpr(tableName string) {
	delete(r.insertExprs, tableName)
	delete(r.updateOldExprs, tableName)
	delete(r.updateNewExprs, tableName)
	delete(r.deleteExprs, tableName)
}

// getInsertExprs returns the expression filter to filter INSERT events.
// This function will lazy calculate expressions if not initialized.
func (r *dmlExprFilterRule) getInsertExpr(tableInfo *common.TableInfo) (
	expression.Expression, error,
) {
	tableName := tableInfo.TableName.String()
	if r.insertExprs[tableName] != nil {
		return r.insertExprs[tableName], nil
	}
	if r.config.IgnoreInsertValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreInsertValueExpr, tableInfo.ToTiDBTableInfo())
		if err != nil {
			return nil, err
		}
		r.insertExprs[tableName] = expr
	}
	return r.insertExprs[tableName], nil
}

func (r *dmlExprFilterRule) getUpdateOldExpr(tableInfo *common.TableInfo) (
	expression.Expression, error,
) {
	tableName := tableInfo.TableName.String()
	if r.updateOldExprs[tableName] != nil {
		return r.updateOldExprs[tableName], nil
	}

	if r.config.IgnoreUpdateOldValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateOldValueExpr, tableInfo.ToTiDBTableInfo())
		if err != nil {
			return nil, err
		}
		r.updateOldExprs[tableName] = expr
	}
	return r.updateOldExprs[tableName], nil
}

func (r *dmlExprFilterRule) getUpdateNewExpr(tableInfo *common.TableInfo) (
	expression.Expression, error,
) {
	tableName := tableInfo.TableName.String()
	if r.updateNewExprs[tableName] != nil {
		return r.updateNewExprs[tableName], nil
	}

	if r.config.IgnoreUpdateNewValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreUpdateNewValueExpr, tableInfo.ToTiDBTableInfo())
		if err != nil {
			return nil, err
		}
		r.updateNewExprs[tableName] = expr
	}
	return r.updateNewExprs[tableName], nil
}

func (r *dmlExprFilterRule) getDeleteExpr(tableInfo *common.TableInfo) (
	expression.Expression, error,
) {
	tableName := tableInfo.TableName.String()
	if r.deleteExprs[tableName] != nil {
		return r.deleteExprs[tableName], nil
	}

	if r.config.IgnoreDeleteValueExpr != "" {
		expr, err := r.getSimpleExprOfTable(r.config.IgnoreDeleteValueExpr, tableInfo.ToTiDBTableInfo())
		if err != nil {
			return nil, err
		}
		r.deleteExprs[tableName] = expr
	}
	return r.deleteExprs[tableName], nil
}

func (r *dmlExprFilterRule) getSimpleExprOfTable(
	expr string,
	tableInfo *model.TableInfo,
) (expression.Expression, error) {
	e, err := expression.ParseSimpleExprWithTableInfo(r.sessCtx.GetExprCtx(), expr, tableInfo)
	if err != nil {
		// If an expression contains an unknown column,
		// we return an error and stop the changefeed.
		if plannererrors.ErrUnknownColumn.Equal(err) {
			log.Error("meet unknown column when generating expression",
				zap.String("expression", expr),
				zap.Error(err))
			return nil, cerror.ErrExpressionColumnNotFound.
				FastGenByArgs(getColumnFromError(err), tableInfo.Name.String(), expr)
		}
		log.Error("failed to parse expression", zap.Error(err))
		return nil, cerror.ErrExpressionParseFailed.FastGenByArgs(err, expr)
	}
	return e, nil
}

func (r *dmlExprFilterRule) shouldSkipDML(
	dmlType common.RowType,
	preRow, row chunk.Row,
	tableInfo *common.TableInfo,
) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ver, ok := r.tables[tableInfo.TableName.TableID]; ok {
		// If one table's tableInfo was updated, we need to reset this rule
		// and update the tableInfo in the cache.
		if tableInfo.GetUpdateTS() != ver {
			r.tables[tableInfo.TableName.TableID] = tableInfo.GetUpdateTS()
			r.resetExpr(tableInfo.TableName.String())
		}
	} else {
		r.tables[tableInfo.TableName.TableID] = tableInfo.GetUpdateTS()
	}

	switch dmlType {
	case common.RowTypeInsert:
		exprs, err := r.getInsertExpr(tableInfo)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(
			row,
			exprs,
			tableInfo,
		)
	case common.RowTypeUpdate:
		oldExprs, err := r.getUpdateOldExpr(tableInfo)
		if err != nil {
			return false, err
		}
		newExprs, err := r.getUpdateNewExpr(tableInfo)
		if err != nil {
			return false, err
		}
		ignoreOld, err := r.skipDMLByExpression(
			preRow,
			oldExprs,
			tableInfo,
		)
		if err != nil {
			return false, err
		}
		ignoreNew, err := r.skipDMLByExpression(
			row,
			newExprs,
			tableInfo,
		)
		if err != nil {
			return false, err
		}
		return ignoreOld || ignoreNew, nil
	case common.RowTypeDelete:
		exprs, err := r.getDeleteExpr(tableInfo)
		if err != nil {
			return false, err
		}
		return r.skipDMLByExpression(
			preRow,
			exprs,
			tableInfo,
		)
	default:
		log.Warn("unknown row changed event type")
		return false, nil
	}
}

func (r *dmlExprFilterRule) buildRowWithVirtualColumns(
	row chunk.Row,
	tableInfo *common.TableInfo,
) (chunk.Row, error) {
	if !tableInfo.HasVirtualColumns() {
		return row, nil
	}

	columns, _, err := expression.ColumnInfos2ColumnsAndNames(
		r.sessCtx.GetExprCtx(),
		ast.CIStr{}, /* unused */
		tableInfo.GetTableNameCIStr(),
		tableInfo.GetColumns(),
		tableInfo.ToTiDBTableInfo())
	if err != nil {
		return chunk.Row{}, err
	}

	vColOffsets, vColFts := collectVirtualColumnOffsetsAndTypes(r.sessCtx.GetExprCtx().GetEvalCtx(), columns)
	err = table.FillVirtualColumnValue(vColFts, vColOffsets, columns, tableInfo.GetColumns(), r.sessCtx.GetExprCtx(), row.Chunk())

	err = table.FillVirtualColumnValue(vColFts, vColOffsets, columns, tableInfo.GetColumns(), r.sessCtx.GetExprCtx(), row.Chunk())
	if err != nil {
		return chunk.Row{}, err
	}
	return row, nil
}

func collectVirtualColumnOffsetsAndTypes(ctx expression.EvalContext, cols []*expression.Column) ([]int, []*types.FieldType) {
	var offsets []int
	var fts []*types.FieldType
	for i, col := range cols {
		if col.VirtualExpr != nil {
			offsets = append(offsets, i)
			fts = append(fts, col.GetType(ctx))
		}
	}
	return offsets, fts
}

func (r *dmlExprFilterRule) skipDMLByExpression(
	row chunk.Row,
	expr expression.Expression,
	tableInfo *common.TableInfo,
) (bool, error) {
	if row.IsEmpty() || expr == nil {
		return false, nil
	}
	row, err := r.buildRowWithVirtualColumns(row, tableInfo)
	if err != nil {
		return false, errors.Trace(err)
	}
	d, err := expr.Eval(r.sessCtx.GetExprCtx().GetEvalCtx(), row)
	if err != nil {
		log.Error("failed to eval expression", zap.Error(err))
		return false, errors.Trace(err)
	}
	if d.GetInt64() == 1 {
		return true, nil
	}
	return false, nil
}

func getColumnFromError(err error) string {
	if !plannererrors.ErrUnknownColumn.Equal(err) {
		return err.Error()
	}
	column := strings.TrimSpace(strings.TrimPrefix(err.Error(),
		"[planner:1054]Unknown column '"))
	column = strings.TrimSuffix(column, "' in 'expression'")
	return column
}

// dmlExprFilter is a filter that filters DML events by SQL expression.
type dmlExprFilter struct {
	rules []*dmlExprFilterRule
}

func newExprFilter(
	timezone string,
	cfg *config.FilterConfig,
) (*dmlExprFilter, error) {
	res := &dmlExprFilter{}
	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": timezone,
	})
	for _, rule := range cfg.EventFilters {
		err := res.addRule(sessCtx, rule)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (f *dmlExprFilter) addRule(
	sessCtx sessionctx.Context,
	cfg *config.EventFilterRule,
) error {
	rule, err := newExprFilterRule(sessCtx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	f.rules = append(f.rules, rule)
	return nil
}

// verify checks if all rules in this filter is valid.
// func (f *dmlExprFilter) verify(tableInfos []*model.TableInfo) error {
// 	for _, rule := range f.rules {
// 		err := rule.verify(tableInfos)
// 		if err != nil {
// 			log.Error("failed to verify expression filter rule", zap.Error(err))
// 			return errors.Trace(err)
// 		}
// 	}
// 	return nil
// }

func (f *dmlExprFilter) getRules(schema, table string) []*dmlExprFilterRule {
	res := make([]*dmlExprFilterRule, 0)
	for _, rule := range f.rules {
		if rule.tableMatcher.MatchTable(schema, table) {
			res = append(res, rule)
		}
	}
	return res
}

// shouldSkipDML skips dml event by sql expression.
func (f *dmlExprFilter) shouldSkipDML(
	dmlType common.RowType,
	preRow, row chunk.Row,
	tableInfo *common.TableInfo,
) (bool, error) {
	if len(f.rules) == 0 {
		return false, nil
	}
	// for defense purpose, normally the tableInfo should not be nil.
	if tableInfo == nil {
		return false, nil
	}

	rules := f.getRules(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	for _, rule := range rules {
		ignore, err := rule.shouldSkipDML(dmlType, preRow, row, tableInfo)
		if err != nil {
			if cerror.ShouldFailChangefeed(err) {
				return false, err
			}
			return false, cerror.WrapError(cerror.ErrFailedToFilterDML, err, row)
		}
		if ignore {
			return true, nil
		}
	}
	return false, nil
}
