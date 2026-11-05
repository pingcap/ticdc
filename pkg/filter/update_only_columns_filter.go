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

package filter

import (
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

type updateOnlyColumnsFilter struct {
	rules []*updateOnlyColumnsRule
}

type updateOnlyColumnsRule struct {
	mu sync.RWMutex

	tableMatcher  tfilter.Filter
	configured    []string
	caseSensitive bool

	// table ID -> resolved columns for a table schema version.
	tables map[int64]resolvedUpdateOnlyColumns
}

type resolvedUpdateOnlyColumns struct {
	updateTS     uint64
	ignoredColID map[int64]struct{}
	keyColID     map[int64]struct{}
}

func newUpdateOnlyColumnsFilter(cfg *config.FilterConfig, caseSensitive bool) (*updateOnlyColumnsFilter, error) {
	res := &updateOnlyColumnsFilter{}
	for _, rule := range cfg.EventFilters {
		if len(rule.IgnoreUpdateOnlyColumns) == 0 {
			continue
		}
		err := res.addRule(rule, caseSensitive)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return res, nil
}

func (f *updateOnlyColumnsFilter) addRule(cfg *config.EventFilterRule, caseSensitive bool) error {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		return errors.WrapError(errors.ErrFilterRuleInvalid, err, cfg.Matcher)
	}
	if !caseSensitive {
		tf = tfilter.CaseInsensitive(tf)
	}
	rule := &updateOnlyColumnsRule{
		tableMatcher:  tf,
		configured:    append([]string(nil), cfg.IgnoreUpdateOnlyColumns...),
		caseSensitive: caseSensitive,
		tables:        make(map[int64]resolvedUpdateOnlyColumns),
	}
	f.rules = append(f.rules, rule)
	return nil
}

func (f *updateOnlyColumnsFilter) shouldSkipDML(
	dmlType common.RowType,
	preRow, row chunk.Row,
	tableInfo *common.TableInfo,
) (bool, error) {
	if len(f.rules) == 0 || tableInfo == nil || dmlType != common.RowTypeUpdate {
		return false, nil
	}

	for _, rule := range f.rules {
		if !rule.tableMatcher.MatchTable(tableInfo.GetSchemaName(), tableInfo.GetTableName()) {
			continue
		}
		ignore, err := rule.shouldSkipUpdate(preRow, row, tableInfo)
		if err != nil {
			return false, errors.WrapError(errors.ErrFailedToFilterDML, err, row)
		}
		if ignore {
			return true, nil
		}
	}
	return false, nil
}

func (r *updateOnlyColumnsRule) shouldSkipUpdate(
	preRow, row chunk.Row,
	tableInfo *common.TableInfo,
) (bool, error) {
	if preRow.IsEmpty() || row.IsEmpty() {
		return false, nil
	}

	resolved := r.resolveColumns(tableInfo)
	if len(resolved.ignoredColID) == 0 {
		return false, nil
	}

	for _, col := range tableInfo.GetColumns() {
		offset, ok := tableInfo.GetRowColumnsOffset()[col.ID]
		if !ok {
			continue
		}
		if offset >= preRow.Len() || offset >= row.Len() {
			return false, nil
		}

		equal, err := columnValueEqual(preRow, row, offset, &col.FieldType)
		if err != nil {
			return false, err
		}
		if equal {
			continue
		}

		if _, ok := resolved.keyColID[col.ID]; ok {
			return false, nil
		}
		if _, ok := resolved.ignoredColID[col.ID]; !ok {
			return false, nil
		}
	}
	return true, nil
}

func (r *updateOnlyColumnsRule) resolveColumns(tableInfo *common.TableInfo) resolvedUpdateOnlyColumns {
	tableID := tableInfo.TableName.TableID
	updateTS := tableInfo.GetUpdateTS()

	r.mu.RLock()
	if resolved, ok := r.tables[tableID]; ok && resolved.updateTS == updateTS {
		r.mu.RUnlock()
		return resolved
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if resolved, ok := r.tables[tableID]; ok && resolved.updateTS == updateTS {
		return resolved
	}

	resolved := resolvedUpdateOnlyColumns{
		updateTS:     updateTS,
		ignoredColID: make(map[int64]struct{}, len(r.configured)),
		keyColID:     makeColumnIDSet(tableInfo.GetIndexColumns()),
	}
	rowColumnsOffset := tableInfo.GetRowColumnsOffset()
	for _, columnName := range r.configured {
		col, ok := r.resolveColumnByName(tableInfo, columnName)
		if !ok {
			log.Warn("ignore update only column not found, skip it",
				zap.String("schema", tableInfo.GetSchemaName()),
				zap.String("table", tableInfo.GetTableName()),
				zap.String("column", columnName),
				zap.Bool("caseSensitive", r.caseSensitive))
			continue
		}
		if _, ok := rowColumnsOffset[col.ID]; !ok {
			log.Warn("ignore update only column not found, skip it",
				zap.String("schema", tableInfo.GetSchemaName()),
				zap.String("table", tableInfo.GetTableName()),
				zap.String("column", columnName),
				zap.Bool("caseSensitive", r.caseSensitive))
			continue
		}
		resolved.ignoredColID[col.ID] = struct{}{}
	}
	r.tables[tableID] = resolved
	return resolved
}

func (r *updateOnlyColumnsRule) resolveColumnByName(tableInfo *common.TableInfo, name string) (*model.ColumnInfo, bool) {
	if r.caseSensitive {
		return tableInfo.GetColumnInfoByName(name)
	}
	for _, col := range tableInfo.GetColumns() {
		if strings.EqualFold(col.Name.O, name) {
			return col, true
		}
	}
	return nil, false
}

func makeColumnIDSet(indexColumns [][]int64) map[int64]struct{} {
	set := make(map[int64]struct{})
	for _, index := range indexColumns {
		for _, colID := range index {
			set[colID] = struct{}{}
		}
	}
	return set
}

func columnValueEqual(preRow, row chunk.Row, offset int, ft *types.FieldType) (bool, error) {
	if preRow.IsNull(offset) || row.IsNull(offset) {
		return preRow.IsNull(offset) == row.IsNull(offset), nil
	}

	if equal, ok := columnValueEqualFastPath(preRow, row, offset, ft); ok {
		return equal, nil
	}

	preValue := preRow.GetDatum(offset, ft)
	rowValue := row.GetDatum(offset, ft)
	cmp, err := preValue.Compare(types.DefaultStmtNoWarningContext, &rowValue, collate.GetBinaryCollator())
	if err != nil {
		return false, errors.Trace(err)
	}
	return cmp == 0, nil
}

func columnValueEqualFastPath(preRow, row chunk.Row, offset int, ft *types.FieldType) (bool, bool) {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			return preRow.GetUint64(offset) == row.GetUint64(offset), true
		}
		return preRow.GetInt64(offset) == row.GetInt64(offset), true
	case mysql.TypeYear:
		return preRow.GetInt64(offset) == row.GetInt64(offset), true
	case mysql.TypeFloat:
		return preRow.GetFloat32(offset) == row.GetFloat32(offset), true
	case mysql.TypeDouble:
		return preRow.GetFloat64(offset) == row.GetFloat64(offset), true
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return preRow.GetString(offset) == row.GetString(offset), true
	default:
		return false, false
	}
}
