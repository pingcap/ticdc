// Copyright 2023 PingCAP, Inc.
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

package columnselector

import (
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
)

type ColumnSelector struct {
	tableF  filter.Filter
	columnM filter.ColumnFilter
}

func NewDefaultColumnSelector() *ColumnSelector {
	return &ColumnSelector{}
}

func newColumnSelector(
	rule *config.ColumnSelector, caseSensitive bool,
) (*ColumnSelector, error) {
	tableM, err := filter.Parse(rule.Matcher)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Matcher)
	}
	if !caseSensitive {
		tableM = filter.CaseInsensitive(tableM)
	}
	columnM, err := filter.ParseColumnFilter(rule.Columns)
	if err != nil {
		return nil, errors.WrapError(errors.ErrFilterRuleInvalid, err, rule.Columns)
	}

	return &ColumnSelector{
		tableF:  tableM,
		columnM: columnM,
	}, nil
}

// Match implements Transformer interface
func (s *ColumnSelector) match(schema, table string) bool {
	return s.tableF.MatchTable(schema, table)
}

// Select decide whether the col should be encoded or not.
func (s *ColumnSelector) Select(colInfo *model.ColumnInfo) bool {
	if s.columnM == nil {
		return true
	}
	colName := colInfo.Name.O
	return s.columnM.MatchColumn(colName)
}

// ColumnSelectors manages an array of selectors, the first selector match the given
// event is used to select out columns.
type ColumnSelectors struct {
	selectors []*ColumnSelector
}

// New return a column selectors
func New(sinkConfig *config.SinkConfig) (*ColumnSelectors, error) {
	selectors := make([]*ColumnSelector, 0, len(sinkConfig.ColumnSelectors))
	for _, r := range sinkConfig.ColumnSelectors {
		selector, err := newColumnSelector(r, sinkConfig.CaseSensitive)
		if err != nil {
			return nil, err
		}
		selectors = append(selectors, selector)
	}

	return &ColumnSelectors{
		selectors: selectors,
	}, nil
}

func (c *ColumnSelectors) Get(schema, table string) *ColumnSelector {
	for _, s := range c.selectors {
		if s.match(schema, table) {
			return s
		}
	}
	return &ColumnSelector{}
}

// VerifyTables return the error if any given table cannot satisfy the column selector constraints.
// 1. if the column is filter out, it must not be a part of handle key or the unique key.
// 2. if the filtered out column is used in the column dispatcher, return error.
func (c *ColumnSelectors) VerifyTables(
	infos []*common.TableInfo, eventRouter *eventrouter.EventRouter,
) error {
	if len(c.selectors) == 0 {
		return nil
	}

	for _, table := range infos {
		for _, s := range c.selectors {
			if !s.match(table.TableName.Schema, table.TableName.Table) {
				continue
			}

			retainedColumns := make(map[string]struct{})
			for _, columnInfo := range table.GetColumns() {
				columnName := columnInfo.Name.O
				if s.columnM.MatchColumn(columnName) {
					retainedColumns[columnName] = struct{}{}
					continue
				}

				partitionDispatcher := eventRouter.GetPartitionGenerator(table.TableName.Schema, table.TableName.Table)
				switch v := partitionDispatcher.(type) {
				case *partition.ColumnsPartitionGenerator:
					for _, col := range v.Columns {
						if col == columnInfo.Name.O {
							return errors.ErrColumnSelectorFailed.GenWithStack(
								"the filtered out column is used in the column dispatcher, "+
									"table: %v, column: %s", table.TableName, columnInfo.Name)
						}
					}
				default:
				}
			}

			if !verifyIndices(table, retainedColumns) {
				return errors.ErrColumnSelectorFailed.GenWithStack(
					"no primary key columns or unique key columns obtained after filter out, table: %+v", table.TableName)
			}
		}
	}
	return nil
}

// verifyIndices return true if the primary key retained,
// else at least there are one unique key columns in the retained columns.
func verifyIndices(table *common.TableInfo, retainedColumns map[string]struct{}) bool {
	primaryKeyColumns := table.GetPrimaryKeyColumnNames()

	retained := true
	for _, name := range primaryKeyColumns {
		if _, ok := retainedColumns[name]; !ok {
			retained = false
			break
		}
	}
	// primary key columns are retained, return true.
	if retained {
		return true
	}

	// at least one unique key columns are retained, return true.
	for _, index := range table.GetIndices() {
		if !index.Unique {
			continue
		}

		retained = true
		for _, col := range index.Columns {
			if _, ok := retainedColumns[col.Name.O]; !ok {
				retained = false
				break
			}
		}
		if retained {
			return true
		}
	}
	return false
}
