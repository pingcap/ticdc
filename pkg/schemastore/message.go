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

package schemastore

import (
	"time"

	"github.com/pingcap/ticdc/eventpb"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
)

const (
	RequestTimeout  = 10 * time.Minute
	TableBatchSize  = 128
	TableBatchBytes = 4 << 20
)

func NewKeyspaceMeta(meta common.KeyspaceMeta) KeyspaceMeta {
	return KeyspaceMeta{ID: meta.ID, Name: meta.Name}
}

func (m KeyspaceMeta) ToCommon() common.KeyspaceMeta {
	return common.KeyspaceMeta{ID: m.ID, Name: m.Name}
}

func NewError(err error) *Error {
	if err == nil {
		return nil
	}
	code, _ := errors.RFCCode(err)
	return &Error{Message: err.Error(), Code: string(code)}
}

func (e *Error) ToError() error {
	if e == nil {
		return nil
	}
	if e.Code != "" {
		return errors.Normalize(e.Message, errors.RFCCodeText(e.Code)).GenWithStackByArgs()
	}
	return errors.ErrSchemaStoreRequestFailed.GenWithStack("%s", e.Message)
}

func NewFilterConfig(cfg *config.FilterConfig) *eventpb.InnerFilterConfig {
	if cfg == nil {
		return nil
	}
	result := &eventpb.InnerFilterConfig{Rules: cfg.Rules, IgnoreTxnStartTs: cfg.IgnoreTxnStartTs}
	for _, rule := range cfg.EventFilters {
		r := &eventpb.EventFilterRule{
			Matcher: rule.Matcher, IgnoreSql: rule.IgnoreSQL,
			IgnoreInsertValueExpr:    util.GetOrZero(rule.IgnoreInsertValueExpr),
			IgnoreUpdateNewValueExpr: util.GetOrZero(rule.IgnoreUpdateNewValueExpr),
			IgnoreUpdateOldValueExpr: util.GetOrZero(rule.IgnoreUpdateOldValueExpr),
			IgnoreDeleteValueExpr:    util.GetOrZero(rule.IgnoreDeleteValueExpr),
			IgnoreUpdateOnlyColumns:  rule.IgnoreUpdateOnlyColumns,
		}
		for _, event := range rule.IgnoreEvent {
			r.IgnoreEvent = append(r.IgnoreEvent, string(event))
		}
		result.EventFilters = append(result.EventFilters, r)
	}
	return result
}

func FilterConfigFromProto(cfg *eventpb.InnerFilterConfig) *config.FilterConfig {
	if cfg == nil {
		return nil
	}
	result := &config.FilterConfig{Rules: cfg.Rules, IgnoreTxnStartTs: cfg.IgnoreTxnStartTs}
	for _, rule := range cfg.EventFilters {
		r := &config.EventFilterRule{
			Matcher: rule.Matcher, IgnoreSQL: rule.IgnoreSql,
			IgnoreInsertValueExpr:    util.AddressOf(rule.IgnoreInsertValueExpr),
			IgnoreUpdateNewValueExpr: util.AddressOf(rule.IgnoreUpdateNewValueExpr),
			IgnoreUpdateOldValueExpr: util.AddressOf(rule.IgnoreUpdateOldValueExpr),
			IgnoreDeleteValueExpr:    util.AddressOf(rule.IgnoreDeleteValueExpr),
			IgnoreUpdateOnlyColumns:  rule.IgnoreUpdateOnlyColumns,
		}
		for _, event := range rule.IgnoreEvent {
			r.IgnoreEvent = append(r.IgnoreEvent, bf.EventType(event))
		}
		result.EventFilters = append(result.EventFilters, r)
	}
	return result
}

func NewPhysicalTables(tables []commonEvent.Table) []PhysicalTable {
	if tables == nil {
		return nil
	}
	result := make([]PhysicalTable, len(tables))
	for i, table := range tables {
		result[i] = PhysicalTable{SchemaID: table.SchemaID, TableID: table.TableID, Splitable: table.Splitable}
		if table.SchemaTableName != nil {
			result[i].SchemaTableName = &SchemaTableName{SchemaName: table.SchemaName, TableName: table.TableName}
		}
	}
	return result
}

func PhysicalTablesFromProto(tables []PhysicalTable) []commonEvent.Table {
	if tables == nil {
		return nil
	}
	result := make([]commonEvent.Table, len(tables))
	for i, table := range tables {
		result[i] = commonEvent.Table{SchemaID: table.SchemaID, TableID: table.TableID, Splitable: table.Splitable}
		if table.SchemaTableName != nil {
			result[i].SchemaTableName = &commonEvent.SchemaTableName{SchemaName: table.SchemaTableName.SchemaName, TableName: table.SchemaTableName.TableName}
		}
	}
	return result
}
