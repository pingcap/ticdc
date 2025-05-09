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
	"fmt"

	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	tifilter "github.com/pingcap/tidb/pkg/util/filter"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// IsSysSchema returns true if the given schema is a system schema
func IsSysSchema(db string) bool {
	switch db {
	// TiCDCSystemSchema is used by TiCDC only.
	// Tables in TiCDCSystemSchema should not be replicated by cdc.
	case TiCDCSystemSchema:
		return true
	case LightningTaskInfoSchema:
		return true
	// TODO: skip workload schema, ref https://github.com/pingcap/ticdc/issues/1105 .
	case TiDBWorkloadSchema:
		return true
	default:
		return tifilter.IsSystemSchema(db)
	}
}

// VerifyTableRules checks the table filter rules in the configuration
// and returns an invalid rule error if the verification fails,
// otherwise it will return a table filter.
func VerifyTableRules(cfg *config.FilterConfig) (tfilter.Filter, error) {
	rules := cfg.Rules
	if len(rules) == 0 {
		rules = []string{"*.*"}
	}
	f, err := tfilter.Parse(rules)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg)
	}

	return f, nil
}

// ddlToEventType get event type from ddl query.
func ddlToEventType(jobType timodel.ActionType) bf.EventType {
	evenType, ok := ddlWhiteListMap[jobType]
	if ok {
		return evenType
	}
	return bf.NullEvent
}

var alterTableSubType = []timodel.ActionType{
	// table related DDLs
	timodel.ActionRenameTable,
	timodel.ActionRenameTables,
	timodel.ActionModifyTableComment,
	timodel.ActionModifyTableCharsetAndCollate,

	// partition related DDLs
	timodel.ActionAddTablePartition,
	timodel.ActionDropTablePartition,
	timodel.ActionTruncateTablePartition,
	timodel.ActionExchangeTablePartition,
	timodel.ActionReorganizePartition,
	timodel.ActionAlterTablePartitioning,
	timodel.ActionRemovePartitioning,

	// column related DDLs
	timodel.ActionAddColumn,
	timodel.ActionDropColumn,
	timodel.ActionModifyColumn,
	timodel.ActionSetDefaultValue,

	// index related DDLs
	timodel.ActionRebaseAutoID,
	timodel.ActionAddPrimaryKey,
	timodel.ActionDropPrimaryKey,
	timodel.ActionAddIndex,
	timodel.ActionDropIndex,
	timodel.ActionRenameIndex,
	timodel.ActionAlterIndexVisibility,

	// TTL related DDLs
	timodel.ActionAlterTTLInfo,
	timodel.ActionAlterTTLRemove,

	// difficult to classify DDLs
	timodel.ActionMultiSchemaChange,

	// deprecated DDLs,see https://github.com/pingcap/tidb/pull/35862.
	// DDL types below are deprecated in TiDB v6.2.0, but we still keep them here
	// In case that some users will use TiCDC to replicate data from TiDB v6.1.x.
	timodel.ActionAddColumns,
	timodel.ActionDropColumns,
}

// isAlterTable returns true if the given job type is alter table's subtype.
func isAlterTable(jobType timodel.ActionType) bool {
	for _, t := range alterTableSubType {
		if t == jobType {
			return true
		}
	}
	return false
}

// SupportedEventTypes returns the supported event types.
func SupportedEventTypes() []bf.EventType {
	supportedEventTypes := []bf.EventType{
		bf.AllDML,
		bf.AllDDL,

		// dml events
		bf.InsertEvent,
		bf.UpdateEvent,
		bf.DeleteEvent,

		// ddl events
		bf.AlterTable,
		bf.CreateSchema,
		bf.DropSchema,
	}

	for _, ddlType := range ddlWhiteListMap {
		supportedEventTypes = append(supportedEventTypes, ddlType)
	}
	return supportedEventTypes
}

func completeExpression(suffix string) string {
	if suffix == "" {
		return suffix
	}
	return fmt.Sprintf("select * from t where %s", suffix)
}
