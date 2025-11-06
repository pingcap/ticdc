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

package kafka

import (
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

func BuildDMLLogFields(info *common.MessageLogInfo) []zap.Field {
	if info == nil || len(info.Rows) == 0 {
		return nil
	}

	rows := make([]map[string]interface{}, 0, len(info.Rows))
	for _, row := range info.Rows {
		if row.Database == "" && row.Table == "" && row.Type == "" && row.CommitTs == 0 && len(row.PrimaryKeys) == 0 {
			continue
		}
		rowMap := map[string]interface{}{
			"type":      row.Type,
			"database":  row.Database,
			"table":     row.Table,
			"commitTs":  row.CommitTs,
			"primaryPK": nil,
		}
		if len(row.PrimaryKeys) > 0 {
			pkMap := make(map[string]interface{}, len(row.PrimaryKeys))
			for _, pk := range row.PrimaryKeys {
				pkMap[pk.Name] = pk.Value
			}
			rowMap["primaryPK"] = pkMap
		} else {
			delete(rowMap, "primaryPK")
		}
		if row.CommitTs == 0 {
			delete(rowMap, "commitTs")
		}
		rows = append(rows, rowMap)
	}
	if len(rows) == 0 {
		return nil
	}
	return []zap.Field{zap.Any("dmlInfo", rows)}
}

// BuildDDLLogFields converts DDL log info into zap fields.
func BuildDDLLogFields(info *common.MessageLogInfo) []zap.Field {
	if info == nil || info.DDL == nil {
		return nil
	}

	ddlInfo := map[string]interface{}{}
	if info.DDL.Query != "" {
		ddlInfo["query"] = info.DDL.Query
	}
	if info.DDL.CommitTs != 0 {
		ddlInfo["commitTs"] = info.DDL.CommitTs
	}
	if len(ddlInfo) == 0 {
		return nil
	}
	return []zap.Field{zap.Any("ddlInfo", ddlInfo)}
}

// BuildCheckpointLogFields converts checkpoint log info into zap fields.
func BuildCheckpointLogFields(info *common.MessageLogInfo) []zap.Field {
	if info == nil || info.Checkpoint == nil {
		return nil
	}
	if info.Checkpoint.CommitTs == 0 {
		return nil
	}
	return []zap.Field{zap.Uint64("checkpointTs", info.Checkpoint.CommitTs)}
}

// DetermineEventType infers the event type based on MessageLogInfo content.
func DetermineEventType(info *common.MessageLogInfo) string {
	if info == nil {
		return "unknown"
	}
	if info.DDL != nil {
		return "ddl"
	}
	if info.Checkpoint != nil {
		return "checkpoint"
	}
	if len(info.Rows) > 0 {
		return "dml"
	}
	return "unknown"
}

// BuildEventLogFields builds zap fields for an event, including keyspace/changefeed/eventType and details.
func BuildEventLogFields(keyspace, changefeed string, info *common.MessageLogInfo) []zap.Field {
	fields := []zap.Field{
		zap.String("keyspace", keyspace),
		zap.String("changefeed", changefeed),
		zap.String("eventType", DetermineEventType(info)),
	}
	fields = append(fields, BuildDMLLogFields(info)...)
	fields = append(fields, BuildDDLLogFields(info)...)
	fields = append(fields, BuildCheckpointLogFields(info)...)
	return fields
}
