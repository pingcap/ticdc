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
	"encoding/json"
	"strconv"
	"strings"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const (
	maxEventLogRows      = 20
	maxEventLogJSONBytes = 8 * 1024
)

func BuildDMLLogFields(info *common.MessageLogInfo) []zap.Field {
	if info == nil || len(info.Rows) == 0 {
		return nil
	}

	rows := make([]map[string]interface{}, 0, len(info.Rows))
	for _, row := range info.Rows {
		if row.Database == "" && row.Table == "" && row.Type == "" && row.CommitTs == 0 && row.StartTs == 0 && len(row.PrimaryKeys) == 0 {
			continue
		}
		rowMap := map[string]interface{}{
			"type":     row.Type,
			"database": row.Database,
			"table":    row.Table,
		}
		if row.StartTs != 0 {
			rowMap["startTs"] = row.StartTs
		}
		if len(row.PrimaryKeys) > 0 {
			pkMap := make(map[string]interface{}, len(row.PrimaryKeys))
			for _, pk := range row.PrimaryKeys {
				pkMap[pk.Name] = pk.Value
			}
			rowMap["primaryPK"] = pkMap
		}
		if row.CommitTs != 0 {
			rowMap["commitTs"] = row.CommitTs
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

// BuildEventLogContext builds a textual representation of event info.
func BuildEventLogContext(keyspace, changefeed string, info *common.MessageLogInfo) string {
	var sb strings.Builder
	sb.WriteString("keyspace=")
	sb.WriteString(keyspace)
	sb.WriteString(", changefeed=")
	sb.WriteString(changefeed)
	sb.WriteString(", eventType=")
	sb.WriteString(DetermineEventType(info))

	if info == nil {
		return sb.String()
	}

	if len(info.Rows) > 0 {
		if rowsStr, truncated, truncatedRows := formatDMLInfo(info.Rows); rowsStr != "" {
			sb.WriteString(", dmlInfo=")
			sb.WriteString(rowsStr)
			if truncated {
				sb.WriteString(", dmlInfoTruncated=true")
				if truncatedRows > 0 {
					sb.WriteString(", truncatedRows=")
					sb.WriteString(strconv.Itoa(truncatedRows))
				}
				sb.WriteString(", totalRows=")
				sb.WriteString(strconv.Itoa(len(info.Rows)))
			}
		}
	}

	if info.DDL != nil {
		if info.DDL.Query != "" {
			sb.WriteString(", ddlQuery=")
			sb.WriteString(strconv.Quote(info.DDL.Query))
		}
		if info.DDL.CommitTs != 0 {
			sb.WriteString(", ddlCommitTs=")
			sb.WriteString(strconv.FormatUint(info.DDL.CommitTs, 10))
		}
	}

	if info.Checkpoint != nil && info.Checkpoint.CommitTs != 0 {
		sb.WriteString(", checkpointTs=")
		sb.WriteString(strconv.FormatUint(info.Checkpoint.CommitTs, 10))
	}

	return sb.String()
}

// AnnotateEventError appends event context to the error message while keeping the original text intact.
func AnnotateEventError(
	keyspace, changefeed string,
	info *common.MessageLogInfo,
	err error,
) error {
	contextStr := BuildEventLogContext(keyspace, changefeed, info)
	if contextStr == "" || err == nil {
		return err
	}
	return &annotatedEventError{base: err, context: contextStr}
}

type annotatedEventError struct {
	base    error
	context string
}

func (e *annotatedEventError) Error() string {
	switch {
	case e.base == nil:
		return e.context
	case e.context == "":
		return e.base.Error()
	default:
		return e.base.Error() + ": " + e.context
	}
}

func (e *annotatedEventError) Unwrap() error { return e.base }

func (e *annotatedEventError) Cause() error { return e.base }

func formatDMLInfo(rows []common.RowLogInfo) (string, bool, int) {
	if len(rows) == 0 {
		return "", false, 0
	}
	truncated := false
	truncatedRows := 0
	limited := rows
	if len(rows) > maxEventLogRows {
		truncated = true
		truncatedRows = len(rows) - maxEventLogRows
		limited = rows[:maxEventLogRows]
	}
	data, err := json.Marshal(limited)
	if err != nil {
		return "", false, 0
	}
	dataStr := string(data)
	if len(dataStr) > maxEventLogJSONBytes {
		truncated = true
		dataStr = dataStr[:maxEventLogJSONBytes] + "...(truncated)"
	}
	return dataStr, truncated, truncatedRows
}
