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

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

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
		if rowsStr := formatDMLInfo(info.Rows); rowsStr != "" {
			sb.WriteString(", dmlInfo=")
			sb.WriteString(rowsStr)
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

// AnnotateEventError logs the event context and annotates the error with that context.
func AnnotateEventError(
	keyspace, changefeed string,
	info *common.MessageLogInfo,
	err error,
) error {
	if err == nil {
		return nil
	}
	if contextStr := BuildEventLogContext(keyspace, changefeed, info); contextStr != "" {
		return errors.Annotate(err, contextStr+"; ErrorInfo:"+err.Error())
	}
	return err
}

func formatDMLInfo(rows []common.RowLogInfo) string {
	data, err := json.Marshal(rows)
	if err != nil {
		return ""
	}
	return string(data)
}
