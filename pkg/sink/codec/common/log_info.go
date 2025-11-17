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

package common

import (
	"errors"

	perrors "github.com/pingcap/errors"
	commonPkg "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// AttachMessageLogInfo binds row event diagnostic info onto sink messages.
func AttachMessageLogInfo(messages []*Message, events []*commonEvent.RowEvent) error {
	if len(messages) == 0 || len(events) == 0 {
		return nil
	}

	eventIdx := 0
	var mismatchErr error
	for idx, message := range messages {
		rowsNeeded := message.GetRowsCount()
		remaining := len(events) - eventIdx
		if rowsNeeded > remaining {
			mismatchErr = annotateMismatchError(mismatchErr, "messageIndex=%d rowsNeeded=%d remaining=%d", idx, rowsNeeded, remaining)
			rowsNeeded = remaining
		}
		if rowsNeeded <= 0 {
			message.LogInfo = nil
			continue
		}
		message.LogInfo = buildMessageLogInfo(events[eventIdx : eventIdx+rowsNeeded])
		eventIdx += rowsNeeded
		if eventIdx >= len(events) {
			break
		}
	}
	if eventIdx != len(events) {
		mismatchErr = annotateMismatchError(mismatchErr, "eventsRemaining=%d totalEvents=%d", len(events)-eventIdx, len(events))
	}
	return mismatchErr
}

func annotateMismatchError(existing error, format string, args ...interface{}) error {
	if existing == nil {
		return perrors.Annotatef(errors.New("message rows count mismatches row events"), format, args...)
	}
	return perrors.Annotatef(existing, format, args...)
}

func buildMessageLogInfo(events []*commonEvent.RowEvent) *MessageLogInfo {
	rows := make([]RowLogInfo, 0, len(events))
	for _, event := range events {
		if event == nil || event.TableInfo == nil {
			continue
		}
		rowInfo := RowLogInfo{
			Type:     rowEventType(event),
			Database: event.TableInfo.GetSchemaName(),
			Table:    event.TableInfo.GetTableName(),
			StartTs:  event.StartTs,
			CommitTs: event.CommitTs,
		}
		if pk := extractPrimaryKeys(event); len(pk) > 0 {
			rowInfo.PrimaryKeys = pk
		}
		rows = append(rows, rowInfo)
	}
	if len(rows) == 0 {
		return nil
	}
	return &MessageLogInfo{Rows: rows}
}

func rowEventType(event *commonEvent.RowEvent) string {
	switch {
	case event.IsInsert():
		return "insert"
	case event.IsUpdate():
		return "update"
	case event.IsDelete():
		return "delete"
	default:
		return "unknown"
	}
}

func extractPrimaryKeys(event *commonEvent.RowEvent) []ColumnLogInfo {
	indexes, columns := event.PrimaryKeyColumn()
	if len(columns) == 0 {
		return nil
	}
	row := event.GetRows()
	if event.IsDelete() {
		row = event.GetPreRows()
	}

	values := make([]ColumnLogInfo, 0, len(columns))
	for i, col := range columns {
		if col == nil {
			continue
		}
		value := commonPkg.ExtractColVal(row, col, indexes[i])
		values = append(values, ColumnLogInfo{
			Name:  col.Name.String(),
			Value: value,
		})
	}
	return values
}

// SetDDLMessageLogInfo attaches DDL diagnostic info on message.
func SetDDLMessageLogInfo(msg *Message, event *commonEvent.DDLEvent) {
	if msg == nil || event == nil {
		return
	}
	logInfo := msg.LogInfo
	if logInfo == nil {
		logInfo = &MessageLogInfo{}
	}
	logInfo.DDL = &DDLLogInfo{
		Query:    event.Query,
		CommitTs: event.GetCommitTs(),
	}
	msg.LogInfo = logInfo
}

// SetCheckpointMessageLogInfo attaches checkpoint info on message.
func SetCheckpointMessageLogInfo(msg *Message, commitTs uint64) {
	if msg == nil {
		return
	}
	logInfo := msg.LogInfo
	if logInfo == nil {
		logInfo = &MessageLogInfo{}
	}
	logInfo.Checkpoint = &CheckpointLogInfo{
		CommitTs: commitTs,
	}
	msg.LogInfo = logInfo
}
