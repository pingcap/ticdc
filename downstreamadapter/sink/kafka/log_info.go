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
	commonPkg "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

func attachMessageLogInfo(messages []*common.Message, events []*commonEvent.RowEvent) {
	if len(messages) == 0 || len(events) == 0 {
		return
	}

	eventIdx := 0
	for _, message := range messages {
		rowsNeeded := message.GetRowsCount()
		if rowsNeeded <= 0 {
			rowsNeeded = len(events) - eventIdx
		}
		if rowsNeeded <= 0 {
			message.LogInfo = nil
			continue
		}
		end := eventIdx + rowsNeeded
		if end > len(events) {
			end = len(events)
		}
		if end <= eventIdx {
			message.LogInfo = nil
			continue
		}
		message.LogInfo = buildMessageLogInfo(events[eventIdx:end])
		eventIdx = end
		if eventIdx >= len(events) {
			break
		}
	}
}

func buildMessageLogInfo(events []*commonEvent.RowEvent) *common.MessageLogInfo {
	rows := make([]common.RowLogInfo, 0, len(events))
	for _, event := range events {
		if event == nil || event.TableInfo == nil {
			continue
		}
		rowInfo := common.RowLogInfo{
			Type:     rowEventType(event),
			Database: event.TableInfo.GetSchemaName(),
			Table:    event.TableInfo.GetTableName(),
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
	return &common.MessageLogInfo{Rows: rows}
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

func extractPrimaryKeys(event *commonEvent.RowEvent) []common.ColumnLogInfo {
	indexes, columns := event.PrimaryKeyColumn()
	if len(columns) == 0 {
		return nil
	}
	row := event.GetRows()
	if event.IsDelete() {
		row = event.GetPreRows()
	}
	if row == nil {
		return nil
	}

	values := make([]common.ColumnLogInfo, 0, len(columns))
	for i, col := range columns {
		if col == nil {
			continue
		}
		value := commonPkg.ExtractColVal(row, col, indexes[i])
		values = append(values, common.ColumnLogInfo{
			Name:  col.Name.String(),
			Value: value,
		})
	}
	return values
}
