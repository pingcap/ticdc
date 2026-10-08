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

package applier

import (
	"fmt"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

func isNonEmptyUniqueOrHandleCol(col commonEvent.RedoColumnValue) bool {
	colFlag := commonType.ColumnFlagType(col.Flag)
	return colFlag.IsUniqueKey() || colFlag.IsHandleKey()
}

// columnValueString returns the string representation of the column value
func columnValueString(c interface{}) string {
	var data string
	switch v := c.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(v, 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(v, 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}
	return data
}

// shouldSplitUpdateEvent determines if the split event is needed to align the old format based on
// whether the handle key column or unique key has been modified.
// If is modified, we need to use splitUpdateEvent to split the update event into a delete and an insert commonEvent.
func shouldSplitUpdateEvent(updateEvent *commonEvent.RedoDMLEvent) bool {
	// nil event will never be split.
	if updateEvent == nil {
		return false
	}

	for i := range updateEvent.Columns {
		col := updateEvent.Columns[i]
		preCol := updateEvent.PreColumns[i]
		if isNonEmptyUniqueOrHandleCol(col) && isNonEmptyUniqueOrHandleCol(preCol) {
			colValueString := columnValueString(col.Value)
			preColValueString := columnValueString(preCol.Value)
			// If one unique key columns is updated, we need to split the event row.
			if colValueString != preColValueString {
				return true
			}
		}
	}
	return false
}

// splitUpdateEvent splits an update event into a delete and an insert commonEvent.
func splitUpdateEvent(
	updateEvent *commonEvent.RedoDMLEvent,
) (*commonEvent.RedoDMLEvent, *commonEvent.RedoDMLEvent, error) {
	if updateEvent == nil {
		return nil, nil, errors.New("nil event cannot be split")
	}

	// If there is an update to handle key columns,
	// we need to split the event into two events to be compatible with the old format.
	// NOTICE: Here we don't need a full deep copy because
	// our two events need Columns and PreColumns respectively,
	// so it won't have an impact and no more full deep copy wastes memory.
	deleteEvent := &commonEvent.RedoDMLEvent{
		PreColumns: updateEvent.PreColumns,
		Row: &commonEvent.DMLEventInRedoLog{
			StartTs:      updateEvent.Row.StartTs,
			CommitTs:     updateEvent.Row.CommitTs,
			Table:        updateEvent.Row.Table,
			PreColumns:   updateEvent.Row.PreColumns,
			IndexColumns: updateEvent.Row.IndexColumns,
		},
	}

	insertEvent := &commonEvent.RedoDMLEvent{
		Columns: updateEvent.Columns,
		Row: &commonEvent.DMLEventInRedoLog{
			StartTs:      updateEvent.Row.StartTs,
			CommitTs:     updateEvent.Row.CommitTs,
			Table:        updateEvent.Row.Table,
			Columns:      updateEvent.Row.Columns,
			IndexColumns: updateEvent.Row.IndexColumns,
		},
	}

	log.Debug("split update event", zap.Uint64("startTs", updateEvent.Row.StartTs),
		zap.Uint64("commitTs", updateEvent.Row.CommitTs),
		zap.String("schema", updateEvent.Row.Table.Schema),
		zap.String("table", updateEvent.Row.Table.Table),
		zap.Any("preCols", updateEvent.PreColumns),
		zap.Any("cols", updateEvent.Columns))

	return deleteEvent, insertEvent, nil
}

// Share one restorer across all redo rows so the spill backlog does not retain
// a decoder closure or a materialized DML event for each row.
var redoDMLMessageRestorer = codeccommon.NewDMLMessageRestorer(func(data []byte) ([]*codeccommon.DMLMessage, error) {
	_, value, err := util.UnmarshalDMLMessageData(data)
	if err != nil {
		return nil, err
	}
	row := new(commonEvent.RedoDMLEvent)
	if _, err := row.UnmarshalMsg(value); err != nil {
		return nil, errors.WrapError(errors.ErrUnmarshalFailed, err)
	}
	return []*codeccommon.DMLMessage{newRedoDMLMessage(row)}, nil
})

func newRedoDMLMessage(row *commonEvent.RedoDMLEvent) *codeccommon.DMLMessage {
	rowType := commonType.RowTypeInsert
	if row.IsDelete() {
		rowType = commonType.RowTypeDelete
	} else if row.IsUpdate() {
		rowType = commonType.RowTypeUpdate
	}
	return codeccommon.NewDMLMessage(row.Row.Table.TableID, row.Row.Table.Schema,
		row.Row.Table.Table, row.Row.CommitTs, rowType, row.ToDMLEvent)
}

type ddlTs struct {
	ts      int64
	skipDML bool
}
