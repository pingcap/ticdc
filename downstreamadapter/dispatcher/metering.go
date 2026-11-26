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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatcher

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const unknownSQLTypeLogIntervalSeconds int64 = 60

var lastUnknownSQLTypeLogUnix atomic.Int64

// measureDMLEventV1 computes the canonical effective_bytes for all logical rows
// in event matching selector, and rewinds event so downstream sinks read from
// the first row. Unknown SQL column types are counted as 0 bytes (fail-open)
// while returning the first encountered error for diagnostics.
func measureDMLEventV1(event *commonEvent.DMLEvent, selector commonEvent.Selector) (uint64, error) {
	if event == nil || event.TableInfo == nil || event.Rows == nil || event.Len() == 0 {
		return 0, nil
	}
	defer event.Rewind()

	cols := event.TableInfo.GetColumns()
	if len(cols) == 0 {
		return 0, nil
	}

	var total uint64
	var firstErr error
	addRow := func(row chunk.Row) {
		n, err := measureRowV1(row, cols, selector)
		total += n
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for {
		rowChange, ok := event.GetNextRow()
		if !ok {
			break
		}
		switch rowChange.RowType {
		case common.RowTypeInsert:
			addRow(rowChange.Row)
		case common.RowTypeDelete:
			addRow(rowChange.PreRow)
		case common.RowTypeUpdate:
			addRow(rowChange.PreRow)
			addRow(rowChange.Row)
		}
	}
	return total, firstErr
}

func measureRowV1(row chunk.Row, cols []*model.ColumnInfo, selector commonEvent.Selector) (uint64, error) {
	if row.IsEmpty() {
		return 0, nil
	}
	var total uint64
	var firstErr error
	for idx, col := range cols {
		if col == nil || col.IsVirtualGenerated() {
			continue
		}
		if selector != nil && !selector.Select(col) {
			continue
		}
		n, err := metering.MeasureValueV1(row, idx, col.GetType())
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		total += n
	}
	return total, firstErr
}

func recordMeteringMeasureError(changefeedID common.ChangeFeedID, err error) {
	if err == nil {
		return
	}
	reason := "value_error"
	if errors.Is(err, metering.ErrUnsupportedSQLType) {
		reason = "unsupported_type"
	}
	meteringMeasureErrorCounter.WithLabelValues(reason).Inc()
	now := time.Now().Unix()
	prev := lastUnknownSQLTypeLogUnix.Load()
	if now-prev >= unknownSQLTypeLogIntervalSeconds && lastUnknownSQLTypeLogUnix.CompareAndSwap(prev, now) {
		log.Warn("traffic metering column measurement error, treating column as 0 bytes",
			zap.Stringer("changefeedID", changefeedID),
			zap.String("reason", reason),
			zap.Error(err))
	}
}
