// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"sort"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

func groupEventsByTable(events []*commonEvent.DMLEvent) map[int64][][]*commonEvent.DMLEvent {
	// group the events by table ID and updateTs
	eventsGroup := make(map[int64]map[uint64][]*commonEvent.DMLEvent) // tableID --> updateTs --> events
	for _, event := range events {
		tableID := event.GetTableID()
		updateTs := event.TableInfo.GetUpdateTS()

		if _, ok := eventsGroup[tableID]; !ok {
			eventsGroup[tableID] = make(map[uint64][]*commonEvent.DMLEvent)
		}
		eventsGroup[tableID][updateTs] = append(eventsGroup[tableID][updateTs], event)
	}

	// sorted by updateTs for each tableID
	eventsGroupSortedByUpdateTs := make(map[int64][][]*commonEvent.DMLEvent)

	for tableID, updateTsMap := range eventsGroup {
		// Collect all updateTs keys and sort them
		var updateTsKeys []uint64
		for updateTs := range updateTsMap {
			updateTsKeys = append(updateTsKeys, updateTs)
		}

		sort.Slice(updateTsKeys, func(i, j int) bool {
			return updateTsKeys[i] < updateTsKeys[j]
		})

		// Create sorted events array for this tableID
		var sortedEvents [][]*commonEvent.DMLEvent
		for _, updateTs := range updateTsKeys {
			sortedEvents = append(sortedEvents, updateTsMap[updateTs])
		}
		eventsGroupSortedByUpdateTs[tableID] = sortedEvents
	}
	return eventsGroupSortedByUpdateTs
}

// for multiple events, we try to batch the events of the same table into limited update / insert / delete query,
// to enhance the performance of the sink.
// While we only support to batch the events with pks, and all the events inSafeMode or all not in inSafeMode.
// the process is as follows:
//  1. we group the events by tableID, and hold the order for the events of the same table
//  2. For each group,
//     if the table does't have a handle key or have virtual column, we just generate the sqls for each event row.
//     Otherwise,
//     if there is only one rows of the whole group, we generate the sqls for the row.
//     Otherwise, we batch all the event rows for the same dispatcherID to limited delete / update/ insert query(in order)
func (w *Writer) prepareDMLs(events []*commonEvent.DMLEvent) (*preparedDMLs, error) {
	dmls := dmlsPool.Get().(*preparedDMLs)
	dmls.reset()

	// calculate metrics
	for _, event := range events {
		dmls.rowCount += int(event.Len())
		if len(dmls.tsPairs) == 0 || dmls.tsPairs[len(dmls.tsPairs)-1].startTs != event.StartTs {
			dmls.tsPairs = append(dmls.tsPairs, tsPair{startTs: event.StartTs, commitTs: event.CommitTs})
		}
		dmls.approximateSize += event.GetSize()
	}

	// Step 1: group the events by table ID and updateTs
	eventsGroupSortedByUpdateTs := groupEventsByTable(events)

	// Step 2: prepare the dmls for each group
	var (
		queryList []string
		argsList  [][]interface{}
	)
	for _, sortedEventGroups := range eventsGroupSortedByUpdateTs {
		for _, eventsInGroup := range sortedEventGroups {
			tableInfo := eventsInGroup[0].TableInfo
			if w.cfg.EnableActiveActive {
				queryList, argsList = w.genActiveActiveSQL(tableInfo, eventsInGroup)
			} else {
				if !w.shouldGenBatchSQL(tableInfo, eventsInGroup) {
					queryList, argsList = w.generateNormalSQLs(eventsInGroup)
				} else {
					queryList, argsList = w.generateBatchSQL(eventsInGroup)
				}
			}
			dmls.sqls = append(dmls.sqls, queryList...)
			dmls.values = append(dmls.values, argsList...)
		}
	}
	// Pre-check log level to avoid dmls.String() being called unnecessarily
	// This method is expensive, so we only log it when the log level is debug.

	dmls.LogDebug(events, w.id)

	return dmls, nil
}

func (w *Writer) genActiveActiveSQL(tableInfo *common.TableInfo, eventsInGroup []*commonEvent.DMLEvent) ([]string, [][]interface{}) {
	if !w.shouldGenBatchSQL(tableInfo, eventsInGroup) {
		return w.generateActiveActiveNormalSQLs(eventsInGroup)
	}
	return w.generateActiveActiveBatchSQL(eventsInGroup)
}

// shouldGenBatchSQL determines whether batch SQL generation should be used based on table properties and events.
// Batch SQL generation is used when:
// 1. BatchDMLEnable = true, and rows > 1
// 2. The table has a pk or not null unique key
// 3. The handle key contains no virtual generated columns
// 4. There's more than one row in the group
// 5. All events have the same safe mode status
func (w *Writer) shouldGenBatchSQL(tableInfo *common.TableInfo, events []*commonEvent.DMLEvent) bool {
	if !w.cfg.BatchDMLEnable {
		return false
	}

	if !tableInfo.HasPKOrNotNullUK {
		return false
	}
	// if the table has pk or uk, but the handle key contains virtual generated columns,
	// we can't batch the events by pk or uk,
	// because the value of the virtual generated column is calculated by other column,
	// and we can't guarantee the value of the virtual generated column is the same for the same pk or uk.
	colIDs := tableInfo.GetOrderedHandleKeyColumnIDs()
	for _, colID := range colIDs {
		info, exist := tableInfo.GetColumnInfo(colID)
		if !exist {
			continue
		}
		if info.IsVirtualGenerated() {
			return false
		}
	}
	// if tableInfo.HasVirtualColumns()
	if len(events) == 1 && events[0].Len() == 1 {
		return false
	}

	return allRowInSameSafeMode(w.cfg.SafeMode, events)
}

// allRowInSameSafeMode determines whether all DMLEvents in a batch have the same safe mode status.
// Safe mode is either globally enabled via the safemode parameter, or determined per event
// by comparing CommitTs and ReplicatingTs.
//
// Parameters:
//   - safemode: If true, global safe mode is enabled and the function returns true immediately
//   - events: A slice of DMLEvents to check for consistent safe mode status
//
// Returns:
//
//	true if either:
//	- global safe mode is enabled (safemode=true), or
//	- all events have the same safe mode status (all events' CommitTs > ReplicatingTs, or all ≤)
//	false if events have inconsistent safe mode status
func allRowInSameSafeMode(safemode bool, events []*commonEvent.DMLEvent) bool {
	if safemode {
		return true
	}

	if len(events) == 0 {
		return false
	}

	firstSafeMode := events[0].CommitTs > events[0].ReplicatingTs
	for _, event := range events {
		currentSafeMode := event.CommitTs > event.ReplicatingTs
		if currentSafeMode != firstSafeMode {
			return false
		}
	}

	return true
}
