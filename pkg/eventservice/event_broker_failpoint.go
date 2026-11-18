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

package eventservice

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// These helpers implement the bounded scheduling fault used by the syncpoint
// regression test. The failpoint.Inject sites stay in event_broker.go at the
// actual scan decisions, so normal builds do not call these helpers. The same
// fault can be applied to master and the fix: it delays selected scans only;
// it never changes the scan cursor, memory quota, or barrier state.
//
// DelayTableScanUntil uses "tableID:deadlineMillis" for the slow table's
// active transaction cursor. DelayChangefeedScanUntil uses
// "changefeedName:slowTableID:deadlineMillis" to delay other tables, including
// idle scans. DelayCursorRoundsUntil uses
// "changefeedName:slowTableID:firstDeadlineMillis:secondDeadlineMillis" to
// hold two distinct cursor positions of the fast tables. Absolute deadlines
// bound each fault even if the test process loses control.

// Cursor state is keyed by scan task so independently dispatched tables do
// not share round numbers. It is removed when the deadline expires or the
// active transaction cursor ends.
var testCursorPauseStates sync.Map

type testCursorPauseState struct {
	sync.Mutex
	position string
	count    int
}

func testShouldDelayCursorRound(task scanTask, specification string) bool {
	parts := strings.SplitN(specification, ":", 4)
	if len(parts) != 4 || task.changefeedStat.changefeedID.Name() != parts[0] {
		return false
	}
	slowTableID, e0 := strconv.ParseInt(parts[1], 10, 64)
	firstUntil, e1 := strconv.ParseInt(parts[2], 10, 64)
	secondUntil, e2 := strconv.ParseInt(parts[3], 10, 64)
	if e0 != nil || e1 != nil || e2 != nil || task.info.GetTableSpan().TableID == slowTableID {
		return false
	}
	if time.Now().UnixMilli() >= secondUntil {
		testCursorPauseStates.Delete(task)
		return false
	}
	progress := task.loadScanProgress()
	if progress.txnStartTs == 0 {
		testCursorPauseStates.Delete(task)
		return false
	}
	valueState, _ := testCursorPauseStates.LoadOrStore(task, &testCursorPauseState{})
	state := valueState.(*testCursorPauseState)
	state.Lock()
	position := string(progress.rowLevelScanPosition)
	changed := state.count == 0 || position != state.position
	if changed {
		state.count++
		state.position = position
	}
	count := state.count
	state.Unlock()
	until := secondUntil
	if count == 1 {
		until = firstUntil
	}
	if changed {
		log.Info("test cursor scheduling delay",
			zap.String("keyspace", task.changefeedStat.changefeedID.Keyspace()),
			zap.String("changefeedID", task.changefeedStat.changefeedID.Name()),
			zap.Stringer("dispatcherID", task.id),
			zap.Int64("tableID", task.info.GetTableSpan().TableID),
			zap.Int("cursorRound", count),
			zap.Int64("untilMillis", until))
	}
	return time.Now().UnixMilli() < until
}

func testShouldDelayChangefeedScan(task scanTask, specification string) bool {
	parts := strings.SplitN(specification, ":", 3)
	if len(parts) != 3 || task.changefeedStat.changefeedID.Name() != parts[0] {
		return false
	}
	slowTableID, tableErr := strconv.ParseInt(parts[1], 10, 64)
	until, timeErr := strconv.ParseInt(parts[2], 10, 64)
	return tableErr == nil && timeErr == nil && task.info.GetTableSpan().TableID != slowTableID && time.Now().UnixMilli() < until
}

func testShouldDelayTableScan(task scanTask, specification string) bool {
	parts := strings.SplitN(specification, ":", 2)
	if len(parts) != 2 {
		return false
	}
	tableID, tableErr := strconv.ParseInt(parts[0], 10, 64)
	until, timeErr := strconv.ParseInt(parts[1], 10, 64)
	return tableErr == nil && timeErr == nil && task.info.GetTableSpan().TableID == tableID &&
		task.loadScanProgress().txnStartTs != 0 && time.Now().UnixMilli() < until
}
