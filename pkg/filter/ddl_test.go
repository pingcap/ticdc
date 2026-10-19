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

package filter

import (
	"testing"

	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/stretchr/testify/require"
)

func TestSingleTableDDL(t *testing.T) {
	for d := range singleTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is not in the white list", d)
	}
	for d := range multiTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is not in the white list", d)
	}
	for d := range globalTableDDLs {
		_, ok := ddlWhiteListMap[d]
		require.True(t, ok, "DDL %s is in the white list", d)
	}
	require.Equal(t, 41, len(ddlWhiteListMap))
}

// TestWhitelistedDDLEventsUsable checks the invariant that every whitelisted ddl
// event type is usable by the event filter. `ddlToEventType` passes these event
// types to `BinlogEvent.Filter`, so an event type that cannot be classified by
// binlog-filter would fail the event scan of the table, and an event type that
// is rejected by `toEventType` cannot be written in `ignore-event`.
func TestWhitelistedDDLEventsUsable(t *testing.T) {
	for ddlType, eventType := range ddlWhiteListMap {
		require.NotEqual(t, bf.NullEvent, eventType, "ddl %s", ddlType.String())

		require.NotEqual(t, bf.NullEvent, bf.ClassifyEvent(eventType),
			"ddl %s maps to unknown event type %s", ddlType.String(), eventType)

		require.NoError(t, verifyIgnoreEvents([]bf.EventType{eventType}),
			"event type %s must be accepted by ignore-event", eventType)

		_, err := bf.NewBinlogEvent(false, []*bf.BinlogEventRule{{
			SchemaPattern: "test",
			TablePattern:  "t",
			Events:        []bf.EventType{eventType},
			Action:        bf.Ignore,
		}})
		require.NoError(t, err, "event type %s must be usable in a binlog event rule", eventType)
	}
}
