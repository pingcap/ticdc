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
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestResolveAffectedDispatchersByBlockedTables(t *testing.T) {
	dispatcher1 := commonType.NewDispatcherID()
	dispatcher2 := commonType.NewDispatcherID()
	dispatcher3 := commonType.NewDispatcherID()

	workers := &dmlWriters{
		dispatcherTableIDs: make(map[commonType.DispatcherID]int64),
		tableDispatchers:   make(map[int64]map[commonType.DispatcherID]struct{}),
	}
	workers.recordDispatcherTable(dispatcher1, 101)
	workers.recordDispatcherTable(dispatcher2, 201)
	workers.recordDispatcherTable(dispatcher3, 102)

	tableStore := commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{
		{
			SchemaID: 1,
			Tables: []*heartbeatpb.TableInfo{
				{TableID: 101},
				{TableID: 102},
			},
		},
		{
			SchemaID: 2,
			Tables: []*heartbeatpb.TableInfo{
				{TableID: 201},
			},
		},
	}, commonType.CloudStorageSinkType, false)

	testCases := []struct {
		name      string
		blocked   *commonEvent.InfluencedTables
		expected  []commonType.DispatcherID
		useSchema bool
	}{
		{
			name: "normal blocked tables",
			blocked: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeNormal,
				TableIDs:      []int64{101},
			},
			expected: []commonType.DispatcherID{dispatcher1},
		},
		{
			name: "db blocked tables",
			blocked: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeDB,
				SchemaID:      1,
			},
			expected:  []commonType.DispatcherID{dispatcher1, dispatcher3},
			useSchema: true,
		},
		{
			name: "all blocked tables",
			blocked: &commonEvent.InfluencedTables{
				InfluenceType: commonEvent.InfluenceTypeAll,
			},
			expected:  []commonType.DispatcherID{dispatcher1, dispatcher2, dispatcher3},
			useSchema: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ddl := &commonEvent.DDLEvent{
				DispatcherID: dispatcher1,
				FinishedTs:   100,
				BlockedTables: &commonEvent.InfluencedTables{
					InfluenceType: tc.blocked.InfluenceType,
					TableIDs:      tc.blocked.TableIDs,
					SchemaID:      tc.blocked.SchemaID,
				},
			}

			var store *commonEvent.TableSchemaStore
			if tc.useSchema {
				store = tableStore
			}

			affected := workers.resolveAffectedDispatchers(ddl, store)
			require.ElementsMatch(t, tc.expected, affected)
		})
	}
}
