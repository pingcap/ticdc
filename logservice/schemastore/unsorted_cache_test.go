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

package schemastore

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	cache := newDDLCache()
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 1,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 100,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 50,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 30,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 40,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 9,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 15,
		Job: &model.Job{
			BinlogInfo: &model.HistoryInfo{
				SchemaVersion: 1,
			},
		},
	})
	events := cache.fetchSortedDDLEventBeforeTS(30)
	require.Equal(t, len(events), 4)
	require.Equal(t, events[0].CommitTs, uint64(1))
	require.Equal(t, events[1].CommitTs, uint64(9))
	require.Equal(t, events[2].CommitTs, uint64(15))
	require.Equal(t, events[3].CommitTs, uint64(30))
	events = cache.fetchSortedDDLEventBeforeTS(50)
	require.Equal(t, len(events), 2)
	require.Equal(t, events[0].CommitTs, uint64(40))
	require.Equal(t, events[1].CommitTs, uint64(50))
}

func TestDDLCacheOrdersAndDeduplicatesSameCommitTs(t *testing.T) {
	cache := newDDLCache()
	newEvent := func(schemaVersion, jobID int64) DDLJobWithCommitTs {
		return DDLJobWithCommitTs{
			CommitTs: 100,
			Job: &model.Job{
				ID: jobID,
				BinlogInfo: &model.HistoryInfo{
					SchemaVersion: schemaVersion,
				},
			},
		}
	}

	cache.addDDLEvent(newEvent(3, 3))
	cache.addDDLEvent(newEvent(1, 1))
	cache.addDDLEvent(newEvent(2, 2))
	// The B-tree key is commit-ts plus schema version, so this replaces the
	// previous schema-version-2 event rather than adding another event.
	cache.addDDLEvent(newEvent(2, 20))

	events := cache.fetchSortedDDLEventBeforeTS(100)
	require.Len(t, events, 3)
	require.Equal(t, []int64{1, 2, 3}, []int64{
		events[0].Job.BinlogInfo.SchemaVersion,
		events[1].Job.BinlogInfo.SchemaVersion,
		events[2].Job.BinlogInfo.SchemaVersion,
	})
	require.Equal(t, int64(20), events[1].Job.ID)
	require.Empty(t, cache.fetchSortedDDLEventBeforeTS(100))
}

func TestDDLCacheUsesCommitTsForOrderingAndFetchBoundary(t *testing.T) {
	cache := newDDLCache()
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 20,
		Job: &model.Job{BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 1,
			FinishedTS:    1,
		}},
	})
	cache.addDDLEvent(DDLJobWithCommitTs{
		CommitTs: 10,
		Job: &model.Job{BinlogInfo: &model.HistoryInfo{
			SchemaVersion: 2,
			FinishedTS:    100,
		}},
	})

	events := cache.fetchSortedDDLEventBeforeTS(10)
	require.Len(t, events, 1)
	require.Equal(t, uint64(10), events[0].CommitTs)
	require.Equal(t, uint64(100), events[0].Job.BinlogInfo.FinishedTS)

	events = cache.fetchSortedDDLEventBeforeTS(20)
	require.Len(t, events, 1)
	require.Equal(t, uint64(20), events[0].CommitTs)
}
