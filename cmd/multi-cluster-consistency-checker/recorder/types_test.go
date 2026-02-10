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

package recorder

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/stretchr/testify/require"
)

func TestDataLossItem_String(t *testing.T) {
	t.Parallel()

	t.Run("data loss", func(t *testing.T) {
		t.Parallel()
		item := &DataLossItem{
			DownstreamClusterID: "cluster-2",
			PK:                  "pk-1",
			OriginTS:            100,
			CommitTS:            200,
			Inconsistent:        false,
		}
		s := item.String()
		require.Equal(t, "downstream cluster: cluster-2, pk: pk-1, origin ts: 100, commit ts: 200, type: data loss", s)
	})

	t.Run("data inconsistent", func(t *testing.T) {
		t.Parallel()
		item := &DataLossItem{
			DownstreamClusterID: "cluster-3",
			PK:                  "pk-2",
			OriginTS:            300,
			CommitTS:            400,
			Inconsistent:        true,
		}
		s := item.String()
		require.Equal(t, "downstream cluster: cluster-3, pk: pk-2, origin ts: 300, commit ts: 400, type: data inconsistent", s)
	})
}

func TestDataRedundantItem_String(t *testing.T) {
	t.Parallel()
	item := &DataRedundantItem{PK: "pk-x", OriginTS: 10, CommitTS: 20}
	s := item.String()
	require.Equal(t, "pk: pk-x, origin ts: 10, commit ts: 20", s)
}

func TestLWWViolationItem_String(t *testing.T) {
	t.Parallel()
	item := &LWWViolationItem{
		PK:               "pk-y",
		ExistingOriginTS: 1,
		ExistingCommitTS: 2,
		OriginTS:         3,
		CommitTS:         4,
	}
	s := item.String()
	require.Equal(t, "pk: pk-y, existing origin ts: 1, existing commit ts: 2, origin ts: 3, commit ts: 4", s)
}

func TestClusterReport(t *testing.T) {
	t.Parallel()

	t.Run("new cluster report is empty and does not need flush", func(t *testing.T) {
		t.Parallel()
		cr := NewClusterReport("c1")
		require.Equal(t, "c1", cr.ClusterID)
		require.Empty(t, cr.DataLossItems)
		require.Empty(t, cr.DataRedundantItems)
		require.Empty(t, cr.LWWViolationItems)
		require.False(t, cr.needFlush)
	})

	t.Run("add data loss item sets needFlush", func(t *testing.T) {
		t.Parallel()
		cr := NewClusterReport("c1")
		cr.AddDataLossItem("downstream-1", "pk-1", 100, 200, false)
		require.Len(t, cr.DataLossItems, 1)
		require.True(t, cr.needFlush)
		require.Equal(t, "downstream-1", cr.DataLossItems[0].DownstreamClusterID)
		require.Equal(t, "pk-1", cr.DataLossItems[0].PK)
		require.Equal(t, uint64(100), cr.DataLossItems[0].OriginTS)
		require.Equal(t, uint64(200), cr.DataLossItems[0].CommitTS)
		require.False(t, cr.DataLossItems[0].Inconsistent)
	})

	t.Run("add data redundant item sets needFlush", func(t *testing.T) {
		t.Parallel()
		cr := NewClusterReport("c1")
		cr.AddDataRedundantItem("pk-2", 300, 400)
		require.Len(t, cr.DataRedundantItems, 1)
		require.True(t, cr.needFlush)
	})

	t.Run("add lww violation item sets needFlush", func(t *testing.T) {
		t.Parallel()
		cr := NewClusterReport("c1")
		cr.AddLWWViolationItem("pk-3", 1, 2, 3, 4)
		require.Len(t, cr.LWWViolationItems, 1)
		require.True(t, cr.needFlush)
		require.Equal(t, uint64(1), cr.LWWViolationItems[0].ExistingOriginTS)
		require.Equal(t, uint64(2), cr.LWWViolationItems[0].ExistingCommitTS)
		require.Equal(t, uint64(3), cr.LWWViolationItems[0].OriginTS)
		require.Equal(t, uint64(4), cr.LWWViolationItems[0].CommitTS)
	})

	t.Run("add multiple items", func(t *testing.T) {
		t.Parallel()
		cr := NewClusterReport("c1")
		cr.AddDataLossItem("d1", "pk-1", 1, 2, false)
		cr.AddDataLossItem("d2", "pk-2", 3, 4, true)
		cr.AddDataRedundantItem("pk-3", 5, 6)
		cr.AddLWWViolationItem("pk-4", 7, 8, 9, 10)
		require.Len(t, cr.DataLossItems, 2)
		require.Len(t, cr.DataRedundantItems, 1)
		require.Len(t, cr.LWWViolationItems, 1)
	})
}

func TestReport(t *testing.T) {
	t.Parallel()

	t.Run("new report does not need flush", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		require.Equal(t, uint64(1), r.Round)
		require.Empty(t, r.ClusterReports)
		require.False(t, r.NeedFlush())
	})

	t.Run("add empty cluster report does not set needFlush", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		cr := NewClusterReport("c1")
		r.AddClusterReport("c1", cr)
		require.Len(t, r.ClusterReports, 1)
		require.False(t, r.NeedFlush())
	})

	t.Run("add non-empty cluster report sets needFlush", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		cr := NewClusterReport("c1")
		cr.AddDataLossItem("d1", "pk-1", 1, 2, false)
		r.AddClusterReport("c1", cr)
		require.True(t, r.NeedFlush())
	})

	t.Run("needFlush propagates from any cluster report", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		cr1 := NewClusterReport("c1")
		cr2 := NewClusterReport("c2")
		cr2.AddDataRedundantItem("pk-1", 1, 2)
		r.AddClusterReport("c1", cr1)
		r.AddClusterReport("c2", cr2)
		require.True(t, r.NeedFlush())
	})
}

func TestReport_MarshalReport(t *testing.T) {
	t.Parallel()

	t.Run("empty report", func(t *testing.T) {
		t.Parallel()
		r := NewReport(5)
		s := r.MarshalReport()
		require.Equal(t, "round: 5\n\n", s)
	})

	t.Run("report with data loss items", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		cr := NewClusterReport("c1")
		cr.AddDataLossItem("d1", "pk-1", 100, 200, false)
		r.AddClusterReport("c1", cr)
		s := r.MarshalReport()
		require.Equal(t, "round: 1\n\n"+
			"[cluster: c1]\n"+
			"  - [data loss items: 1]\n"+
			"    - [downstream cluster: d1, pk: pk-1, origin ts: 100, commit ts: 200, type: data loss]\n\n",
			s)
	})

	t.Run("report with data redundant items", func(t *testing.T) {
		t.Parallel()
		r := NewReport(2)
		cr := NewClusterReport("c2")
		cr.AddDataRedundantItem("pk-r", 10, 20)
		r.AddClusterReport("c2", cr)
		s := r.MarshalReport()
		require.Equal(t, "round: 2\n\n"+
			"[cluster: c2]\n"+
			"  - [data redundant items: 1]\n"+
			"    - [pk: pk-r, origin ts: 10, commit ts: 20]\n\n",
			s)
	})

	t.Run("report with lww violation items", func(t *testing.T) {
		t.Parallel()
		r := NewReport(3)
		cr := NewClusterReport("c3")
		cr.AddLWWViolationItem("pk-v", 1, 2, 3, 4)
		r.AddClusterReport("c3", cr)
		s := r.MarshalReport()
		require.Equal(t, "round: 3\n\n"+
			"[cluster: c3]\n"+
			"  - [lww violation items: 1]\n"+
			"    - [pk: pk-v, existing origin ts: 1, existing commit ts: 2, origin ts: 3, commit ts: 4]\n\n",
			s)
	})

	t.Run("skips cluster reports that do not need flush", func(t *testing.T) {
		t.Parallel()
		r := NewReport(1)
		crEmpty := NewClusterReport("empty-cluster")
		crFull := NewClusterReport("full-cluster")
		crFull.AddDataLossItem("d1", "pk-1", 1, 2, false)
		r.AddClusterReport("empty-cluster", crEmpty)
		r.AddClusterReport("full-cluster", crFull)
		s := r.MarshalReport()
		require.Equal(t, "round: 1\n\n"+
			"[cluster: full-cluster]\n"+
			"  - [data loss items: 1]\n"+
			"    - [downstream cluster: d1, pk: pk-1, origin ts: 1, commit ts: 2, type: data loss]\n\n",
			s)
	})

	t.Run("report with mixed items", func(t *testing.T) {
		t.Parallel()
		r := NewReport(10)
		cr := NewClusterReport("c1")
		cr.AddDataLossItem("d1", "pk-1", 1, 2, true)
		cr.AddDataRedundantItem("pk-2", 3, 4)
		cr.AddLWWViolationItem("pk-3", 5, 6, 7, 8)
		r.AddClusterReport("c1", cr)
		s := r.MarshalReport()
		require.Equal(t, "round: 10\n\n"+
			"[cluster: c1]\n"+
			"  - [data loss items: 1]\n"+
			"    - [downstream cluster: d1, pk: pk-1, origin ts: 1, commit ts: 2, type: data inconsistent]\n"+
			"  - [data redundant items: 1]\n"+
			"    - [pk: pk-2, origin ts: 3, commit ts: 4]\n"+
			"  - [lww violation items: 1]\n"+
			"    - [pk: pk-3, existing origin ts: 5, existing commit ts: 6, origin ts: 7, commit ts: 8]\n\n",
			s)
	})
}

func TestNewSchemaTableVersionKeyFromVersionKeyMap(t *testing.T) {
	t.Parallel()

	t.Run("empty map", func(t *testing.T) {
		t.Parallel()
		result := NewSchemaTableVersionKeyFromVersionKeyMap(nil)
		require.Empty(t, result)
	})

	t.Run("single entry", func(t *testing.T) {
		t.Parallel()
		m := map[types.SchemaTableKey]types.VersionKey{
			{Schema: "db", Table: "tbl"}: {Version: 1, VersionPath: "path1"},
		}
		result := NewSchemaTableVersionKeyFromVersionKeyMap(m)
		require.Len(t, result, 1)
		require.Equal(t, "db", result[0].Schema)
		require.Equal(t, "tbl", result[0].Table)
		require.Equal(t, uint64(1), result[0].Version)
		require.Equal(t, "path1", result[0].VersionPath)
	})

	t.Run("multiple entries", func(t *testing.T) {
		t.Parallel()
		m := map[types.SchemaTableKey]types.VersionKey{
			{Schema: "db1", Table: "t1"}: {Version: 1},
			{Schema: "db2", Table: "t2"}: {Version: 2},
		}
		result := NewSchemaTableVersionKeyFromVersionKeyMap(m)
		require.Len(t, result, 2)
	})
}

func TestCheckpoint_NewTimeWindowData(t *testing.T) {
	t.Parallel()

	t.Run("first call populates slot 2", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10}},
		})
		require.Nil(t, cp.CheckpointItems[0])
		require.Nil(t, cp.CheckpointItems[1])
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(0), cp.CheckpointItems[2].Round)
	})

	t.Run("second call shifts slots", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10}},
		})
		cp.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"c1": {TimeWindow: types.TimeWindow{LeftBoundary: 10, RightBoundary: 20}},
		})
		require.Nil(t, cp.CheckpointItems[0])
		require.NotNil(t, cp.CheckpointItems[1])
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(0), cp.CheckpointItems[1].Round)
		require.Equal(t, uint64(1), cp.CheckpointItems[2].Round)
	})

	t.Run("third call fills all slots", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		for i := uint64(0); i < 3; i++ {
			cp.NewTimeWindowData(i, map[string]types.TimeWindowData{
				"c1": {TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10}},
			})
		}
		require.NotNil(t, cp.CheckpointItems[0])
		require.NotNil(t, cp.CheckpointItems[1])
		require.NotNil(t, cp.CheckpointItems[2])
		require.Equal(t, uint64(0), cp.CheckpointItems[0].Round)
		require.Equal(t, uint64(1), cp.CheckpointItems[1].Round)
		require.Equal(t, uint64(2), cp.CheckpointItems[2].Round)
	})

	t.Run("fourth call evicts oldest", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		for i := uint64(0); i < 4; i++ {
			cp.NewTimeWindowData(i, map[string]types.TimeWindowData{
				"c1": {TimeWindow: types.TimeWindow{LeftBoundary: i * 10, RightBoundary: (i + 1) * 10}},
			})
		}
		require.Equal(t, uint64(1), cp.CheckpointItems[0].Round)
		require.Equal(t, uint64(2), cp.CheckpointItems[1].Round)
		require.Equal(t, uint64(3), cp.CheckpointItems[2].Round)
	})

	t.Run("stores max version from time window data", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				TimeWindow: types.TimeWindow{LeftBoundary: 1, RightBoundary: 10},
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					{Schema: "db", Table: "tbl"}: {Version: 5, VersionPath: "vp", DataPath: "dp"},
				},
			},
		})
		info := cp.CheckpointItems[2].ClusterInfo["c1"]
		require.Len(t, info.MaxVersion, 1)
		require.Equal(t, uint64(5), info.MaxVersion[0].Version)
		require.Equal(t, "vp", info.MaxVersion[0].VersionPath)
		require.Equal(t, "dp", info.MaxVersion[0].DataPath)
	})
}

func TestCheckpoint_ToScanRange(t *testing.T) {
	t.Parallel()

	stk := types.SchemaTableKey{Schema: "db", Table: "tbl"}

	t.Run("empty checkpoint returns empty", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		result, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("only item[2] set", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 2, VersionPath: "vp2", DataPath: "dp2"},
				},
			},
		})
		result, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Len(t, result, 1)
		sr := result[stk]
		// With only item[2], Start and End are both from item[2]
		require.Equal(t, "vp2", sr.StartVersionKey)
		require.Equal(t, "vp2", sr.EndVersionKey)
		require.Equal(t, "dp2", sr.StartDataPath)
		require.Equal(t, "dp2", sr.EndDataPath)
	})

	t.Run("items[1] and items[2] set", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 1, VersionPath: "vp1", DataPath: "dp1"},
				},
			},
		})
		cp.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 2, VersionPath: "vp2", DataPath: "dp2"},
				},
			},
		})
		result, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Len(t, result, 1)
		sr := result[stk]
		// End comes from item[2], Start overridden by item[1]
		require.Equal(t, "vp1", sr.StartVersionKey)
		require.Equal(t, "vp2", sr.EndVersionKey)
		require.Equal(t, "dp1", sr.StartDataPath)
		require.Equal(t, "dp2", sr.EndDataPath)
	})

	t.Run("all three items set", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		for i := uint64(0); i < 3; i++ {
			cp.NewTimeWindowData(i, map[string]types.TimeWindowData{
				"c1": {
					MaxVersion: map[types.SchemaTableKey]types.VersionKey{
						stk: {
							Version:     i + 1,
							VersionPath: fmt.Sprintf("vp%d", i),
							DataPath:    fmt.Sprintf("dp%d", i),
						},
					},
				},
			})
		}
		result, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Len(t, result, 1)
		sr := result[stk]
		// End from item[2], Start overridden by item[0] (oldest)
		require.Equal(t, "vp0", sr.StartVersionKey)
		require.Equal(t, "vp2", sr.EndVersionKey)
		require.Equal(t, "dp0", sr.StartDataPath)
		require.Equal(t, "dp2", sr.EndDataPath)
	})

	t.Run("missing key in item[1] returns error", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					{Schema: "other", Table: "other"}: {Version: 1, VersionPath: "vp1"},
				},
			},
		})
		cp.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 2, VersionPath: "vp2"},
				},
			},
		})
		_, err := cp.ToScanRange("c1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("missing key in item[0] returns error", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					{Schema: "other", Table: "other"}: {Version: 1, VersionPath: "vp1"},
				},
			},
		})
		cp.NewTimeWindowData(1, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 2, VersionPath: "vp2"},
				},
			},
		})
		cp.NewTimeWindowData(2, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 3, VersionPath: "vp3"},
				},
			},
		})
		_, err := cp.ToScanRange("c1")
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("unknown cluster returns empty", func(t *testing.T) {
		t.Parallel()
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk: {Version: 1, VersionPath: "vp1"},
				},
			},
		})
		result, err := cp.ToScanRange("unknown-cluster")
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("multiple tables", func(t *testing.T) {
		t.Parallel()
		stk2 := types.SchemaTableKey{Schema: "db2", Table: "tbl2"}
		cp := NewCheckpoint()
		cp.NewTimeWindowData(0, map[string]types.TimeWindowData{
			"c1": {
				MaxVersion: map[types.SchemaTableKey]types.VersionKey{
					stk:  {Version: 1, VersionPath: "vp1-t1", DataPath: "dp1-t1"},
					stk2: {Version: 1, VersionPath: "vp1-t2", DataPath: "dp1-t2"},
				},
			},
		})
		result, err := cp.ToScanRange("c1")
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, stk)
		require.Contains(t, result, stk2)
	})
}
