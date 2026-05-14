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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestGetLatencySnapshotUsesGlobalResolvedWatermark(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	engine := &replayEngine{
		partitions: []*partitionState{
			{partition: 0, watermark: oracle.GoTimeToTS(now.Add(-10 * time.Second))},
			{partition: 1, watermark: oracle.GoTimeToTS(now.Add(-30 * time.Second))},
		},
	}

	snapshot := engine.getLatencySnapshot(now)
	require.Equal(t, 2, snapshot.partitionCount)
	require.Equal(t, 2, snapshot.initializedPartitions)
	require.Equal(t, int32(1), snapshot.slowestPartition)
	require.Equal(t, 30*time.Second, snapshot.slowestLag)
	require.Equal(t, 30*time.Second, snapshot.globalLag)
	require.Equal(t, engine.partitions[1].watermark, snapshot.globalWatermark)
}

func TestGetLatencySnapshotWaitsForAllPartitions(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	engine := &replayEngine{
		partitions: []*partitionState{
			{partition: 0, watermark: oracle.GoTimeToTS(now.Add(-10 * time.Second))},
			{partition: 1},
		},
	}

	snapshot := engine.getLatencySnapshot(now)
	require.Equal(t, 2, snapshot.partitionCount)
	require.Equal(t, 1, snapshot.initializedPartitions)
	require.Zero(t, snapshot.globalWatermark)
	require.Equal(t, int32(0), snapshot.slowestPartition)
	require.Equal(t, 10*time.Second, snapshot.slowestLag)
}
