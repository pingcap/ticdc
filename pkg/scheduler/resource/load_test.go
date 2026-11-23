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

package resource

import (
	"math"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestResourceLoadProtocol(t *testing.T) {
	// Unknown positive resource types remain separate and round-trip so that
	// the view and screening policy do not need a new branch for each metric.
	expected := Load{Sink: 100, EventStoreWrite: 300, EventStoreRead: 500, Type(100): 700}
	encoded, err := expected.ToNodeLoad("node").Marshal()
	require.NoError(t, err)
	decoded := &heartbeatpb.NodeLoad{}
	require.NoError(t, decoded.Unmarshal(encoded))
	rates, valid := Rates(decoded)
	require.True(t, valid)
	require.Equal(t, expected, rates)
	require.Equal(t, float64(100), decoded.BytesPerSecond, "legacy readers still see sink rate")

	before, valid := Counters(Snapshot())
	require.True(t, valid)
	SinkBytes.Add(10)
	EventStoreWriteBytes.Add(20)
	EventStoreReadBytes.Add(30)
	snapshot := Snapshot()
	after, valid := Counters(snapshot)
	require.True(t, valid)
	require.Equal(t, before[Sink]+10, after[Sink])
	require.Equal(t, before[EventStoreWrite]+20, after[EventStoreWrite])
	require.Equal(t, before[EventStoreRead]+30, after[EventStoreRead])
	require.Equal(t, after[Sink], snapshot.FlushedEventBytes)
	again, valid := Counters(Snapshot())
	require.True(t, valid)
	require.Equal(t, after, again, "sampling must not reset process counters")

	rates, valid = Rates(&heartbeatpb.NodeLoad{BytesPerSecond: 42})
	require.True(t, valid)
	require.Equal(t, Load{Sink: 42}, rates)
	counters, valid := Counters(&heartbeatpb.NodeLoad{FlushedEventBytes: 99})
	require.True(t, valid)
	require.Equal(t, map[Type]uint64{Sink: 99}, counters)
}

func TestResourceLoadRejectsInvalidVectors(t *testing.T) {
	for _, load := range []*heartbeatpb.NodeLoad{
		nil,
		{Resources: []*heartbeatpb.ResourceUsage{nil}},
		{Resources: []*heartbeatpb.ResourceUsage{{Resource: heartbeatpb.ResourceType_RESOURCE_UNKNOWN}}},
		{Resources: []*heartbeatpb.ResourceUsage{{Resource: Sink}, {Resource: Sink}}},
		{Resources: []*heartbeatpb.ResourceUsage{{Resource: EventStoreRead}}},
	} {
		_, valid := Rates(load)
		require.False(t, valid)
		_, valid = Counters(load)
		require.False(t, valid)
	}
	for _, rate := range []float64{-1, math.NaN(), math.Inf(1)} {
		for _, load := range []*heartbeatpb.NodeLoad{
			{BytesPerSecond: rate},
			{Resources: []*heartbeatpb.ResourceUsage{{Resource: Sink}, {Resource: EventStoreRead, BytesPerSecond: rate}}},
		} {
			_, valid := Rates(load)
			require.False(t, valid)
		}
	}
}
