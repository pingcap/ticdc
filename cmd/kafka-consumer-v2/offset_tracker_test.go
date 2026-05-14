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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestOffsetTrackerOnlyCommitsContiguousOffsets(t *testing.T) {
	tracker := newOffsetTracker()

	source10 := tracker.NewSource("topic", 0, kafka.Offset(10))
	source11 := tracker.NewSource("topic", 0, kafka.Offset(11))
	source12 := tracker.NewSource("topic", 0, kafka.Offset(12))

	source11.Close()
	require.Empty(t, tracker.DrainCommittable())

	source10.Close()
	committable := tracker.DrainCommittable()
	require.Len(t, committable, 1)
	require.Equal(t, int32(0), committable[0].Partition)
	require.Equal(t, kafka.Offset(12), committable[0].Offset)

	source12.AddWork()
	source12.Close()
	require.Empty(t, tracker.DrainCommittable())

	source12.Done()
	committable = tracker.DrainCommittable()
	require.Len(t, committable, 1)
	require.Equal(t, kafka.Offset(13), committable[0].Offset)
}
