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

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
)

func TestOffsetCommitBufferMergesByPartition(t *testing.T) {
	now := time.Now()
	buffer := newOffsetCommitBuffer(now)
	topic := "topic"

	buffer.Add([]kafka.TopicPartition{
		{Topic: &topic, Partition: 1, Offset: 10},
		{Topic: &topic, Partition: 0, Offset: 8},
		{Topic: &topic, Partition: 1, Offset: 9},
		{Topic: &topic, Partition: 0, Offset: 12},
	})

	offsets := buffer.Offsets()
	require.Len(t, offsets, 2)
	require.Equal(t, int32(0), offsets[0].Partition)
	require.Equal(t, kafka.Offset(12), offsets[0].Offset)
	require.Equal(t, int32(1), offsets[1].Partition)
	require.Equal(t, kafka.Offset(10), offsets[1].Offset)
	require.False(t, buffer.ShouldFlush(now.Add(offsetCommitInterval-time.Millisecond)))
	require.True(t, buffer.ShouldFlush(now.Add(offsetCommitInterval)))

	buffer.MarkCommitted(now.Add(offsetCommitInterval))
	require.Empty(t, buffer.Offsets())
	require.False(t, buffer.ShouldFlush(now.Add(2*offsetCommitInterval)))
}

func TestOffsetCommitBufferFlushesAfterUpdateThreshold(t *testing.T) {
	now := time.Now()
	buffer := newOffsetCommitBuffer(now)
	topic := "topic"

	for i := 0; i < offsetCommitUpdateThreshold; i++ {
		buffer.Add([]kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: kafka.Offset(i + 1)},
		})
	}

	require.True(t, buffer.ShouldFlush(now))
}
