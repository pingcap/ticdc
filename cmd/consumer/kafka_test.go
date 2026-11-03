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

package main

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaReadScheduling(t *testing.T) {
	for _, test := range []struct {
		name          string
		underPressure bool
		continuing    bool
		targets       []int32
		valueBytes    int
		buffered      [2]int
		order         []int32
	}{
		{name: "fair batches", buffered: [2]int{batchRows + 1, batchRows + 1}, order: []int32{0, 1, 0, 1}},
		{name: "byte bounded batches", valueBytes: batchBytes, buffered: [2]int{2, 0}, order: []int32{0, 0}},
		{name: "control partition", continuing: true, targets: []int32{0}, buffered: [2]int{2 * batchRows, 2 * batchRows}, order: []int32{0, 1, 0, 1}},
		{name: "lagging partition", continuing: true, targets: []int32{0, 1}, buffered: [2]int{2 * batchRows, 2 * batchRows}, order: []int32{1, 0, 1, 0}},
		{name: "dependency at budget", underPressure: true, continuing: true, targets: []int32{1}, buffered: [2]int{2 * batchRows, 2 * batchRows}, order: []int32{1, 0, 1, 0}},
		{name: "buffered independent input", underPressure: true, continuing: true, targets: []int32{1}, buffered: [2]int{1, 0}, order: []int32{0}},
		{name: "unknown schema", underPressure: true, continuing: true, buffered: [2]int{batchRows, batchRows}, order: []int32{0, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, err := kgo.NewClient()
			require.NoError(t, err)
			memory := newMemoryUsage()
			if test.underPressure {
				memory.reserve(memory.limit)
			}
			r := &kafkaReader{
				readLimit: newReadLimit(memory), client: client, topic: "scheduling", memory: memory,
				partitions: map[int32]*kafkaPartition{
					0: {watermark: 200},
					1: {watermark: 100},
				},
			}
			defer r.Close()
			for id, count := range test.buffered {
				for offset := range count {
					record := &kgo.Record{Partition: int32(id), Offset: int64(offset), Value: make([]byte, test.valueBytes)}
					memory.reserve(int64(len(record.Value)))
					r.partitions[int32(id)].polled = append(r.partitions[int32(id)].polled, record)
				}
			}
			if len(test.targets) != 0 {
				r.boundary = &readBoundary{}
				for _, id := range test.targets {
					r.partitions[id].targetTs = 300
				}
			}
			r.ContinueReading(test.continuing)
			offsets := [2]int64{}
			for _, id := range test.order {
				batch, err := r.Read(ctx)
				require.NoError(t, err)
				limit := batchRows
				if test.valueBytes >= batchBytes {
					limit = 1
				}
				require.Len(t, batch, min(limit, test.buffered[id]-int(offsets[id])))
				for _, input := range batch {
					require.Equal(t, id, input.message.Partition)
					require.Equal(t, offsets[id], input.message.Offset)
					offsets[id]++
					memory.release(int64(len(input.message.Value)))
				}
			}
			if test.underPressure && len(test.targets) != 0 {
				require.Equal(t, map[string][]int32{"scheduling": {0}}, client.PauseFetchPartitions(nil))
				r.Advance([]readUpdate{{stream: 1, watermark: 300}})
				require.True(t, r.boundary.reached)
			}
			if test.underPressure {
				memory.release(memory.limit)
			}
			r.ContinueReading(false)
			require.Empty(t, client.PauseFetchPartitions(nil))
			require.Zero(t, memory.used())
		})
	}
}

func TestKafkaCatchupKeepsTargetAndPrioritizesDependencies(t *testing.T) {
	memory := newMemoryUsage()
	r := &kafkaReader{
		readLimit: newReadLimit(memory), memory: memory,
		partitions: map[int32]*kafkaPartition{
			0: {watermark: 100},
			1: {watermark: 200},
		},
	}
	c := &conductor{reader: r, memory: memory, watermark: 100, readThrough: 200}
	w := &writer{}
	c.advanceReading(w)
	first := c.boundary
	require.Equal(t, uint64(200), r.partitions[0].targetTs)
	require.Zero(t, r.partitions[1].targetTs)

	c.readThrough = 300
	c.advanceReading(w)
	require.Same(t, first, c.boundary)
	require.Equal(t, uint64(200), r.partitions[0].targetTs)
	require.Zero(t, r.partitions[1].targetTs)

	r.Advance([]readUpdate{{stream: 0, watermark: 200}})
	c.watermark = 200
	require.True(t, first.reached)
	c.advanceReading(w)
	require.NotSame(t, first, c.boundary)
	require.Equal(t, uint64(300), r.partitions[0].targetTs)
	require.Equal(t, uint64(300), r.partitions[1].targetTs)

	c.controlTs = 250
	c.advanceReading(w)
	require.Equal(t, uint64(250), r.partitions[0].targetTs)
	require.Zero(t, r.partitions[1].targetTs)

	c.controlTs = 0
	ddl := c.queueDDL(&event.DDLEvent{FinishedTs: 240, SchemaName: "test", TableName: "t"})
	c.advanceReading(w)
	require.Same(t, ddl.event, c.boundaryDDL)
	require.Same(t, c.boundary, ddl.event.boundary)
	require.Equal(t, uint64(240), r.partitions[0].targetTs)
	require.Equal(t, uint64(240), r.partitions[1].targetTs)
}

func TestKafkaReadWaitsForMemoryAndResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const topic = "backpressure"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	require.NoError(t, err)
	memory := newMemoryUsage()
	r := &kafkaReader{
		readLimit: newReadLimit(memory), client: client, topic: topic, memory: memory,
		partitions: map[int32]*kafkaPartition{0: {}},
	}
	defer r.Close()
	type readResult struct {
		batch []kafkaInput
		err   error
	}
	finished := make(chan readResult, 1)
	go func() {
		batch, err := r.Read(ctx)
		finished <- readResult{batch, err}
	}()
	require.Eventually(t, func() bool {
		r.progressMu.Lock()
		defer r.progressMu.Unlock()
		return r.pollCancel != nil
	}, 5*time.Second, time.Millisecond)
	memory.reserve(memory.limit)
	r.ContinueReading(false)
	require.Eventually(t, func() bool { return memory.waiting.Load() }, 5*time.Second, time.Millisecond)
	require.Len(t, client.PauseFetchPartitions(nil)[topic], 1)
	require.NoError(t, client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("row")}).FirstErr())
	select {
	case result := <-finished:
		t.Fatalf("reader returned before memory was released: %v", result.err)
	default:
	}
	memory.release(memory.limit)
	result := <-finished
	require.NoError(t, result.err)
	require.Len(t, result.batch, 1)
	require.Equal(t, "row", string(result.batch[0].message.Value))
	memory.release(int64(len(result.batch[0].message.Value)))
	require.Empty(t, client.PauseFetchPartitions(nil))

	memory.reserve(memory.limit)
	readCtx, stop := context.WithCancel(ctx)
	go func() {
		_, err := r.Read(readCtx)
		finished <- readResult{err: err}
	}()
	require.Eventually(t, func() bool { return len(client.PauseFetchPartitions(nil)[topic]) == 1 }, 5*time.Second, time.Millisecond)
	stop()
	require.ErrorIs(t, (<-finished).err, context.Canceled)
	memory.release(memory.limit)
}

func TestKafkaReadyRowsDoNotRequestMoreInput(t *testing.T) {
	client, err := kgo.NewClient()
	require.NoError(t, err)
	memory := newMemoryUsage()
	r := &kafkaReader{
		readLimit: newReadLimit(memory), client: client, topic: "ready", memory: memory,
		partitions: map[int32]*kafkaPartition{0: {watermark: 200}, 1: {watermark: 100}},
	}
	defer r.Close()
	c := &conductor{reader: r, memory: memory, watermark: 100, readThrough: 300}
	row := conductorTestRow(memory, 1, 1, 150, true)
	row.boundary = &readBoundary{watermark: &r.partitions[0].watermark}
	c.queueDML(row)
	memory.limit = memory.used()
	require.Nil(t, c.next(func(*decodeTask) bool { return false }))
	w := &writer{}
	c.advanceReading(w)
	require.False(t, r.continuing.Load())
	require.ElementsMatch(t, []int32{0, 1}, client.PauseFetchPartitions(nil)["ready"])

	c.popDML(row)
	w.inFlight = []*writeBatch{{}}
	c.advanceReading(w)
	require.False(t, r.continuing.Load())

	blocked := conductorTestRow(memory, 1, 1, 250, true)
	blocked.boundary = row.boundary
	c.queueDML(blocked)
	require.Nil(t, c.next(func(*decodeTask) bool { return false }))
	c.advanceReading(w)
	require.True(t, r.continuing.Load())
	require.Equal(t, map[string][]int32{"ready": {1}}, client.PauseFetchPartitions(nil))
	r.Advance([]readUpdate{{stream: 0, watermark: 250}})
	// Reevaluate the actual dependency even before another selection resets controlTs.
	c.advanceReading(w)
	require.False(t, r.continuing.Load())
	c.popDML(blocked)
	memory.release(blocked.bytes)

	w.inFlight = nil
	memory.release(row.bytes)
	c.advanceReading(w)
	require.True(t, r.continuing.Load())
	require.Empty(t, client.PauseFetchPartitions(nil))
}
