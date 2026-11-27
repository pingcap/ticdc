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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNodeStatsLifecycleAndSampling(t *testing.T) {
	stats, other := NewNodeStats(), NewNodeStats()
	counter := stats.NewCounter(Sink)
	first, second := counter.Recorder(), counter.Recorder()
	now := time.Now()
	require.Zero(t, stats.Snapshot(now).FlushedEventBytes)
	first.Add(100)
	second.Add(200)
	require.Zero(t, stats.Snapshot(now.Add(SampleInterval-time.Nanosecond)).FlushedEventBytes)
	load := stats.Snapshot(now.Add(SampleInterval))
	require.Equal(t, uint64(300), load.FlushedEventBytes)
	load.FlushedEventBytes, load.Resources[0].TotalBytes = 999, 999
	require.Equal(t, uint64(300), stats.Snapshot(now.Add(SampleInterval)).FlushedEventBytes)
	require.Zero(t, other.Snapshot(now).FlushedEventBytes, "captures must be isolated")

	first.Add(50)
	counter.Close()
	counter.Close()
	require.Empty(t, stats.counters)
	require.Nil(t, counter.Recorder())
	require.Equal(t, uint64(300), stats.Snapshot(now.Add(SampleInterval)).FlushedEventBytes,
		"retirement must not force extra sampling")
	require.Equal(t, uint64(350), stats.Snapshot(now.Add(2*SampleInterval)).FlushedEventBytes)
	replacement := stats.NewCounter(Sink)
	replacement.Recorder().Add(25)
	require.Equal(t, uint64(375), stats.Snapshot(now.Add(3*SampleInterval)).FlushedEventBytes)
}

func TestNodeStatsIntegerPrecision(t *testing.T) {
	stats := NewNodeStats()
	recorder := stats.NewCounter(Sink).Recorder()
	now := time.Now()
	recorder.Add(1 << 54)
	require.Equal(t, uint64(1<<54), stats.Snapshot(now).FlushedEventBytes)
	recorder.Add(1)
	require.Equal(t, uint64(1<<54)+1, stats.Snapshot(now.Add(SampleInterval)).FlushedEventBytes)
}

func TestNodeStatsConcurrentCollection(t *testing.T) {
	stats := NewNodeStats()
	counter := stats.NewCounter(Sink)
	const writers, iterations = 32, 2000
	var wg sync.WaitGroup
	for range writers {
		recorder := counter.Recorder()
		wg.Go(func() {
			for range iterations {
				recorder.Add(7)
			}
		})
	}
	now := time.Now()
	for i := range 100 {
		stats.Snapshot(now.Add(time.Duration(i) * SampleInterval))
	}
	wg.Wait()
	counter.Close()
	require.Equal(t, uint64(writers*iterations*7), stats.Snapshot(now.Add(101*SampleInterval)).FlushedEventBytes)
}

func BenchmarkResourceRecord(b *testing.B) {
	b.Run("sharedAtomic", func(b *testing.B) {
		var counter atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				counter.Add(100)
			}
		})
	})
	b.Run("componentShards", func(b *testing.B) {
		counter := NewNodeStats().NewCounter(Sink)
		b.RunParallel(func(pb *testing.PB) {
			recorder := counter.Recorder()
			for pb.Next() {
				recorder.Add(100)
			}
		})
	})
}

func BenchmarkNodeStatsSnapshot(b *testing.B) {
	stats := NewNodeStats()
	for range 1000 {
		stats.NewCounter(Sink).Recorder().Add(100)
	}
	now := time.Now()
	stats.Snapshot(now)
	b.Run("cached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			stats.Snapshot(now)
		}
	})
	b.Run("collect1000Components", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			now = now.Add(SampleInterval)
			stats.Snapshot(now)
		}
	})
}
