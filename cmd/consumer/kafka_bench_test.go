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
	"strconv"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func BenchmarkKafkaReadScheduling(b *testing.B) {
	for _, count := range []int{4, 64} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			memory := newMemoryUsage()
			c := &kafkaReader{
				readLimit:  newReadLimit(memory),
				memory:     memory,
				partitions: make(map[int32]*kafkaPartition, count),
			}
			for id := range count {
				c.partitions[int32(id)] = &kafkaPartition{
					hasWatermark: true,
					watermark:    10,
					polled:       []*kgo.Record{{Partition: int32(id)}},
				}
			}
			data := &readData{}
			b.ReportAllocs()
			for b.Loop() {
				c.limitReads()
				id := c.nextPartition()
				c.readSequence++
				c.partitions[id].sequence = c.readSequence
				data.stream = id
				c.Advance([]readUpdate{{data: data, decoded: true}})
			}
		})
	}
}
