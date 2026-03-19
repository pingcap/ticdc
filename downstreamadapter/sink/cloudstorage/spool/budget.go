// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

// budget stores the current queued byte counts and the byte limits derived from spool config.
type budget struct {
	// memoryQuotaBytes is the largest byte count we still keep in memory.
	// If adding a new entry would cross this value, spool writes that entry
	// to local spool files instead of keeping it in memory.
	memoryQuotaBytes int64
	// highWatermarkBytes is the byte count that makes spool stop running the
	// new onEnqueued callback immediately. The callback is saved in memory and
	// will be run later.
	highWatermarkBytes int64
	// lowWatermarkBytes is the byte count that lets spool run the saved
	// onEnqueued callbacks again after some queued data has been flushed to the
	// downstream storage or discarded locally.
	lowWatermarkBytes int64

	// memoryBytes is the number of queued bytes that are still kept in memory.
	memoryBytes int64
	// diskBytes is the number of queued bytes that have already been written to local spool files.
	diskBytes int64
}

func newBudget(options *options) *budget {
	return &budget{
		memoryQuotaBytes:   int64(float64(options.quotaBytes) * options.memoryRatio),
		highWatermarkBytes: int64(float64(options.quotaBytes) * options.highWatermarkRatio),
		lowWatermarkBytes:  int64(float64(options.quotaBytes) * options.lowWatermarkRatio),
	}
}

// shouldSpill decides whether a new entry should stay in memory or be written
// to local spool files. It only looks at the memory limit. It does not reject
// new writes.
func (b *budget) shouldSpill(entryBytes int64) bool {
	return b.memoryBytes+entryBytes > b.memoryQuotaBytes
}

// reserve adds a newly accepted entry to the current byte counters.
func (b *budget) reserve(entryBytes int64, spilled bool) {
	if spilled {
		b.diskBytes += entryBytes
	}
	if !spilled {
		b.memoryBytes += entryBytes
	}
}

// release removes an entry from the current byte counters after the entry has
// been flushed or discarded.
func (b *budget) release(entryBytes int64, spilled bool) {
	if spilled {
		b.diskBytes -= entryBytes
	}
	if !spilled {
		b.memoryBytes -= entryBytes
	}
	if b.memoryBytes < 0 {
		b.memoryBytes = 0
	}
	if b.diskBytes < 0 {
		b.diskBytes = 0
	}
}

func (b *budget) overHighWatermark() bool {
	return b.totalBytes() > b.highWatermarkBytes
}

func (b *budget) atOrBelowLowWatermark() bool {
	return b.totalBytes() <= b.lowWatermarkBytes
}

func (b *budget) reset() {
	b.memoryBytes = 0
	b.diskBytes = 0
}

func (b *budget) totalBytes() int64 {
	return b.memoryBytes + b.diskBytes
}
