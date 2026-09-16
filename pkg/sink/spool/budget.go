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

// Limits defines the byte limits used by a spool budget.
type Limits struct {
	DiskQuotaBytes     int64
	MemoryQuotaBytes   int64
	HighWatermarkBytes int64
	LowWatermarkBytes  int64
}

// Budget tracks the memory and disk bytes owned by a spool. It is not
// thread-safe; callers should synchronize compound admission decisions.
type Budget struct {
	limits Limits

	memoryBytes int64
	diskBytes   int64
}

// NewBudget creates an empty spool budget with the supplied limits.
func NewBudget(limits Limits) *Budget {
	return &Budget{limits: limits}
}

// CanFitMemory reports whether an entry can be admitted into memory.
func (b *Budget) CanFitMemory(entryBytes int64) bool {
	return b.memoryBytes+entryBytes <= b.limits.MemoryQuotaBytes
}

// ShouldSpill reports whether a new entry should be written to disk instead
// of being retained in memory.
func (b *Budget) ShouldSpill(entryBytes int64) bool {
	return !b.CanFitMemory(entryBytes)
}

// EntryExceedsDiskQuota reports whether one entry is larger than the entire
// disk quota.
func (b *Budget) EntryExceedsDiskQuota(entryBytes int64) bool {
	return entryBytes > b.limits.DiskQuotaBytes
}

// SpillWouldExceedDiskQuota reports whether admitting one more spilled entry
// would exceed the disk quota.
func (b *Budget) SpillWouldExceedDiskQuota(entryBytes int64) bool {
	return b.diskBytes+entryBytes > b.limits.DiskQuotaBytes
}

// Acquire records an admitted entry and reports whether total staged bytes
// are above the high watermark.
func (b *Budget) Acquire(entryBytes int64, spilled bool) bool {
	if spilled {
		b.diskBytes += entryBytes
	} else {
		b.memoryBytes += entryBytes
	}
	return b.TotalBytes() > b.limits.HighWatermarkBytes
}

// Release removes a flushed or discarded entry and reports whether total
// staged bytes are at or below the low watermark.
func (b *Budget) Release(entryBytes int64, spilled bool) bool {
	if spilled {
		b.diskBytes -= entryBytes
	} else {
		b.memoryBytes -= entryBytes
	}
	if b.memoryBytes < 0 {
		b.memoryBytes = 0
	}
	if b.diskBytes < 0 {
		b.diskBytes = 0
	}
	return b.TotalBytes() <= b.limits.LowWatermarkBytes
}

// MemoryBytes returns currently staged in-memory bytes.
func (b *Budget) MemoryBytes() int64 {
	return b.memoryBytes
}

// DiskBytes returns currently staged on-disk bytes.
func (b *Budget) DiskBytes() int64 {
	return b.diskBytes
}

// TotalBytes returns all currently staged bytes.
func (b *Budget) TotalBytes() int64 {
	return b.memoryBytes + b.diskBytes
}

// Limits returns the immutable limits of this budget.
func (b *Budget) Limits() Limits {
	return b.limits
}
