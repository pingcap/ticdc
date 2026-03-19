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

import "github.com/pingcap/ticdc/pkg/errors"

// budgetSnapshot is the current staged-byte view maintained by budgetCore.
type budgetSnapshot struct {
	memoryBytes int64
	diskBytes   int64
}

func (s budgetSnapshot) totalBytes() int64 {
	return s.memoryBytes + s.diskBytes
}

// budgetCore only owns threshold math and byte accounting.
// It does not know anything about callbacks, metrics, or spool lifecycle policy.
type budgetCore struct {
	memoryQuotaBytes   int64
	highWatermarkBytes int64
	lowWatermarkBytes  int64

	memoryBytes int64
	diskBytes   int64
}

func newBudgetCore(options *Options) *budgetCore {
	return &budgetCore{
		memoryQuotaBytes:   int64(float64(options.QuotaBytes) * options.MemoryRatio),
		highWatermarkBytes: int64(float64(options.QuotaBytes) * options.HighWatermarkRatio),
		lowWatermarkBytes:  int64(float64(options.QuotaBytes) * options.LowWatermarkRatio),
	}
}

// validateBudgetOptions checks the ratios needed by budgetCore's soft-control
// model. The model assumes one total budget that is split into a memory tier
// plus high/low watermarks for wake suppression.
func validateBudgetOptions(options *Options) error {
	if options.QuotaBytes <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool disk quota must be greater than 0, but got %d",
			options.QuotaBytes,
		)
	}
	if options.MemoryRatio <= 0 || options.MemoryRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool memory ratio must be in (0, 1), but got %f",
			options.MemoryRatio,
		)
	}
	if options.LowWatermarkRatio <= 0 || options.LowWatermarkRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool low watermark ratio must be in (0, 1), but got %f",
			options.LowWatermarkRatio,
		)
	}
	if options.HighWatermarkRatio <= 0 || options.HighWatermarkRatio >= 1 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool high watermark ratio must be in (0, 1), but got %f",
			options.HighWatermarkRatio,
		)
	}
	if options.LowWatermarkRatio >= options.HighWatermarkRatio {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool low watermark ratio must be less than high watermark ratio, low: %f high: %f",
			options.LowWatermarkRatio,
			options.HighWatermarkRatio,
		)
	}
	return nil
}

// shouldSpill decides whether a new entry still fits in the in-memory tier.
// This is intentionally a memory-tier decision only; it does not enforce a
// hard total quota and does not reject new writes.
func (b *budgetCore) shouldSpill(entryBytes int64) bool {
	return b.memoryBytes+entryBytes > b.memoryQuotaBytes
}

// reserve records a newly accepted entry in either the memory tier or the disk tier.
func (b *budgetCore) reserve(entryBytes int64, spilled bool) budgetSnapshot {
	if spilled {
		b.diskBytes += entryBytes
	}
	if !spilled {
		b.memoryBytes += entryBytes
	}
	return b.snapshot()
}

// release records that an entry has been fully consumed or discarded.
func (b *budgetCore) release(entryBytes int64, spilled bool) budgetSnapshot {
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
	return b.snapshot()
}

func (b *budgetCore) overHighWatermark() bool {
	return b.snapshot().totalBytes() > b.highWatermarkBytes
}

func (b *budgetCore) atOrBelowLowWatermark() bool {
	return b.snapshot().totalBytes() <= b.lowWatermarkBytes
}

func (b *budgetCore) reset() budgetSnapshot {
	b.memoryBytes = 0
	b.diskBytes = 0
	return b.snapshot()
}

func (b *budgetCore) snapshot() budgetSnapshot {
	return budgetSnapshot{
		memoryBytes: b.memoryBytes,
		diskBytes:   b.diskBytes,
	}
}
