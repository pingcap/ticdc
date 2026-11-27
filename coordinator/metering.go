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

package coordinator

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metering"
	"go.uber.org/zap"
)

const (
	meteringReportInterval       = time.Minute
	meteringShutdownFlushTimeout = 10 * time.Second
	maxShutdownFlushAttempts     = 5
)

// meteringAccumulator buffers unreported per-changefeed traffic deltas in memory.
// Sequence deduplication is tracked per ChangeFeedID, while unreported byte
// balances are keyed by DisplayName so concurrent deletions and uploads stay decoupled.
type meteringAccumulator struct {
	mu      sync.Mutex
	lastSeq map[common.ChangeFeedID]uint64
	pending map[common.ChangeFeedDisplayName]uint64
}

func newMeteringAccumulator() *meteringAccumulator {
	return &meteringAccumulator{
		lastSeq: make(map[common.ChangeFeedID]uint64),
		pending: make(map[common.ChangeFeedDisplayName]uint64),
	}
}

// Accept records a maintainer round report if seq == lastSeq + 1 and returns
// the cumulative accepted sequence number for ACK. Duplicate reports with
// seq <= lastSeq return (lastSeq, true) idempotently without double-counting.
func (m *meteringAccumulator) Accept(cfID common.ChangeFeedID, seq, delta uint64) (ackedSeq uint64, ok bool) {
	if seq == 0 {
		return 0, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	prev := m.lastSeq[cfID]
	if seq == prev+1 {
		m.lastSeq[cfID] = seq
		if delta > 0 {
			m.pending[cfID.DisplayName] += delta
		}
		return seq, true
	}
	if seq <= prev {
		return prev, true
	}
	return prev, false
}

// Forget releases sequence tracking for cfID while preserving any
// unreported pending bytes in memory until uploaded.
func (m *meteringAccumulator) Forget(cfID common.ChangeFeedID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lastSeq, cfID)
}

// Snapshot returns a deterministic batch of all positive pending balances
// and the total buffered bytes without holding the mutex across network I/O.
func (m *meteringAccumulator) Snapshot() ([]metering.Record, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pending) == 0 {
		return nil, 0
	}
	records := make([]metering.Record, 0, len(m.pending))
	var total uint64
	for name, bytes := range m.pending {
		if bytes == 0 {
			continue
		}
		total += bytes
		records = append(records, metering.Record{
			KeyspaceName:   name.Keyspace,
			ChangefeedName: name.Name,
			EffectiveBytes: bytes,
		})
	}
	slices.SortFunc(records, func(a, b metering.Record) int {
		if c := cmp.Compare(a.KeyspaceName, b.KeyspaceName); c != 0 {
			return c
		}
		return cmp.Compare(a.ChangefeedName, b.ChangefeedName)
	})
	return records, total
}

// Commit deducts only the snapshotted amounts after a successful upload,
// preserving increments received concurrently during the upload, and returns
// the remaining buffered bytes.
func (m *meteringAccumulator) Commit(records []metering.Record) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, r := range records {
		key := common.ChangeFeedDisplayName{Keyspace: r.KeyspaceName, Name: r.ChangefeedName}
		cur := m.pending[key]
		if cur <= r.EffectiveBytes {
			delete(m.pending, key)
			continue
		}
		m.pending[key] = cur - r.EffectiveBytes
	}
	var remaining uint64
	for _, bytes := range m.pending {
		remaining += bytes
	}
	return remaining
}

// meteringReporter schedules per-minute delta uploads and shutdown flush for one coordinator term.
type meteringReporter struct {
	reporter         *metering.Reporter
	accumulator      *meteringAccumulator
	uncertainTS      int64
	uncertainRecords []metering.Record
}

func newMeteringReporter(reporter *metering.Reporter, accumulator *meteringAccumulator) *meteringReporter {
	return &meteringReporter{
		reporter:    reporter,
		accumulator: accumulator,
	}
}

func (r *meteringReporter) Run(ctx context.Context) error {
	next := time.Now().Truncate(meteringReportInterval).Add(meteringReportInterval)

	for waitUntil(ctx, next) {
		// reportAt logs its own failures; the pending bytes stay in the accumulator
		// and are retried on the next tick.
		_ = r.reportAt(ctx, next)
		next = next.Add(meteringReportInterval)
		if now := time.Now(); !now.Before(next) {
			next = now.Truncate(meteringReportInterval).Add(meteringReportInterval)
		}
	}
	return nil
}

// waitUntil blocks until deadline or ctx cancellation and reports whether deadline was reached.
func waitUntil(ctx context.Context, deadline time.Time) bool {
	wait := time.Until(deadline)
	if wait <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *meteringReporter) resolveUncertain(ctx context.Context) error {
	if len(r.uncertainRecords) == 0 {
		return nil
	}
	persisted, err := r.reporter.ResolveUncertainUpload(ctx)
	if err != nil {
		meteringReportCounter.WithLabelValues("failure").Inc()
		log.Warn("failed to verify uncertain traffic metering upload",
			zap.Int64("timestamp", r.uncertainTS),
			zap.Int("recordCount", len(r.uncertainRecords)),
			zap.Error(err))
		return err
	}
	if persisted {
		meteringPendingBytesGauge.Set(float64(r.accumulator.Commit(r.uncertainRecords)))
		meteringReportCounter.WithLabelValues("success").Inc()
		meteringLastSuccessTimestampGauge.Set(float64(r.uncertainTS))
	}
	r.uncertainTS = 0
	r.uncertainRecords = nil
	return nil
}

func (r *meteringReporter) reportAt(ctx context.Context, reportTime time.Time) error {
	if err := r.resolveUncertain(ctx); err != nil {
		return err
	}
	records, pending := r.accumulator.Snapshot()
	meteringPendingBytesGauge.Set(float64(pending))
	if len(records) == 0 {
		return nil
	}
	ts := reportTime.Truncate(time.Minute).Unix()

	start := time.Now()
	err := r.reporter.Report(ctx, ts, records)
	meteringReportDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		if r.reporter.HasUncertainUpload() {
			r.uncertainTS = ts
			r.uncertainRecords = records
		}
		reason := "failure"
		if errors.Is(err, writer.ErrFileExists) {
			reason = "exists"
		}
		meteringReportCounter.WithLabelValues(reason).Inc()
		log.Warn("failed to upload traffic metering delta",
			zap.Int64("timestamp", ts),
			zap.Int("recordCount", len(records)),
			zap.Error(err))
		return err
	}

	meteringPendingBytesGauge.Set(float64(r.accumulator.Commit(records)))
	meteringReportCounter.WithLabelValues("success").Inc()
	meteringLastSuccessTimestampGauge.Set(float64(ts))
	return nil
}

func (r *meteringReporter) FlushOnShutdown() {
	records, _ := r.accumulator.Snapshot()
	if len(records) == 0 && len(r.uncertainRecords) == 0 {
		return
	}
	flushTS := time.Now().Truncate(time.Minute).Add(time.Minute)
	flushCtx, cancel := context.WithTimeout(context.Background(), meteringShutdownFlushTimeout)
	defer cancel()

	var err error
	for range maxShutdownFlushAttempts {
		err = r.reportAt(flushCtx, flushTS)
		if err == nil || !errors.Is(err, writer.ErrFileExists) || flushCtx.Err() != nil {
			break
		}
		flushTS = flushTS.Add(time.Minute)
	}
	if err != nil {
		log.Warn("shutdown traffic metering flush did not complete",
			zap.Int64("timestamp", flushTS.Unix()),
			zap.Error(err))
	}
}

func (r *meteringReporter) Close() {
	r.reporter.Close()
}
