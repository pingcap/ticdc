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
	stderrors "errors"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/log"
	meteringwriterapi "github.com/pingcap/metering_sdk/writer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metering"
	"go.uber.org/zap"
)

const (
	meteringReportInterval       = time.Minute
	meteringShutdownFlushTimeout = 10 * time.Second
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

// OnChangefeedRemoved forgets sequence tracking for cfID while preserving any
// unreported pending bytes in memory until uploaded.
func (m *meteringAccumulator) OnChangefeedRemoved(cfID common.ChangeFeedID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lastSeq, cfID)
}

// Snapshot returns a deterministic batch of all positive pending balances
// without holding the mutex across network I/O.
func (m *meteringAccumulator) Snapshot() []metering.Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.pending) == 0 {
		return nil
	}
	records := make([]metering.Record, 0, len(m.pending))
	for name, bytes := range m.pending {
		if bytes == 0 {
			continue
		}
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
	return records
}

// Commit deducts only the snapshotted amounts after a successful upload,
// preserving increments received concurrently during the upload.
func (m *meteringAccumulator) Commit(records []metering.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, r := range records {
		key := common.ChangeFeedDisplayName{Keyspace: r.KeyspaceName, Name: r.ChangefeedName}
		cur := m.pending[key]
		if cur <= r.EffectiveBytes {
			delete(m.pending, key)
		} else {
			m.pending[key] = cur - r.EffectiveBytes
		}
	}
}

// PendingBytesTotal returns the total unreported traffic bytes currently in memory.
func (m *meteringAccumulator) PendingBytesTotal() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total uint64
	for _, bytes := range m.pending {
		total += bytes
	}
	return total
}

// meteringReporter schedules per-minute delta uploads and shutdown flush for one coordinator term.
type meteringReporter struct {
	reporter      *metering.Reporter
	accumulator   *meteringAccumulator
	lastAttemptTS int64
}

func newMeteringReporter(reporter *metering.Reporter, accumulator *meteringAccumulator) *meteringReporter {
	return &meteringReporter{
		reporter:    reporter,
		accumulator: accumulator,
	}
}

func (r *meteringReporter) Run(ctx context.Context) error {
	next := time.Now().Truncate(meteringReportInterval).Add(meteringReportInterval)

	for {
		wait := time.Until(next)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			return nil
		}

		r.reportAt(ctx, next)
		next = next.Add(meteringReportInterval)
		if now := time.Now(); !now.Before(next) {
			next = now.Truncate(meteringReportInterval).Add(meteringReportInterval)
		}
	}
}

func (r *meteringReporter) reportAt(ctx context.Context, reportTime time.Time) error {
	if r.reporter == nil || r.accumulator == nil {
		return nil
	}
	meteringPendingBytesGauge.Set(float64(r.accumulator.PendingBytesTotal()))
	records := r.accumulator.Snapshot()
	if len(records) == 0 {
		return nil
	}
	ts := reportTime.Truncate(time.Minute).Unix()
	if ts > r.lastAttemptTS {
		r.lastAttemptTS = ts
	}

	start := time.Now()
	err := r.reporter.Report(ctx, ts, records)
	meteringReportDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		if stderrors.Is(err, meteringwriterapi.ErrFileExists) {
			meteringReportCounter.WithLabelValues("exists").Inc()
		} else {
			meteringReportCounter.WithLabelValues("failure").Inc()
		}
		log.Warn("failed to upload traffic metering delta",
			zap.Int64("timestamp", ts),
			zap.Int("recordCount", len(records)),
			zap.Error(err))
		return err
	}

	r.accumulator.Commit(records)
	meteringReportCounter.WithLabelValues("success").Inc()
	meteringLastSuccessTimestampGauge.Set(float64(ts))
	meteringPendingBytesGauge.Set(float64(r.accumulator.PendingBytesTotal()))
	return nil
}

func (r *meteringReporter) FlushOnShutdown() {
	if r.reporter == nil || r.accumulator == nil || r.accumulator.PendingBytesTotal() == 0 {
		return
	}
	flushTS := time.Now().Truncate(time.Minute).Add(time.Minute)
	if flushTS.Unix() <= r.lastAttemptTS {
		flushTS = time.Unix(r.lastAttemptTS, 0).Add(time.Minute)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), meteringShutdownFlushTimeout)
	defer cancel()
	if err := r.reportAt(flushCtx, flushTS); err != nil {
		log.Warn("shutdown traffic metering flush did not complete",
			zap.Int64("timestamp", flushTS.Unix()),
			zap.Error(err))
	}
}

func (r *meteringReporter) Close() {
	if r.reporter != nil {
		r.reporter.Close()
	}
}
