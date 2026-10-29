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
	"os"
	"runtime/metrics"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
)

const diagnosticInterval = 30 * time.Second

type consumerDiagnostics struct {
	source      sourceType
	consumerID  string
	memory      *memoryUsage
	readBytes   atomic.Uint64
	writtenRows atomic.Uint64
	state       atomic.Pointer[consumerSnapshot]
}

// The conductor publishes values it owns. Reporting never reads mutable queues.
type consumerSnapshot struct {
	At              time.Time `json:"at"`
	Watermark       uint64    `json:"watermark"`
	Checkpoint      uint64    `json:"checkpoint"`
	WaitingSchema   int       `json:"waitingSchema"`
	PendingDML      int       `json:"pendingDML"`
	PendingDDL      int       `json:"pendingDDL"`
	PendingDelivery int       `json:"pendingDelivery"`
	PendingWrite    int       `json:"pendingWrite"`
	DDLInFlight     bool      `json:"ddlInFlight"`
	InFlightEvents  int       `json:"inFlightEvents"`
	InFlightBytes   int64     `json:"inFlightBytes"`
	PendingControls int       `json:"pendingControls"`
	DMLDedupKeys    int       `json:"dmlDedupKeys"`
	DDLDedupKeys    int       `json:"ddlDedupKeys"`
	DDLInputEntries int       `json:"ddlInputEntries"`
}

func newConsumerDiagnostics(source sourceType, consumerID string, memory *memoryUsage) *consumerDiagnostics {
	d := &consumerDiagnostics{source: source, consumerID: consumerID, memory: memory}
	memory.diagnostics = d
	return d
}

func (d *consumerDiagnostics) run(ctx context.Context) error {
	resources := processResources{}
	resources.sample(ctx) // Establish the CPU baseline before the first report.
	tick := time.Tick(diagnosticInterval)
	previousAt := time.Now()
	var previousRead, previousRows uint64
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
			now := time.Now()
			read, rows := d.readBytes.Load(), d.writtenRows.Load()
			seconds := now.Sub(previousAt).Seconds()
			fields := d.fields(now)
			fields = append(fields, zap.Uint64("readBytes", read), zap.Uint64("writtenRows", rows),
				zap.Float64("readBytesPerSecond", float64(read-previousRead)/seconds),
				zap.Float64("writtenRowsPerSecond", float64(rows-previousRows)/seconds))
			fields = append(fields, resources.sample(ctx)...)
			log.Info("consumer progress and resource summary", fields...)
			previousAt, previousRead, previousRows = now, read, rows
		}
	}
}

func (d *consumerDiagnostics) fields(now time.Time) []zap.Field {
	fields := []zap.Field{
		zap.String("sourceType", string(d.source)), zap.String("consumerID", d.consumerID),
		zap.Int64("budgetBytes", d.memory.used()), zap.Int64("budgetLimitBytes", d.memory.limit),
	}
	if s := d.state.Load(); s != nil {
		fields = append(fields, zap.Any("progress", s), zap.Duration("snapshotAge", now.Sub(s.At)))
	}
	return fields
}

// Called only by the conductor. Existing counters make sampling constant time.
func (c *conductor) publishDiagnostics(w *writer) {
	c.memory.diagnostics.state.Store(&consumerSnapshot{
		At: time.Now(), Watermark: c.watermark, Checkpoint: w.checkpoint,
		WaitingSchema: c.waitingMetadata, PendingDML: c.pendingDMLCount,
		PendingDDL: len(c.pendingDDL) + len(w.pendingDDL), PendingDelivery: 0,
		PendingWrite: len(w.pendingDML), DDLInFlight: w.ddlInFlight != nil,
		InFlightEvents: w.inFlightEvents, InFlightBytes: w.inFlightBytes,
		PendingControls: len(c.controls) + len(c.pendingWatermarks) + len(w.pendingWatermarks),
		DMLDedupKeys:    len(w.mutations), DDLDedupKeys: len(w.ddlWrites), DDLInputEntries: len(c.ddlInputs),
	})
}

type processResources struct {
	process     *process.Process
	previousCPU float64
	previousAt  time.Time
}

func (r *processResources) sample(parent context.Context) []zap.Field {
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	fields := []zap.Field{zap.Uint64("goHeapBytes", samples[0].Value.Uint64())}
	if r.process == nil {
		var err error
		r.process, err = process.NewProcessWithContext(ctx, int32(os.Getpid()))
		if err != nil {
			return append(fields, zap.Bool("rssAvailable", false), zap.Bool("cpuAvailable", false),
				zap.NamedError("resourceError", errors.WrapError(errors.ErrInternalCheckFailed, err, "sample consumer process")))
		}
	}
	if memory, err := r.process.MemoryInfoWithContext(ctx); err != nil {
		fields = append(fields, zap.Bool("rssAvailable", false),
			zap.NamedError("rssError", errors.WrapError(errors.ErrInternalCheckFailed, err, "sample consumer RSS")))
	} else {
		fields = append(fields, zap.Bool("rssAvailable", true), zap.Uint64("rssBytes", memory.RSS))
	}
	if times, err := r.process.TimesWithContext(ctx); err != nil {
		fields = append(fields, zap.Bool("cpuAvailable", false),
			zap.NamedError("cpuError", errors.WrapError(errors.ErrInternalCheckFailed, err, "sample consumer CPU")))
	} else {
		now, cpu := time.Now(), times.User+times.System
		if !r.previousAt.IsZero() && cpu >= r.previousCPU {
			seconds := now.Sub(r.previousAt).Seconds()
			fields = append(fields, zap.Bool("cpuAvailable", true),
				zap.Float64("cpuCores", (cpu-r.previousCPU)/seconds), zap.Float64("cpuSampleSeconds", seconds))
		} else {
			fields = append(fields, zap.Bool("cpuAvailable", false))
		}
		r.previousAt, r.previousCPU = now, cpu
	}
	return fields
}
