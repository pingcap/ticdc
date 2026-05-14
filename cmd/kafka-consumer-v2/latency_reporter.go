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
	"context"
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const consumerLatencyLogInterval = time.Minute

type consumerLatencySnapshot struct {
	partitionCount        int
	initializedPartitions int

	globalWatermark uint64
	globalLag       time.Duration
	globalTime      time.Time

	slowestPartition int32
	slowestWatermark uint64
	slowestLag       time.Duration
	slowestTime      time.Time
}

func (e *replayEngine) runLatencyReporter(ctx context.Context) error {
	ticker := time.NewTicker(consumerLatencyLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.logLatency()
		}
	}
}

func (e *replayEngine) logLatency() {
	snapshot := e.getLatencySnapshot(time.Now())
	fields := []zap.Field{
		zap.String("consumerID", e.consumerID),
		zap.String("topic", e.topic),
		zap.Int("partitionCount", snapshot.partitionCount),
		zap.Int("initializedPartitions", snapshot.initializedPartitions),
	}

	if snapshot.globalWatermark != 0 {
		fields = append(fields,
			zap.Uint64("globalResolvedTs", snapshot.globalWatermark),
			zap.Time("globalResolvedTime", snapshot.globalTime),
			zap.Duration("globalResolvedLag", snapshot.globalLag),
			zap.Float64("globalResolvedLagSeconds", snapshot.globalLag.Seconds()))
	} else {
		fields = append(fields, zap.Bool("globalResolvedTsAvailable", false))
	}

	if snapshot.slowestWatermark != 0 {
		fields = append(fields,
			zap.Int32("slowestPartition", snapshot.slowestPartition),
			zap.Uint64("slowestPartitionResolvedTs", snapshot.slowestWatermark),
			zap.Time("slowestPartitionResolvedTime", snapshot.slowestTime),
			zap.Duration("slowestPartitionLag", snapshot.slowestLag),
			zap.Float64("slowestPartitionLagSeconds", snapshot.slowestLag.Seconds()))
	}

	log.Info("kafka consumer latency", fields...)
}

func (e *replayEngine) getLatencySnapshot(now time.Time) consumerLatencySnapshot {
	e.watermarkMu.RLock()
	defer e.watermarkMu.RUnlock()

	snapshot := consumerLatencySnapshot{
		partitionCount:   len(e.partitions),
		slowestPartition: -1,
	}
	globalWatermark := uint64(math.MaxUint64)
	for _, progress := range e.partitions {
		if progress.watermark == 0 {
			continue
		}
		snapshot.initializedPartitions++
		if progress.watermark < globalWatermark {
			globalWatermark = progress.watermark
		}

		partitionLag := resolvedTsLag(now, progress.watermark)
		if snapshot.slowestPartition == -1 || partitionLag > snapshot.slowestLag {
			snapshot.slowestPartition = progress.partition
			snapshot.slowestWatermark = progress.watermark
			snapshot.slowestLag = partitionLag
			snapshot.slowestTime = oracle.GetTimeFromTS(progress.watermark)
		}
	}

	if snapshot.partitionCount > 0 && snapshot.initializedPartitions == snapshot.partitionCount {
		snapshot.globalWatermark = globalWatermark
		snapshot.globalLag = resolvedTsLag(now, globalWatermark)
		snapshot.globalTime = oracle.GetTimeFromTS(globalWatermark)
	}
	return snapshot
}

func resolvedTsLag(now time.Time, ts uint64) time.Duration {
	if ts == 0 {
		return 0
	}
	lag := now.Sub(oracle.GetTimeFromTS(ts))
	if lag < 0 {
		return 0
	}
	return lag
}
