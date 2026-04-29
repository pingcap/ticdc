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

package eventservice

import (
	"bufio"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	observedScanWindowTraceFixtureFile = "kafka_test_observed_trace_20260423.jsonl"
	observedScanWindowTraceTimeLayout  = "2006-01-02 15:04:05"
)

type observedScanWindowTracePoint struct {
	ts                            time.Time
	offsetSec                     int
	observedScanIntervalSec       float64
	observedResetDispatcherPerSec float64
	observedSinkRowsPerSec        *float64
	observedMemoryQuotaBytes      float64
}

type observedScanWindowTraceRow struct {
	Time                          string   `json:"time"`
	OffsetSec                     int      `json:"offsetSec"`
	ObservedScanIntervalSec       *float64 `json:"observedScanIntervalSec"`
	ObservedResetDispatcherPerSec *float64 `json:"observedResetDispatcherPerSec"`
	ObservedSinkRowsPerSec        *float64 `json:"observedSinkRowsPerSec"`
	ObservedMemoryQuotaBytes      *float64 `json:"observedMemoryQuotaBytes"`
}

type observedScanWindowCollapse struct {
	peak      observedScanWindowTracePoint
	floor     observedScanWindowTracePoint
	dropRatio float64
}

func TestObservedScanWindowTraceFixtureLoadsExpectedShape(t *testing.T) {
	t.Parallel()

	trace := loadObservedScanWindowTrace(t)
	require.Len(t, trace, 233)

	require.Equal(t, mustParseObservedScanWindowTraceTime(t, "2026-04-23 18:17:45"), trace[0].ts)
	require.Equal(t, 0, trace[0].offsetSec)
	require.Nil(t, trace[0].observedSinkRowsPerSec)
	require.InDelta(t, 0, trace[0].observedMemoryQuotaBytes, 1e-9)

	last := trace[len(trace)-1]
	require.Equal(t, mustParseObservedScanWindowTraceTime(t, "2026-04-23 19:32:30"), last.ts)
	require.Equal(t, 4485, last.offsetSec)

	cadenceCounts := observedTraceCadenceCounts(trace)
	require.Equal(t, 231, cadenceCounts[15])
	require.Equal(t, 1, cadenceCounts[1020])
	require.Len(t, cadenceCounts, 2)

	dominantQuotaBytes, dominantQuotaCount := dominantObservedTraceQuota(trace)
	require.Equal(t, int64(1<<30), dominantQuotaBytes)
	require.Equal(t, 232, dominantQuotaCount)

	require.InDelta(t, 811.0, maxObservedTraceScanInterval(trace), 1e-9)
}

func TestObservedScanWindowTraceCharacterizationCapturesLegacySawtooth(t *testing.T) {
	t.Parallel()

	trace := loadObservedScanWindowTrace(t)

	peaks := findObservedTraceSeparatedPeaks(trace, 300, 5*time.Minute)
	require.GreaterOrEqual(t, len(peaks), 3)

	collapses := findObservedTraceFloorCollapses(trace, 300, time.Minute, defaultScanInterval.Seconds(), 0.8)
	require.GreaterOrEqual(t, len(collapses), 3)
	for _, collapse := range collapses {
		require.LessOrEqual(t, collapse.floor.observedScanIntervalSec, defaultScanInterval.Seconds())
	}

	intervals := observedTraceScanIntervals(trace)
	require.InDelta(t, 25.3, observedTracePercentile(intervals, 0.5), 1e-9)
	require.InDelta(t, 438.0, observedTracePercentile(intervals, 0.95), 1e-9)
	require.Greater(t, observedTracePercentile(intervals, 0.95), observedTracePercentile(intervals, 0.5)*10)
}

func TestObservedScanWindowTraceWindowOracles(t *testing.T) {
	t.Parallel()

	trace := loadObservedScanWindowTrace(t)

	w1Peak := findObservedTracePointByTime(t, trace, "2026-04-23 18:24:30")
	w1Floor := findObservedTracePointByTime(t, trace, "2026-04-23 18:25:15")
	require.InDelta(t, 438.0, w1Peak.observedScanIntervalSec, 1e-9)
	require.InDelta(t, 1.0, w1Floor.observedScanIntervalSec, 1e-9)
	require.Equal(t, 45, w1Floor.offsetSec-w1Peak.offsetSec)

	w3Peak := findObservedTracePointByTime(t, trace, "2026-04-23 18:56:45")
	w3Floor := findObservedTracePointByTime(t, trace, "2026-04-23 18:57:45")
	require.InDelta(t, 811.0, w3Peak.observedScanIntervalSec, 1e-9)
	require.InDelta(t, 1.0, w3Floor.observedScanIntervalSec, 1e-9)
	require.Equal(t, 60, w3Floor.offsetSec-w3Peak.offsetSec)
	require.GreaterOrEqual(t, w3Peak.observedResetDispatcherPerSec, 0.9)
	require.GreaterOrEqual(t, w3Floor.observedResetDispatcherPerSec, 0.8)

	w4Window := selectObservedTraceWindow(
		t,
		trace,
		"2026-04-23 19:08:15",
		"2026-04-23 19:10:30",
	)
	require.GreaterOrEqual(t, maxObservedTraceScanInterval(w4Window), 592.0)
	require.GreaterOrEqual(t, countObservedTraceScanIntervalsAtLeast(w4Window, 300), 8)
	require.InDelta(
		t,
		1.0,
		findObservedTracePointByTime(t, trace, "2026-04-23 19:11:00").observedScanIntervalSec,
		1e-9,
	)

	w5Window := selectObservedTraceWindow(
		t,
		trace,
		"2026-04-23 19:15:45",
		"2026-04-23 19:18:30",
	)
	require.GreaterOrEqual(t, maxObservedTraceScanInterval(w5Window), 135.0)
	require.GreaterOrEqual(t, maxObservedTraceResetRate(w5Window), 1.7)
	require.InDelta(
		t,
		1.0,
		findObservedTracePointByTime(t, trace, "2026-04-23 19:18:45").observedScanIntervalSec,
		1e-9,
	)
}

func loadObservedScanWindowTrace(t *testing.T) []observedScanWindowTracePoint {
	t.Helper()

	file, err := os.Open(observedScanWindowTraceFixturePath(t))
	require.NoError(t, err)
	defer file.Close()

	trace := make([]observedScanWindowTracePoint, 0, 256)
	scanner := bufio.NewScanner(file)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		var row observedScanWindowTraceRow
		err = json.Unmarshal(scanner.Bytes(), &row)
		require.NoError(t, err, "line %d", lineNo)
		require.NotNil(t, row.ObservedScanIntervalSec, "line %d", lineNo)
		require.NotNil(t, row.ObservedResetDispatcherPerSec, "line %d", lineNo)
		require.NotNil(t, row.ObservedMemoryQuotaBytes, "line %d", lineNo)

		trace = append(trace, observedScanWindowTracePoint{
			ts:                            mustParseObservedScanWindowTraceTime(t, row.Time),
			offsetSec:                     row.OffsetSec,
			observedScanIntervalSec:       *row.ObservedScanIntervalSec,
			observedResetDispatcherPerSec: *row.ObservedResetDispatcherPerSec,
			observedSinkRowsPerSec:        row.ObservedSinkRowsPerSec,
			observedMemoryQuotaBytes:      *row.ObservedMemoryQuotaBytes,
		})
	}
	require.NoError(t, scanner.Err())

	return trace
}

func observedScanWindowTraceFixturePath(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Join(
		filepath.Dir(filename),
		"..",
		"..",
		".issue",
		"replay-data",
		observedScanWindowTraceFixtureFile,
	)
}

func mustParseObservedScanWindowTraceTime(t *testing.T, value string) time.Time {
	t.Helper()

	ts, err := time.ParseInLocation(observedScanWindowTraceTimeLayout, value, time.Local)
	require.NoError(t, err)
	return ts
}

func observedTraceCadenceCounts(trace []observedScanWindowTracePoint) map[int]int {
	counts := make(map[int]int)
	for i := 1; i < len(trace); i++ {
		delta := trace[i].offsetSec - trace[i-1].offsetSec
		counts[delta]++
	}
	return counts
}

func dominantObservedTraceQuota(trace []observedScanWindowTracePoint) (int64, int) {
	counts := make(map[int64]int)
	for _, point := range trace {
		quotaBytes := int64(math.Round(point.observedMemoryQuotaBytes))
		counts[quotaBytes]++
	}

	var dominantQuotaBytes int64
	dominantQuotaCount := -1
	for quotaBytes, count := range counts {
		if count > dominantQuotaCount {
			dominantQuotaBytes = quotaBytes
			dominantQuotaCount = count
		}
	}
	return dominantQuotaBytes, dominantQuotaCount
}

func maxObservedTraceScanInterval(trace []observedScanWindowTracePoint) float64 {
	maxInterval := 0.0
	for _, point := range trace {
		maxInterval = maxFloat64(maxInterval, point.observedScanIntervalSec)
	}
	return maxInterval
}

func observedTraceScanIntervals(trace []observedScanWindowTracePoint) []float64 {
	intervals := make([]float64, 0, len(trace))
	for _, point := range trace {
		intervals = append(intervals, point.observedScanIntervalSec)
	}
	return intervals
}

func observedTracePercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}

	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)

	position := percentile * float64(len(sorted)-1)
	lowerIndex := int(math.Floor(position))
	upperIndex := int(math.Ceil(position))
	if lowerIndex == upperIndex {
		return sorted[lowerIndex]
	}

	weight := position - float64(lowerIndex)
	return sorted[lowerIndex]*(1-weight) + sorted[upperIndex]*weight
}

func findObservedTraceSeparatedPeaks(
	trace []observedScanWindowTracePoint,
	minPeakIntervalSec float64,
	minSeparation time.Duration,
) []observedScanWindowTracePoint {
	peaks := make([]observedScanWindowTracePoint, 0, 8)
	for i := 1; i < len(trace)-1; i++ {
		current := trace[i]
		if current.observedScanIntervalSec < minPeakIntervalSec {
			continue
		}
		if current.observedScanIntervalSec < trace[i-1].observedScanIntervalSec ||
			current.observedScanIntervalSec < trace[i+1].observedScanIntervalSec {
			continue
		}

		if len(peaks) == 0 || current.ts.Sub(peaks[len(peaks)-1].ts) >= minSeparation {
			peaks = append(peaks, current)
			continue
		}

		if current.observedScanIntervalSec > peaks[len(peaks)-1].observedScanIntervalSec {
			peaks[len(peaks)-1] = current
		}
	}
	return peaks
}

func findObservedTraceFloorCollapses(
	trace []observedScanWindowTracePoint,
	minPeakIntervalSec float64,
	lookback time.Duration,
	floorIntervalSec float64,
	minDropRatio float64,
) []observedScanWindowCollapse {
	collapses := make([]observedScanWindowCollapse, 0, 8)
	lookbackSec := int(lookback / time.Second)

	for i := 1; i < len(trace); i++ {
		floor := trace[i]
		if floor.observedScanIntervalSec > floorIntervalSec {
			continue
		}
		if trace[i-1].observedScanIntervalSec <= floorIntervalSec {
			continue
		}

		peakIndex := -1
		peakInterval := 0.0
		for j := i - 1; j >= 0; j-- {
			if floor.offsetSec-trace[j].offsetSec > lookbackSec {
				break
			}
			if trace[j].observedScanIntervalSec > peakInterval {
				peakInterval = trace[j].observedScanIntervalSec
				peakIndex = j
			}
		}

		if peakIndex == -1 || peakInterval < minPeakIntervalSec {
			continue
		}

		dropRatio := 1 - floor.observedScanIntervalSec/peakInterval
		if dropRatio < minDropRatio {
			continue
		}

		collapses = append(collapses, observedScanWindowCollapse{
			peak:      trace[peakIndex],
			floor:     floor,
			dropRatio: dropRatio,
		})
	}

	return collapses
}

func findObservedTracePointByTime(
	t *testing.T,
	trace []observedScanWindowTracePoint,
	timeText string,
) observedScanWindowTracePoint {
	t.Helper()

	target := mustParseObservedScanWindowTraceTime(t, timeText)
	for _, point := range trace {
		if point.ts.Equal(target) {
			return point
		}
	}

	t.Fatalf("trace point not found: %s", timeText)
	return observedScanWindowTracePoint{}
}

func selectObservedTraceWindow(
	t *testing.T,
	trace []observedScanWindowTracePoint,
	startText string,
	endText string,
) []observedScanWindowTracePoint {
	t.Helper()

	start := mustParseObservedScanWindowTraceTime(t, startText)
	end := mustParseObservedScanWindowTraceTime(t, endText)
	require.False(t, end.Before(start))

	window := make([]observedScanWindowTracePoint, 0, 32)
	for _, point := range trace {
		if point.ts.Before(start) || point.ts.After(end) {
			continue
		}
		window = append(window, point)
	}
	require.NotEmpty(t, window)
	return window
}

func countObservedTraceScanIntervalsAtLeast(trace []observedScanWindowTracePoint, thresholdSec float64) int {
	count := 0
	for _, point := range trace {
		if point.observedScanIntervalSec >= thresholdSec {
			count++
		}
	}
	return count
}

func maxObservedTraceResetRate(trace []observedScanWindowTracePoint) float64 {
	maxRate := 0.0
	for _, point := range trace {
		maxRate = maxFloat64(maxRate, point.observedResetDispatcherPerSec)
	}
	return maxRate
}
