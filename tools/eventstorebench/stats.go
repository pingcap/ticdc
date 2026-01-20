package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

type benchStats struct {
	name   string
	start  time.Time
	events atomic.Uint64
	bytes  atomic.Uint64

	hist   *hdrhistogram.Histogram
	histMu sync.Mutex
}

func newBenchStats(name string) *benchStats {
	return &benchStats{
		name:  name,
		start: time.Now(),
		hist:  hdrhistogram.New(1, int64((10 * time.Second).Microseconds()), 3),
	}
}

func (s *benchStats) recordBatch(eventCount int, totalBytes int64, sendTime time.Time) func() {
	if eventCount > 0 {
		s.events.Add(uint64(eventCount))
	}
	if totalBytes > 0 {
		s.bytes.Add(uint64(totalBytes))
	}
	return func() {
		latency := time.Since(sendTime)
		micros := latency.Microseconds()
		if micros <= 0 {
			micros = 1
		}
		s.histMu.Lock()
		if err := s.hist.RecordValue(micros); err != nil {
			_ = s.hist.RecordValue(s.hist.HighestTrackableValue())
		}
		s.histMu.Unlock()
	}
}

func (s *benchStats) elapsed() time.Duration {
	return time.Since(s.start)
}

type benchResult struct {
	Scenario         benchScenario
	Duration         time.Duration
	Events           uint64
	Bytes            uint64
	ThroughputEvents float64
	ThroughputBytes  float64
	LatencyAvg       time.Duration
	LatencyP50       time.Duration
	LatencyP95       time.Duration
	LatencyP99       time.Duration
	LatencyMax       time.Duration
}

func (r benchResult) latencySummary() string {
	return formatDuration(r.LatencyP50) + "/" + formatDuration(r.LatencyP95) + "/" + formatDuration(r.LatencyP99) + "/" + formatDuration(r.LatencyMax)
}

func (r benchResult) avgLatencySummary() string {
	return formatDuration(r.LatencyAvg)
}

func (s *benchStats) snapshot(scenario benchScenario, duration time.Duration) benchResult {
	result := benchResult{
		Scenario: scenario,
		Duration: duration,
		Events:   s.events.Load(),
		Bytes:    s.bytes.Load(),
	}
	if duration > 0 {
		seconds := duration.Seconds()
		if seconds > 0 {
			result.ThroughputEvents = float64(result.Events) / seconds
			result.ThroughputBytes = float64(result.Bytes) / seconds
		}
	}

	s.histMu.Lock()
	if s.hist.TotalCount() > 0 {
		result.LatencyAvg = microsToDuration(int64(s.hist.Mean()))
		result.LatencyP50 = microsToDuration(s.hist.ValueAtQuantile(50))
		result.LatencyP95 = microsToDuration(s.hist.ValueAtQuantile(95))
		result.LatencyP99 = microsToDuration(s.hist.ValueAtQuantile(99))
		result.LatencyMax = microsToDuration(s.hist.Max())
	}
	s.histMu.Unlock()
	return result
}

func microsToDuration(value int64) time.Duration {
	if value <= 0 {
		return time.Microsecond
	}
	return time.Duration(value) * time.Microsecond
}

func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.1fµs", float64(d.Nanoseconds())/1e3)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1e3)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}
