package main

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
)

// benchScenario describes a predefined benchmarking case.
type benchScenario struct {
	Name            string
	Description     string
	TableCount      int
	TableIDBase     int64
	WritersPerTable int
	BatchSize       int
	PayloadBytes    int
	OldValueBytes   int
	UpdateEvery     int
	ResolvedLag     uint64
	StartTS         uint64
}

func (s benchScenario) spanList() []*heartbeatpb.TableSpan {
	base := s.TableIDBase
	if base == 0 {
		base = 1
	}
	count := s.TableCount
	if count <= 0 {
		count = 1
	}
	result := make([]*heartbeatpb.TableSpan, 0, count)
	for i := 0; i < count; i++ {
		tableID := base + int64(i)
		result = append(result, &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: encodeSpanBoundary(tableID, false),
			EndKey:   encodeSpanBoundary(tableID, true),
		})
	}
	return result
}

func (s benchScenario) batchSize() int {
	if s.BatchSize <= 0 {
		return 128
	}
	return s.BatchSize
}

func (s benchScenario) writers() int {
	if s.WritersPerTable <= 0 {
		return 1
	}
	return s.WritersPerTable
}

func (s benchScenario) updateEvery() int {
	if s.UpdateEvery <= 0 {
		return 0
	}
	return s.UpdateEvery
}

func (s benchScenario) resolvedLag() uint64 {
	if s.ResolvedLag == 0 {
		return 64
	}
	return s.ResolvedLag
}

func (s benchScenario) startTS() uint64 {
	if s.StartTS == 0 {
		return 100
	}
	return s.StartTS
}

func (s benchScenario) payloadSize() int {
	if s.PayloadBytes <= 0 {
		return 256
	}
	return s.PayloadBytes
}

func (s benchScenario) oldValueSize() int {
	if s.OldValueBytes <= 0 {
		return 0
	}
	return s.OldValueBytes
}

func (s benchScenario) formatHeading() string {
	return fmt.Sprintf("%s (%s)", s.Name, s.Description)
}

func encodeSpanBoundary(tableID int64, end bool) []byte {
	buf := make([]byte, 9)
	binary.BigEndian.PutUint64(buf[:8], uint64(tableID))
	if end {
		buf[8] = 0xFF
	}
	return buf
}

func parseScenarioNames(input string) []string {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil
	}
	items := strings.Split(input, ",")
	result := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		result = append(result, strings.ToLower(item))
	}
	return result
}

func defaultBenchScenarios() map[string]benchScenario {
	return map[string]benchScenario{
		"single-table": {
			Name:            "single-table",
			Description:     "one table with balanced payload",
			TableCount:      1,
			WritersPerTable: 8,
			BatchSize:       256,
			PayloadBytes:    512,
			OldValueBytes:   0,
			UpdateEvery:     0,
			ResolvedLag:     256,
			StartTS:         100,
		},
		"multi-table": {
			Name:            "multi-table",
			Description:     "many small tables in parallel",
			TableCount:      32,
			TableIDBase:     1000,
			WritersPerTable: 2,
			BatchSize:       128,
			PayloadBytes:    256,
			ResolvedLag:     512,
			StartTS:         200,
		},
		"wide-table": {
			Name:            "wide-table",
			Description:     "large rows with update workloads",
			TableCount:      1,
			WritersPerTable: 4,
			BatchSize:       64,
			PayloadBytes:    4096,
			OldValueBytes:   4096,
			UpdateEvery:     1,
			ResolvedLag:     1024,
			StartTS:         300,
		},
		"narrow-table": {
			Name:            "narrow-table",
			Description:     "small rows at high concurrency",
			TableCount:      1,
			WritersPerTable: 12,
			BatchSize:       512,
			PayloadBytes:    128,
			ResolvedLag:     128,
			StartTS:         150,
		},
	}
}

func (s benchScenario) advanceTicker(intervalMs int64) time.Duration {
	if intervalMs <= 0 {
		return 200 * time.Millisecond
	}
	return time.Duration(intervalMs) * time.Millisecond
}
