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

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/tidb/br/pkg/storage"
	pd "github.com/tikv/pd/client"
)

// mockPDClient simulates a PD server's TSO service.
// Each GetTS call returns a monotonically increasing physical timestamp.
type mockPDClient struct {
	pd.Client
	seq  atomic.Int64
	base int64 // base physical time in milliseconds
	step int64 // increment per call in milliseconds
}

func (m *mockPDClient) GetTS(_ context.Context) (int64, int64, error) {
	n := m.seq.Add(1)
	return m.base + n*m.step, 0, nil
}

func (m *mockPDClient) Close() {}

// ---------- Mock Checkpoint Watcher ----------

// mockWatcher simulates a checkpoint watcher.
// It always returns minCheckpointTs + delta, ensuring the result exceeds the minimum.
type mockWatcher struct {
	delta uint64
}

func (m *mockWatcher) AdvanceCheckpointTs(_ context.Context, minCheckpointTs uint64) (uint64, error) {
	return minCheckpointTs + m.delta, nil
}

func (m *mockWatcher) Close() {}

// MockMultiCluster manages the mock infrastructure for simulating multiple
// TiCDC clusters. It provides:
//   - Mock PD clients for TSO generation
//   - Mock checkpoint watchers for inter-cluster replication checkpoints
//   - Mock S3 checkpoint watchers and S3 watchers for cloud storage
//   - In-memory S3 storage for each cluster
//   - Helpers to write canal-JSON formatted data files
type MockMultiCluster struct {
	ClusterIDs []string
	Tables     map[string][]string // schema -> table names

	S3Storages map[string]storage.ExternalStorage
	pdClients  map[string]*mockPDClient
	CPWatchers map[string]map[string]watcher.Watcher
	S3Watchers map[string]*watcher.S3Watcher

	// fileCounters tracks the next DML file index per cluster.
	// Files are written with monotonically increasing indices so the
	// S3Consumer discovers only new files in each round.
	fileCounters map[string]uint64

	date string // fixed date used in all DML file paths
}

// NewMockMultiCluster creates a new mock multi-cluster environment.
//
// Parameters:
//   - clusterIDs: identifiers for the clusters (e.g. ["c1", "c2"])
//   - tables: schema -> table names mapping (e.g. {"test": ["t1"]})
//   - pdBase: base physical time (ms) for mock PD TSO generation
//   - pdStep: physical time increment (ms) per GetTS call
//   - cpDelta: checkpoint watcher returns minCheckpointTs + cpDelta
//   - s3Delta: S3 checkpoint watcher returns minCheckpointTs + s3Delta
func NewMockMultiCluster(
	clusterIDs []string,
	tables map[string][]string,
	pdBase, pdStep int64,
	cpDelta, s3Delta uint64,
) *MockMultiCluster {
	mc := &MockMultiCluster{
		ClusterIDs:   clusterIDs,
		Tables:       tables,
		S3Storages:   make(map[string]storage.ExternalStorage),
		pdClients:    make(map[string]*mockPDClient),
		CPWatchers:   make(map[string]map[string]watcher.Watcher),
		S3Watchers:   make(map[string]*watcher.S3Watcher),
		fileCounters: make(map[string]uint64),
		date:         "2026-02-11",
	}

	for _, id := range clusterIDs {
		mc.S3Storages[id] = storage.NewMemStorage()
		mc.pdClients[id] = &mockPDClient{base: pdBase, step: pdStep}

		// Checkpoint watchers: one per replicated cluster
		watchers := make(map[string]watcher.Watcher)
		for _, other := range clusterIDs {
			if other != id {
				watchers[other] = &mockWatcher{delta: cpDelta}
			}
		}
		mc.CPWatchers[id] = watchers

		// S3 watcher: uses in-memory storage + mock checkpoint watcher
		s3CpWatcher := &mockWatcher{delta: s3Delta}
		mc.S3Watchers[id] = watcher.NewS3Watcher(
			s3CpWatcher,
			mc.S3Storages[id],
			tables,
		)
	}

	return mc
}

// InitSchemaFiles writes initial schema files for all tables in all clusters.
// The schema file content is empty (parser is nil in the current implementation).
func (mc *MockMultiCluster) InitSchemaFiles(ctx context.Context) error {
	for _, s3 := range mc.S3Storages {
		for schema, tableList := range mc.Tables {
			for _, table := range tableList {
				path := fmt.Sprintf("%s/%s/meta/schema_1_0000000000.json", schema, table)
				if err := s3.WriteFile(ctx, path, []byte("{}")); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// WriteDMLFile writes a canal-JSON DML file to a cluster's S3 storage.
// Each call increments the file index for that cluster, ensuring the
// S3Consumer discovers it as a new file.
func (mc *MockMultiCluster) WriteDMLFile(ctx context.Context, clusterID string, content []byte) error {
	mc.fileCounters[clusterID]++
	idx := mc.fileCounters[clusterID]
	for schema, tableList := range mc.Tables {
		for _, table := range tableList {
			path := fmt.Sprintf("%s/%s/1/%s/CDC%020d.json", schema, table, mc.date, idx)
			if err := mc.S3Storages[clusterID].WriteFile(ctx, path, content); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetPDClients returns mock PD clients as the pd.Client interface.
func (mc *MockMultiCluster) GetPDClients() map[string]pd.Client {
	clients := make(map[string]pd.Client)
	for id, c := range mc.pdClients {
		clients[id] = c
	}
	return clients
}

// Close closes all S3 watchers.
func (mc *MockMultiCluster) Close() {
	for _, sw := range mc.S3Watchers {
		sw.Close()
	}
}

// MakeCanalJSON builds a canal-JSON formatted record for testing.
//
// Parameters:
//   - pkID: primary key value (int column "id")
//   - commitTs: TiDB commit timestamp
//   - originTs: origin timestamp (0 for locally-written records, non-zero for replicated records)
//   - val: value for the "val" varchar column
func MakeCanalJSON(pkID int, commitTs uint64, originTs uint64, val string) string {
	originTsVal := "null"
	if originTs > 0 {
		originTsVal = fmt.Sprintf(`"%d"`, originTs)
	}
	return fmt.Sprintf(
		`{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT",`+
			`"es":0,"ts":0,"sql":"","sqlType":{"id":4,"val":12,"_tidb_origin_ts":-5},`+
			`"mysqlType":{"id":"int","val":"varchar","_tidb_origin_ts":"bigint"},`+
			`"old":null,"data":[{"id":"%d","val":"%s","_tidb_origin_ts":%s}],`+
			`"_tidb":{"commitTs":%d}}`,
		pkID, val, originTsVal, commitTs)
}

// MakeContent combines canal-JSON records with CRLF terminator (matching codec config).
func MakeContent(records ...string) []byte {
	return []byte(strings.Join(records, "\r\n"))
}
