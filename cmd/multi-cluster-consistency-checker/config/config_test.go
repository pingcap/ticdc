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

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	t.Run("valid config", func(t *testing.T) {
		t.Parallel()
		// Create a temporary config file
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1", "table2"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "info", cfg.GlobalConfig.LogLevel)
		require.Equal(t, "/tmp/data", cfg.GlobalConfig.DataDir)
		require.Len(t, cfg.Clusters, 2)
		require.Contains(t, cfg.Clusters, "cluster1")
		require.Contains(t, cfg.Clusters, "cluster2")
		require.Equal(t, []string{"127.0.0.1:2379"}, cfg.Clusters["cluster1"].PDAddrs)
		require.Equal(t, "s3://bucket/cluster1/", cfg.Clusters["cluster1"].S3SinkURI)
		require.Equal(t, "s3-cf-1", cfg.Clusters["cluster1"].S3ChangefeedID)
		require.Len(t, cfg.Clusters["cluster1"].PeerClusterChangefeedConfig, 1)
		require.Equal(t, "cf-1-to-2", cfg.Clusters["cluster1"].PeerClusterChangefeedConfig["cluster2"].ChangefeedID)
		// max-report-files not set, should default to DefaultMaxReportFiles
		require.Equal(t, DefaultMaxReportFiles, cfg.GlobalConfig.MaxReportFiles)
	})

	t.Run("custom max-report-files", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
max-report-files = 50
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.NoError(t, err)
		require.Equal(t, 50, cfg.GlobalConfig.MaxReportFiles)
	})

	t.Run("file not exists", func(t *testing.T) {
		t.Parallel()
		cfg, err := LoadConfig("/nonexistent/path/config.toml")
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "config file does not exist")
	})

	t.Run("invalid toml", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `invalid toml content [`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "failed to decode config file")
	})

	t.Run("missing data-dir", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "data-dir is required")
	})

	t.Run("missing tables", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "at least one schema must be configured in tables")
	})

	t.Run("empty table list in schema", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = []

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "at least one table must be configured")
	})

	t.Run("no clusters", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "at least one cluster must be configured")
	})

	t.Run("missing pd-addrs", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "pd-addrs is required")
	})

	t.Run("missing s3-sink-uri", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-changefeed-id = "s3-cf-1"
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "s3-sink-uri is required")
	})

	t.Run("missing s3-changefeed-id", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "s3-changefeed-id is required")
	})

	t.Run("incomplete replicated cluster changefeed config", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = { changefeed-id = "cf-1-to-2" }

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "peer-cluster-changefeed-config is not entirely configured")
	})

	t.Run("missing changefeed-id in peer cluster config", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, "config.toml")
		configContent := `
[global]
log-level = "info"
data-dir = "/tmp/data"
  [global.tables]
  schema1 = ["table1"]

[clusters]
  [clusters.cluster1]
  pd-addrs = ["127.0.0.1:2379"]
  s3-sink-uri = "s3://bucket/cluster1/"
  s3-changefeed-id = "s3-cf-1"
  [clusters.cluster1.peer-cluster-changefeed-config]
  cluster2 = {}

  [clusters.cluster2]
  pd-addrs = ["127.0.0.1:2479"]
  s3-sink-uri = "s3://bucket/cluster2/"
  s3-changefeed-id = "s3-cf-2"
  [clusters.cluster2.peer-cluster-changefeed-config]
  cluster1 = { changefeed-id = "cf-2-to-1" }
`
		err := os.WriteFile(configPath, []byte(configContent), 0644)
		require.NoError(t, err)

		cfg, err := LoadConfig(configPath)
		require.Error(t, err)
		require.Nil(t, cfg)
		require.Contains(t, err.Error(), "changefeed-id is required")
	})
}
