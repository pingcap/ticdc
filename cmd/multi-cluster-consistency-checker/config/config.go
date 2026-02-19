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
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/security"
)

// Config represents the configuration for multi-cluster consistency checker
type Config struct {
	// GlobalConfig contains global settings (reserved for future use)
	GlobalConfig GlobalConfig `toml:"global" json:"global"`

	// Clusters contains configurations for multiple clusters
	Clusters map[string]ClusterConfig `toml:"clusters" json:"clusters"`
}

const DefaultMaxReportFiles = 1000

// GlobalConfig contains global configuration settings
type GlobalConfig struct {
	LogLevel       string              `toml:"log-level" json:"log-level"`
	DataDir        string              `toml:"data-dir" json:"data-dir"`
	MaxReportFiles int                 `toml:"max-report-files" json:"max-report-files"`
	Tables         map[string][]string `toml:"tables" json:"tables"`
}

type PeerClusterChangefeedConfig struct {
	// ChangefeedID is the changefeed ID for the changefeed
	ChangefeedID string `toml:"changefeed-id" json:"changefeed-id"`
}

// ClusterConfig represents configuration for a single cluster
type ClusterConfig struct {
	// PDAddrs is the addresses of the PD (Placement Driver) servers
	PDAddrs []string `toml:"pd-addrs" json:"pd-addrs"`

	// S3SinkURI is the S3 sink URI for this cluster
	S3SinkURI string `toml:"s3-sink-uri" json:"s3-sink-uri"`

	// S3ChangefeedID is the changefeed ID for the S3 changefeed
	S3ChangefeedID string `toml:"s3-changefeed-id" json:"s3-changefeed-id"`

	// SecurityConfig is the security configuration for the cluster
	SecurityConfig security.Credential `toml:"security-config" json:"security-config"`

	// PeerClusterChangefeedConfig is the configuration for the changefeed of the peer cluster
	// mapping from peer cluster ID to the changefeed configuration
	PeerClusterChangefeedConfig map[string]PeerClusterChangefeedConfig `toml:"peer-cluster-changefeed-config" json:"peer-cluster-changefeed-config"`
}

// loadConfig loads the configuration from a TOML file
func LoadConfig(path string) (*Config, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s", path)
	}

	cfg := &Config{
		Clusters: make(map[string]ClusterConfig),
	}

	meta, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}

	// Apply defaults
	if cfg.GlobalConfig.MaxReportFiles <= 0 {
		cfg.GlobalConfig.MaxReportFiles = DefaultMaxReportFiles
	}

	// Validate DataDir
	if cfg.GlobalConfig.DataDir == "" {
		return nil, fmt.Errorf("global: data-dir is required")
	}

	// Validate Tables
	if len(cfg.GlobalConfig.Tables) == 0 {
		return nil, fmt.Errorf("global: at least one schema must be configured in tables")
	}
	for schema, tables := range cfg.GlobalConfig.Tables {
		if len(tables) == 0 {
			return nil, fmt.Errorf("global: tables[%s]: at least one table must be configured", schema)
		}
	}

	// Validate that at least one cluster is configured
	if len(cfg.Clusters) == 0 {
		return nil, fmt.Errorf("at least one cluster must be configured")
	}

	// Validate cluster configurations
	for name, cluster := range cfg.Clusters {
		if len(cluster.PDAddrs) == 0 {
			return nil, fmt.Errorf("cluster '%s': pd-addrs is required", name)
		}
		if cluster.S3SinkURI == "" {
			return nil, fmt.Errorf("cluster '%s': s3-sink-uri is required", name)
		}
		if cluster.S3ChangefeedID == "" {
			return nil, fmt.Errorf("cluster '%s': s3-changefeed-id is required", name)
		}
		if len(cluster.PeerClusterChangefeedConfig) != len(cfg.Clusters)-1 {
			return nil, fmt.Errorf("cluster '%s': peer-cluster-changefeed-config is not entirely configured", name)
		}
		for peerClusterID, peerClusterChangefeedConfig := range cluster.PeerClusterChangefeedConfig {
			if peerClusterID == name {
				return nil, fmt.Errorf("cluster '%s': peer-cluster-changefeed-config references itself", name)
			}
			if _, ok := cfg.Clusters[peerClusterID]; !ok {
				return nil, fmt.Errorf("cluster '%s': peer-cluster-changefeed-config references unknown cluster '%s'", name, peerClusterID)
			}
			if peerClusterChangefeedConfig.ChangefeedID == "" {
				return nil, fmt.Errorf("cluster '%s': peer-cluster-changefeed-config[%s]: changefeed-id is required", name, peerClusterID)
			}
		}
	}

	// Check for unknown configuration keys
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		// Filter out keys under [global] and [clusters] sections
		var unknownKeys []string
		for _, key := range undecoded {
			keyStr := key.String()
			// Only warn about keys that are not in the expected sections
			if keyStr != "global" && keyStr != "clusters" {
				unknownKeys = append(unknownKeys, keyStr)
			}
		}
		if len(unknownKeys) > 0 {
			fmt.Fprintf(os.Stderr, "Warning: unknown configuration keys found: %v\n", unknownKeys)
		}
	}

	return cfg, nil
}
