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
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Config represents the configuration for multi-cluster consistency checker
type Config struct {
	// GlobalConfig contains global settings (reserved for future use)
	GlobalConfig GlobalConfig `toml:"global" json:"global"`

	// Clusters contains configurations for multiple clusters
	Clusters map[string]ClusterConfig `toml:"clusters" json:"clusters"`
}

// GlobalConfig contains global configuration settings
// This is reserved for future use
type GlobalConfig struct {
	// Add global configuration fields here as needed
	// For example:
	// Timeout     time.Duration `toml:"timeout" json:"timeout"`
	// RetryCount  int           `toml:"retry-count" json:"retry-count"`
}

// ClusterConfig represents configuration for a single cluster
type ClusterConfig struct {
	// PDAddr is the address of the PD (Placement Driver) server
	PDAddr string `toml:"pd-addr" json:"pd-addr"`

	// CDCAddr is the address of the CDC server
	CDCAddr string `toml:"cdc-addr" json:"cdc-addr"`

	// S3SinkURI is the S3 sink URI for this cluster
	S3SinkURI string `toml:"s3-sink-uri" json:"s3-sink-uri"`
}

// loadConfig loads the configuration from a TOML file
func loadConfig(path string) (*Config, error) {
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

	// Validate that at least one cluster is configured
	if len(cfg.Clusters) == 0 {
		return nil, fmt.Errorf("at least one cluster must be configured")
	}

	// Validate cluster configurations
	for name, cluster := range cfg.Clusters {
		if cluster.PDAddr == "" {
			return nil, fmt.Errorf("cluster '%s': pd-addr is required", name)
		}
		if cluster.CDCAddr == "" {
			return nil, fmt.Errorf("cluster '%s': cdc-addr is required", name)
		}
		if cluster.S3SinkURI == "" {
			return nil, fmt.Errorf("cluster '%s': s3-sink-uri is required", name)
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
