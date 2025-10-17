// Copyright 2021 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/errors"
)

// DebugConfig represents config for ticdc unexposed feature configurations
type DebugConfig struct {
	DB *DBConfig `toml:"db" json:"db"`

	Messages *MessagesConfig `toml:"messages" json:"messages"`

	// Scheduler is the configuration of the two-phase scheduler.
	Scheduler *SchedulerConfig `toml:"scheduler" json:"scheduler"`

	// Puller is the configuration of the puller.
	Puller *PullerConfig `toml:"puller" json:"puller"`

	EventStore *EventStoreConfig `toml:"event-store" json:"event-store"`

	SchemaStore *SchemaStoreConfig `toml:"schema-store" json:"schema-store"`

	EventService *EventServiceConfig `toml:"event-service" json:"event-service"`
}

// ValidateAndAdjust validates and adjusts the debug configuration
func (c *DebugConfig) ValidateAndAdjust() error {
	if err := c.Messages.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if err := c.DB.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if err := c.Scheduler.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}
	if c.EventStore == nil {
		c.EventStore = NewDefaultEventStoreConfig()
	}

	return nil
}

// PullerConfig represents config for puller
type PullerConfig struct {
	// EnableResolvedTsStuckDetection is used to enable resolved ts stuck detection.
	EnableResolvedTsStuckDetection bool `toml:"enable-resolved-ts-stuck-detection" json:"enable-resolved-ts-stuck-detection"`
	// ResolvedTsStuckInterval is the interval of checking resolved ts stuck.
	ResolvedTsStuckInterval TomlDuration `toml:"resolved-ts-stuck-interval" json:"resolved-ts-stuck-interval"`
	// LogRegionDetails determines whether logs Region details or not in puller and kv-client.
	LogRegionDetails bool `toml:"log-region-details" json:"log-region-details"`

	// PendingRegionRequestQueueSize is the total size of the pending region request queue shared across
	// all puller workers connecting to a single TiKV store. This size is divided equally among all workers.
	// For example, if PendingRegionRequestQueueSize is 256 and there are 8 workers connecting to the same store,
	// each worker's queue size will be 256 / 8 = 32.
	PendingRegionRequestQueueSize int `toml:"pending-region-request-queue-size" json:"pending-region-request-queue-size"`
}

// NewDefaultPullerConfig return the default puller configuration
func NewDefaultPullerConfig() *PullerConfig {
	return &PullerConfig{
		EnableResolvedTsStuckDetection: false,
		ResolvedTsStuckInterval:        TomlDuration(5 * time.Minute),
		LogRegionDetails:               false,
		PendingRegionRequestQueueSize:  256, // Base on test result
	}
}

type EventStoreConfig struct {
	CompressionThreshold int `toml:"compression-threshold" json:"compression-threshold"`
}

// NewDefaultEventStoreConfig returns the default event store configuration.
func NewDefaultEventStoreConfig() *EventStoreConfig {
	return &EventStoreConfig{
		CompressionThreshold: 4096, // 4KB
	}
}

// SchemaStoreConfig represents config for schema store
type SchemaStoreConfig struct {
	EnableGC bool `toml:"enable-gc" json:"enable-gc"`
}

// NewDefaultSchemaStoreConfig return the default schema store configuration
func NewDefaultSchemaStoreConfig() *SchemaStoreConfig {
	return &SchemaStoreConfig{
		EnableGC: false,
	}
}

// EventServiceConfig represents config for event service
type EventServiceConfig struct {
	ScanTaskQueueSize int `toml:"scan-task-queue-size" json:"scan-task-queue-size"`
	ScanLimitInBytes  int `toml:"scan-limit-in-bytes" json:"scan-limit-in-bytes"`
}

// NewDefaultEventServiceConfig return the default event service configuration
func NewDefaultEventServiceConfig() *EventServiceConfig {
	return &EventServiceConfig{
		ScanTaskQueueSize: 1024 * 8,
		ScanLimitInBytes:  1024 * 1024 * 256, // 256MB
	}
}
