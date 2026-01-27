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

	EventStore *EventStoreConfig `toml:"event-store" json:"event_store"`

	SchemaStore *SchemaStoreConfig `toml:"schema-store" json:"schema_store"`

	EventService *EventServiceConfig `toml:"event-service" json:"event_service"`

	// Encryption is the configuration for CMEK encryption at rest
	Encryption *EncryptionConfig `toml:"encryption" json:"encryption"`
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
	if c.Encryption == nil {
		c.Encryption = NewDefaultEncryptionConfig()
	}

	return nil
}

// PullerConfig represents config for puller
type PullerConfig struct {
	// EnableResolvedTsStuckDetection is used to enable resolved ts stuck detection.
	EnableResolvedTsStuckDetection bool `toml:"enable-resolved-ts-stuck-detection" json:"enable_resolved_ts_stuck_detection"`
	// ResolvedTsStuckInterval is the interval of checking resolved ts stuck.
	ResolvedTsStuckInterval TomlDuration `toml:"resolved-ts-stuck-interval" json:"resolved_ts_stuck_interval"`
	// LogRegionDetails determines whether logs Region details or not in puller and kv-client.
	LogRegionDetails bool `toml:"log-region-details" json:"log_region_details"`

	// PendingRegionRequestQueueSize is the total size of the pending region request queue shared across
	// all puller workers connecting to a single TiKV store. This size is divided equally among all workers.
	// For example, if PendingRegionRequestQueueSize is 32 and there are 8 workers connecting to the same store,
	// each worker's queue size will be 32 / 8 = 4.
	PendingRegionRequestQueueSize int `toml:"pending-region-request-queue-size" json:"pending_region_request_queue_size"`
}

// NewDefaultPullerConfig return the default puller configuration
func NewDefaultPullerConfig() *PullerConfig {
	return &PullerConfig{
		EnableResolvedTsStuckDetection: false,
		ResolvedTsStuckInterval:        TomlDuration(5 * time.Minute),
		LogRegionDetails:               false,
		PendingRegionRequestQueueSize:  32, // This value is chosen to reduce the impact of new changefeeds on existing ones.
	}
}

type EventStoreConfig struct {
	CompressionThreshold int `toml:"compression-threshold" json:"compression_threshold"`

	EnableDataSharing bool `toml:"enable-data-sharing" json:"enable_data_sharing"`
}

// NewDefaultEventStoreConfig returns the default event store configuration.
func NewDefaultEventStoreConfig() *EventStoreConfig {
	return &EventStoreConfig{
		CompressionThreshold: 16384, // 16KB
		EnableDataSharing:    false,
	}
}

// SchemaStoreConfig represents config for schema store
type SchemaStoreConfig struct {
	EnableGC bool `toml:"enable-gc" json:"enable_gc"`

	// IgnoreDDLCommitTs is a list of commit ts of ddl jobs to be ignored by schema store.
	IgnoreDDLCommitTs []uint64 `toml:"ignore-ddl-commit-ts" json:"ignore_ddl_commit_ts"`
}

// NewDefaultSchemaStoreConfig return the default schema store configuration
func NewDefaultSchemaStoreConfig() *SchemaStoreConfig {
	return &SchemaStoreConfig{
		EnableGC:          false,
		IgnoreDDLCommitTs: []uint64{},
	}
}

// EventServiceConfig represents config for event service
type EventServiceConfig struct {
	ScanTaskQueueSize int `toml:"scan-task-queue-size" json:"scan_task_queue_size"`
	ScanLimitInBytes  int `toml:"scan-limit-in-bytes" json:"scan_limit_in_bytes"`

	// DMLEventMaxRows is the maximum number of rows in a DML event when split txn is enabled.
	DMLEventMaxRows int32 `toml:"dml-event-max-rows" json:"dml_event_max_rows"`
	// DMLEventMaxBytes is the maximum size of a DML event in bytes when split txn is enabled.
	DMLEventMaxBytes int64 `toml:"dml-event-max-bytes" json:"dml_event_max_bytes"`

	// FIXME: For now we found cdc may OOM when there is a large amount of events to be sent to event collector from a remote event service.
	// So we add this config to be able to disable remote event service in such scenario.
	// TODO: Remove this config after we find a proper way to fix the OOM issue.
	// Ref: https://github.com/pingcap/ticdc/issues/1784
	EnableRemoteEventService bool `toml:"enable-remote-event-service" json:"enable_remote_event_service"`
}

// NewDefaultEventServiceConfig return the default event service configuration
func NewDefaultEventServiceConfig() *EventServiceConfig {
	return &EventServiceConfig{
		ScanTaskQueueSize:        1024 * 8,
		ScanLimitInBytes:         1024 * 1024 * 256, // 256MB
		DMLEventMaxRows:          256,
		DMLEventMaxBytes:         1024 * 1024 * 1, // 1MB
		EnableRemoteEventService: true,
	}
}

// EncryptionConfig represents config for CMEK encryption at rest
type EncryptionConfig struct {
	// EnableEncryption enables encryption for data at rest
	EnableEncryption bool `toml:"enable-encryption" json:"enable_encryption"`

	// MetaRefreshInterval is the interval for refreshing encryption metadata (default: 1 hour)
	MetaRefreshInterval TomlDuration `toml:"meta-refresh-interval" json:"meta_refresh_interval"`

	// MetaCacheTTL is the TTL for caching encryption metadata (default: 1 hour)
	MetaCacheTTL TomlDuration `toml:"meta-cache-ttl" json:"meta_cache_ttl"`

	// AllowDegradeOnError allows graceful degradation to unencrypted mode on encryption errors
	AllowDegradeOnError bool `toml:"allow-degrade-on-error" json:"allow_degrade_on_error"`

	// KMS contains optional KMS client overrides. If unset, TiCDC will use the
	// default credential chain of the corresponding cloud provider.
	KMS *KMSConfig `toml:"kms" json:"kms"`
}

// KMSConfig contains KMS configuration for different cloud providers.
type KMSConfig struct {
	AWS *AWSKMSConfig `toml:"aws" json:"aws"`
	GCP *GCPKMSConfig `toml:"gcp" json:"gcp"`
}

type AWSKMSConfig struct {
	// Region overrides the region from TiKV encryption metadata.
	Region string `toml:"region" json:"region"`
	// Endpoint overrides the endpoint from TiKV encryption metadata.
	Endpoint string `toml:"endpoint" json:"endpoint"`

	// Profile configures the AWS shared config profile to use.
	Profile string `toml:"profile" json:"profile"`

	// Static credentials. If AccessKey is set, SecretAccessKey must also be set.
	AccessKey       string `toml:"access-key" json:"access_key"`
	SecretAccessKey string `toml:"secret-access-key" json:"secret_access_key"`
	SessionToken    string `toml:"session-token" json:"session_token"`
}

type GCPKMSConfig struct {
	// Endpoint overrides the endpoint from TiKV encryption metadata.
	Endpoint string `toml:"endpoint" json:"endpoint"`

	// CredentialsFile specifies a service account JSON file path.
	CredentialsFile string `toml:"credentials-file" json:"credentials_file"`
	// CredentialsJSON specifies a service account JSON content.
	CredentialsJSON string `toml:"credentials-json" json:"credentials_json"`
}

// NewDefaultEncryptionConfig returns the default encryption configuration
func NewDefaultEncryptionConfig() *EncryptionConfig {
	return &EncryptionConfig{
		EnableEncryption:    false,
		MetaRefreshInterval: TomlDuration(1 * time.Hour),
		MetaCacheTTL:        TomlDuration(1 * time.Hour),
		AllowDegradeOnError: true,
		KMS: &KMSConfig{
			AWS: &AWSKMSConfig{},
			GCP: &GCPKMSConfig{},
		},
	}
}
