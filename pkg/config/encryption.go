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

import "time"

// EncryptionConfig represents config for CMEK encryption at rest.
type EncryptionConfig struct {
	// EnableEncryption enables encryption for data at rest.
	EnableEncryption bool `toml:"enable-encryption" json:"enable_encryption"`

	// MetaRefreshInterval is the interval for refreshing encryption metadata.
	MetaRefreshInterval TomlDuration `toml:"meta-refresh-interval" json:"meta_refresh_interval"`

	// MetaCacheTTL is the TTL for caching encryption metadata.
	MetaCacheTTL TomlDuration `toml:"meta-cache-ttl" json:"meta_cache_ttl"`

	// AllowDegradeOnError allows graceful degradation to unencrypted mode on encryption errors.
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

// NewDefaultEncryptionConfig returns the default encryption configuration.
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
