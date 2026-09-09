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

package metering

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.MeteringConfig
		ok   bool
	}{
		{"disabled nil", nil, true},
		{"disabled empty", &config.MeteringConfig{}, true},
		{"missing localfs options", &config.MeteringConfig{Type: storage.ProviderTypeLocalFS}, false},
		{"localfs", config.NewMeteringConfig().WithLocalFS(t.TempDir()), true},
		{"s3", config.NewMeteringConfig().WithS3("us-west-2", "bucket"), true},
		{"s3 missing bucket", config.NewMeteringConfig().WithS3("us-west-2", ""), false},
		{"oss", config.NewMeteringConfig().WithOSS("region", "bucket"), true},
		{"cos endpoint", &config.MeteringConfig{Type: storage.ProviderTypeCOS, Bucket: "bucket", Endpoint: "https://cos.example.com"}, true},
		{"cos missing region and endpoint", &config.MeteringConfig{Type: storage.ProviderTypeCOS, Bucket: "bucket"}, false},
		{"azure", config.NewMeteringConfig().WithAzure("account", "container"), true},
		{"unsupported", &config.MeteringConfig{Type: "unknown"}, false},
		{"pool traversal", config.NewMeteringConfig().WithS3("region", "bucket").WithSharedPoolID("../pool"), false},
		{"endpoint credentials", config.NewMeteringConfig().WithS3("region", "bucket").WithEndpoint("https://user:secret@example.com"), false},
		{"endpoint token", config.NewMeteringConfig().WithS3("region", "bucket").WithEndpoint("https://example.com?token=secret"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret")
			}
		})
	}
}

func TestRedactConfig(t *testing.T) {
	c := &config.MeteringConfig{
		Endpoint: "https://user:secret@example.com?token=secret#secret",
		AWS:      &config.MeteringAWSConfig{AccessKey: "secret", SecretAccessKey: "secret", SessionToken: "secret"},
		OSS:      &config.MeteringOSSConfig{AccessKey: "secret", SecretAccessKey: "secret", SessionToken: "secret"},
		COS:      &config.MeteringCOSConfig{AccessKey: "secret", SecretAccessKey: "secret", SessionToken: "secret"},
		Azure:    &config.MeteringAzureConfig{AccountKey: "secret", SASToken: "secret"},
	}
	before, err := json.Marshal(c)
	require.NoError(t, err)
	redacted := RedactConfig(c)
	data, err := json.Marshal(redacted)
	require.NoError(t, err)
	require.NotContains(t, string(data), "secret")
	require.Equal(t, "https://example.com", redacted.Endpoint)
	after, err := json.Marshal(c)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Nil(t, RedactConfig(nil))
}
