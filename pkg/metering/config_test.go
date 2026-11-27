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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metering

import (
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
		{"valid s3", config.NewMeteringConfig().WithS3("us-west-2", "bucket").WithEndpoint("https://s3.us-west-2.amazonaws.com").WithSharedPoolID("pool1"), true},
		{"valid oss", config.NewMeteringConfig().WithOSS("oss-cn-beijing", "bucket"), true},
		{"valid cos", config.NewMeteringConfig().WithCOS("ap-beijing", "bucket-123"), true},
		{"unsupported gcs", &config.MeteringConfig{Type: storage.ProviderTypeGCS, Bucket: "bucket"}, false},
		{"valid azure", config.NewMeteringConfig().WithAzure("acct", "container").WithEndpoint("https://acct.blob.core.windows.net"), true},
		{"valid localfs", config.NewMeteringConfig().WithLocalFS("/tmp/metering"), true},
		{"unsupported type", &config.MeteringConfig{Type: "unknown"}, false},
		{"s3 missing bucket", &config.MeteringConfig{Type: storage.ProviderTypeS3, Region: "us-west-2"}, false},
		{"s3 missing region", &config.MeteringConfig{Type: storage.ProviderTypeS3, Bucket: "bucket"}, false},
		{"s3 invalid endpoint", &config.MeteringConfig{Type: storage.ProviderTypeS3, Bucket: "bucket", Region: "us-west-2", Endpoint: "s3.amazonaws.com"}, false},
		{"azure missing account name", &config.MeteringConfig{Type: storage.ProviderTypeAzure, Bucket: "container"}, false},
		{"localfs missing base path", &config.MeteringConfig{Type: storage.ProviderTypeLocalFS}, false},
		{"pool traversal", config.NewMeteringConfig().WithS3("us-west-2", "bucket").WithSharedPoolID("../pool"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
