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

package common

import (
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestApplyReturnsSinkInvalidConfigForQueryBindingError(t *testing.T) {
	cfg := NewConfig(config.ProtocolOpen)
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?max-batch-size=invalid")
	require.NoError(t, err)

	err = cfg.Apply(sinkURI, config.GetDefaultReplicaConfig().Sink)
	errCode, ok := errors.RFCCode(err)
	require.True(t, ok, err)
	require.Equal(t, errors.ErrSinkInvalidConfig.RFCCode(), errCode)
}

func TestValidateMaxBatchMessageBytes(t *testing.T) {
	tests := []struct {
		name     string
		adjust   func(*Config)
		expected string
	}{
		{
			name: "non-positive max message bytes",
			adjust: func(cfg *Config) {
				cfg.MaxMessageBytes = 0
			},
			expected: "invalid max-message-bytes 0",
		},
		{
			name: "negative max batched bytes",
			adjust: func(cfg *Config) {
				cfg.MaxBatchedBytes = -1
			},
			expected: "invalid max-batch-message-bytes -1",
		},
		{
			name: "max batched bytes exceeds max message bytes",
			adjust: func(cfg *Config) {
				cfg.MaxMessageBytes = 100
				cfg.MaxBatchedBytes = 101
			},
			expected: "max-batch-message-bytes 101 cannot be greater than max-message-bytes 100",
		},
		{
			name: "non-positive max batch size",
			adjust: func(cfg *Config) {
				cfg.MaxBatchSize = 0
			},
			expected: "invalid max-batch-size 0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := NewConfig(config.ProtocolOpen)
			test.adjust(cfg)

			err := cfg.Validate()
			require.ErrorContains(t, err, test.expected)
			errCode, ok := errors.RFCCode(err)
			require.True(t, ok, err)
			require.Equal(t, errors.ErrCodecInvalidConfig.RFCCode(), errCode)
		})
	}
}
