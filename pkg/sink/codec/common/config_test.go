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

func TestValidateMessageLimits(t *testing.T) {
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

func TestDebeziumIncludeStartTsConfig(t *testing.T) {
	// URI parameter
	cfg := NewConfig(config.ProtocolDebezium)
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium&debezium-include-start-ts=true")
	require.NoError(t, err)
	require.NoError(t, cfg.Apply(sinkURI, config.GetDefaultReplicaConfig().Sink))
	require.True(t, cfg.DebeziumIncludeStartTs)
	require.NoError(t, cfg.Validate())

	// changefeed config file
	on := true
	cfg2 := NewConfig(config.ProtocolDebezium)
	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.Debezium.IncludeStartTs = &on
	sinkURI2, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium")
	require.NoError(t, err)
	require.NoError(t, cfg2.Apply(sinkURI2, sinkConfig))
	require.True(t, cfg2.DebeziumIncludeStartTs)

	// URI parameter overrides the config file
	cfg3 := NewConfig(config.ProtocolDebezium)
	sinkConfig3 := config.GetDefaultReplicaConfig().Sink
	sinkConfig3.Debezium.IncludeStartTs = &on
	sinkURI3, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium&debezium-include-start-ts=false")
	require.NoError(t, err)
	require.NoError(t, cfg3.Apply(sinkURI3, sinkConfig3))
	require.False(t, cfg3.DebeziumIncludeStartTs)

	// only supported by the debezium (JSON) protocol
	cfg4 := NewConfig(config.ProtocolCanalJSON)
	cfg4.DebeziumIncludeStartTs = true
	errCode, ok := errors.RFCCode(cfg4.Validate())
	require.True(t, ok)
	require.Equal(t, errors.ErrCodecInvalidConfig.RFCCode(), errCode)
}
