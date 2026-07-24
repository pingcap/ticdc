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
	"github.com/pingcap/ticdc/pkg/util"
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

func TestDebeziumAvroSchemaRegistryConfig(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	require.NoError(t, cfg.Validate())

	cfg = NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroGlueSchemaRegistry = &config.GlueSchemaRegistryConfig{
		RegistryName: "test-registry",
		Region:       "us-east-1",
	}
	require.NoError(t, cfg.Validate())

	cfg = NewConfig(config.ProtocolDebeziumAvro)
	require.ErrorContains(
		t,
		cfg.Validate(),
		`Debezium Avro protocol requires parameter "schema-registry" or "glue-schema-registry"`,
	)

	cfg = NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroGlueSchemaRegistry = &config.GlueSchemaRegistryConfig{}
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	require.ErrorContains(
		t,
		cfg.Validate(),
		`Debezium Avro protocol requires only one of "schema-registry" or "glue-schema-registry"`,
	)

	cfg = NewConfig(config.ProtocolDebezium)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	require.ErrorContains(t, cfg.Validate(), `Debezium protocol does not support schema registry`)
}

func TestDebeziumAvroGlueSchemaRegistryConfig(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(config.ProtocolDebeziumAvro)
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium-avro")
	require.NoError(t, err)

	glueSchemaRegistryConfig := &config.GlueSchemaRegistryConfig{
		RegistryName: "test-registry",
		Region:       "us-east-1",
	}
	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.KafkaConfig = &config.KafkaConfig{
		GlueSchemaRegistryConfig: glueSchemaRegistryConfig,
	}

	err = cfg.Apply(sinkURI, sinkConfig)
	require.NoError(t, err)
	require.Same(t, glueSchemaRegistryConfig, cfg.AvroGlueSchemaRegistry)
	require.Empty(t, cfg.AvroConfluentSchemaRegistry)
	require.NoError(t, cfg.Validate())
}

func TestDebeziumAvroWatermarkConfig(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(config.ProtocolDebeziumAvro)
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium-avro&enable-tidb-extension=true&avro-enable-watermark=true")
	require.NoError(t, err)

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.SchemaRegistry = util.AddressOf("http://127.0.0.1:8081")
	err = cfg.Apply(sinkURI, sinkConfig)
	require.NoError(t, err)
	require.True(t, cfg.EnableTiDBExtension)
	require.True(t, cfg.AvroEnableWatermark)
	require.Equal(t, "http://127.0.0.1:8081", cfg.AvroConfluentSchemaRegistry)
}
