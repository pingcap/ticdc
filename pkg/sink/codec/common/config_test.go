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

	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD
func TestDebeziumAvroSchemaRegistryConfig(t *testing.T) {
	t.Parallel()
=======
func TestDebeziumNumericHandlingConfig(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fileConfig    string
		query         string
		decimalMode   string
		unsignedMode  string
		invalidConfig bool
	}{
		{name: "defaults", decimalMode: "double", unsignedMode: "long"},
		{
			name:         "URI",
			query:        "&debezium-decimal-handling-mode=string&debezium-bigint-unsigned-handling-mode=string",
			decimalMode:  "string",
			unsignedMode: "string",
		},
		{
			name:         "file",
			fileConfig:   "decimal-handling-mode = 'string'\nbigint-unsigned-handling-mode = 'string'",
			decimalMode:  "string",
			unsignedMode: "string",
		},
		{
			name:         "URI overrides file",
			fileConfig:   "decimal-handling-mode = 'string'\nbigint-unsigned-handling-mode = 'string'",
			query:        "&debezium-decimal-handling-mode=double&debezium-bigint-unsigned-handling-mode=long",
			decimalMode:  "double",
			unsignedMode: "long",
		},
		{
			name:          "empty decimal mode",
			query:         "&debezium-decimal-handling-mode=",
			unsignedMode:  "long",
			invalidConfig: true,
		},
		{
			name:          "empty bigint mode",
			query:         "&debezium-bigint-unsigned-handling-mode=",
			decimalMode:   "double",
			invalidConfig: true,
		},
		{
			name:          "empty decimal URI overrides file",
			fileConfig:    "decimal-handling-mode = 'string'\nbigint-unsigned-handling-mode = 'string'",
			query:         "&debezium-decimal-handling-mode=",
			unsignedMode:  "string",
			invalidConfig: true,
		},
		{
			name:          "empty bigint URI overrides file",
			fileConfig:    "decimal-handling-mode = 'string'\nbigint-unsigned-handling-mode = 'string'",
			query:         "&debezium-bigint-unsigned-handling-mode=",
			decimalMode:   "string",
			invalidConfig: true,
		},
		{
			name:          "invalid decimal mode",
			query:         "&debezium-decimal-handling-mode=invalid",
			decimalMode:   "invalid",
			unsignedMode:  "long",
			invalidConfig: true,
		},
		{
			name:          "invalid bigint mode",
			query:         "&debezium-bigint-unsigned-handling-mode=invalid",
			decimalMode:   "double",
			unsignedMode:  "invalid",
			invalidConfig: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaConfig := config.GetDefaultReplicaConfig()
			_, err := toml.Decode("[sink.debezium]\n"+tc.fileConfig, replicaConfig)
			require.NoError(t, err)
			sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium" + tc.query)
			require.NoError(t, err)
			cfg := NewConfig(config.ProtocolDebezium)
			require.NoError(t, cfg.Apply(sinkURI, replicaConfig.Sink))
			require.Equal(t, tc.decimalMode, cfg.DebeziumDecimalHandlingMode)
			require.Equal(t, tc.unsignedMode, cfg.DebeziumBigintUnsignedHandlingMode)
			if tc.invalidConfig {
				require.ErrorIs(t, cfg.Validate(), errors.ErrCodecInvalidConfig)
			} else {
				require.NoError(t, cfg.Validate())
			}
		})
	}

	for _, protocol := range []config.Protocol{config.ProtocolDebeziumAvro, config.ProtocolOpen} {
		t.Run(protocol.String(), func(t *testing.T) {
			cfg := NewConfig(protocol)
			cfg.DebeziumDecimalHandlingMode = DecimalHandlingModeString
			require.ErrorIs(t, cfg.Validate(), errors.ErrCodecInvalidConfig)
			cfg.DebeziumDecimalHandlingMode = "double"
			cfg.DebeziumBigintUnsignedHandlingMode = BigintUnsignedHandlingModeString
			require.ErrorIs(t, cfg.Validate(), errors.ErrCodecInvalidConfig)
		})
	}
}

func TestDebeziumBinaryHandlingConfig(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fileMode string
		query    string
		expected string
		invalid  bool
	}{
		{name: "default", expected: "base64"},
		{name: "bytes", query: "&debezium-binary-handling-mode=bytes", expected: "bytes"},
		{name: "base64", query: "&debezium-binary-handling-mode=base64", expected: "base64"},
		{name: "URL safe", query: "&debezium-binary-handling-mode=base64-url-safe", expected: "base64-url-safe"},
		{name: "hex", query: "&debezium-binary-handling-mode=hex", expected: "hex"},
		{name: "file", fileMode: "hex", expected: "hex"},
		{name: "URI overrides file", fileMode: "hex", query: "&debezium-binary-handling-mode=base64", expected: "base64"},
		{name: "invalid", query: "&debezium-binary-handling-mode=invalid", expected: "invalid", invalid: true},
		{name: "empty", query: "&debezium-binary-handling-mode=", invalid: true},
		{name: "empty URI overrides file", fileMode: "hex", query: "&debezium-binary-handling-mode=", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaConfig := config.GetDefaultReplicaConfig()
			if tc.fileMode != "" {
				_, err := toml.Decode("[sink.debezium]\nbinary-handling-mode = '"+tc.fileMode+"'", replicaConfig)
				require.NoError(t, err)
			}
			sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=debezium" + tc.query)
			require.NoError(t, err)
			cfg := NewConfig(config.ProtocolDebezium)
			require.NoError(t, cfg.Apply(sinkURI, replicaConfig.Sink))
			require.Equal(t, tc.expected, cfg.DebeziumBinaryHandlingMode)
			if tc.invalid {
				require.ErrorIs(t, cfg.Validate(), errors.ErrCodecInvalidConfig)
			} else {
				require.NoError(t, cfg.Validate())
			}
		})
	}
	for _, protocol := range []config.Protocol{config.ProtocolAvro, config.ProtocolDebeziumAvro, config.ProtocolOpen} {
		t.Run(protocol.String(), func(t *testing.T) {
			cfg := NewConfig(protocol)
			cfg.DebeziumBinaryHandlingMode = BinaryHandlingModeHex
			err := cfg.Validate()
			require.ErrorIs(t, err, errors.ErrCodecInvalidConfig)
			require.ErrorContains(t, err, "debezium-binary-handling-mode")
		})
	}
}

func TestAvroIncludeBeforeValueConfig(t *testing.T) {
	cfg := NewConfig(config.ProtocolAvro)
	require.False(t, cfg.AvroIncludeBeforeValue)
>>>>>>> d1a3a8dd1 ( sink: add Debezium numeric and binary handling modes (#6263))

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
