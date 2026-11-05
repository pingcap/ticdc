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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

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

	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=avro&avro-include-before-value=true")
	require.NoError(t, err)

	err = cfg.Apply(sinkURI, &config.SinkConfig{})
	require.NoError(t, err)
	require.False(t, cfg.EnableTiDBExtension)
	require.True(t, cfg.AvroIncludeBeforeValue)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	require.NoError(t, cfg.Validate())
}

func TestAvroIncludeBeforeValueConfigFile(t *testing.T) {
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=avro")
	require.NoError(t, err)

	cfg := NewConfig(config.ProtocolAvro)
	err = cfg.Apply(sinkURI, &config.SinkConfig{
		KafkaConfig: &config.KafkaConfig{
			CodecConfig: &config.CodecConfig{
				AvroIncludeBeforeValue: util.AddressOf(true),
			},
		},
	})
	require.NoError(t, err)
	require.False(t, cfg.EnableTiDBExtension)
	require.True(t, cfg.AvroIncludeBeforeValue)
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
	cfg4 := NewConfig(config.ProtocolDebeziumAvro)
	cfg4.DebeziumIncludeStartTs = true
	errCode, ok := errors.RFCCode(cfg4.Validate())
	require.True(t, ok)
	require.Equal(t, errors.ErrCodecInvalidConfig.RFCCode(), errCode)
}

func TestSimpleIncludeStartTsConfig(t *testing.T) {
	cfg := NewConfig(config.ProtocolSimple)
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=simple&simple-include-start-ts=true")
	require.NoError(t, err)
	require.NoError(t, cfg.Apply(sinkURI, config.GetDefaultReplicaConfig().Sink))
	require.True(t, cfg.SimpleIncludeStartTs)
	require.NoError(t, cfg.Validate())

	on := true
	cfg2 := NewConfig(config.ProtocolSimple)
	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.Simple = &config.SimpleConfig{IncludeStartTs: &on}
	sinkURI2, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=simple")
	require.NoError(t, err)
	require.NoError(t, cfg2.Apply(sinkURI2, sinkConfig))
	require.True(t, cfg2.SimpleIncludeStartTs)

	cfg3 := NewConfig(config.ProtocolSimple)
	sinkConfig3 := config.GetDefaultReplicaConfig().Sink
	sinkConfig3.Simple = &config.SimpleConfig{IncludeStartTs: &on}
	sinkURI3, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=simple&simple-include-start-ts=false")
	require.NoError(t, err)
	require.NoError(t, cfg3.Apply(sinkURI3, sinkConfig3))
	require.False(t, cfg3.SimpleIncludeStartTs)

	cfg4 := NewConfig(config.ProtocolSimple)
	cfg4.SimpleIncludeStartTs = true
	cfg4.EncodingFormat = EncodingFormatAvro
	errCode, ok := errors.RFCCode(cfg4.Validate())
	require.True(t, ok)
	require.Equal(t, errors.ErrCodecInvalidConfig.RFCCode(), errCode)

	cfg5 := NewConfig(config.ProtocolDebezium)
	cfg5.SimpleIncludeStartTs = true
	errCode, ok = errors.RFCCode(cfg5.Validate())
	require.True(t, ok)
	require.Equal(t, errors.ErrCodecInvalidConfig.RFCCode(), errCode)
}
