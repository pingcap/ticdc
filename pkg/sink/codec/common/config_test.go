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
