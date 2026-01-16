// Copyright 2022 PingCAP, Inc.
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
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	c := NewConfig(config.ProtocolDefault)
	require.Equal(t, config.ProtocolDefault, c.Protocol)
	require.Equal(t, config.DefaultMaxMessageBytes, c.MaxMessageBytes)
	require.Equal(t, defaultMaxBatchSize, c.MaxBatchSize)
	require.False(t, c.EnableTiDBExtension)
	require.False(t, c.EnableRowChecksum)
	require.NotNil(t, c.LargeMessageHandle)
}

func TestAvroSchemaRegistryRequired(t *testing.T) {
	t.Parallel()

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/abc?protocol=avro")
	require.NoError(t, err)

	c := NewConfig(config.ProtocolAvro)
	require.NoError(t, c.Apply(sinkURI, sinkConfig))
	require.ErrorContains(t, c.Validate(), "schema")

	sinkConfig.SchemaRegistry = util.AddressOf("some-schema-registry")
	c = NewConfig(config.ProtocolAvro)
	require.NoError(t, c.Apply(sinkURI, sinkConfig))
	require.NoError(t, c.Validate())
}

func TestEnableRowChecksumRequiresAvroOptions(t *testing.T) {
	t.Parallel()

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.SchemaRegistry = util.AddressOf("some-schema-registry")
	sinkConfig.Integrity = &config.IntegrityConfig{
		IntegrityCheckLevel:   config.CheckLevelCorrectness,
		CorruptionHandleLevel: config.CorruptionHandleLevelWarn,
	}

	validURI, err := url.Parse(
		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&" +
			"avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string",
	)
	require.NoError(t, err)

	c := NewConfig(config.ProtocolAvro)
	require.NoError(t, c.Apply(validURI, sinkConfig))
	require.True(t, c.EnableRowChecksum)
	require.NoError(t, c.Validate())

	invalidURIs := []string{
		"kafka://127.0.0.1:9092/abc?protocol=avro",
		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true",
		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&avro-decimal-handling-mode=string",
		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&avro-bigint-unsigned-handling-mode=string",
	}

	for _, uri := range invalidURIs {
		sinkURI, err := url.Parse(uri)
		require.NoError(t, err)

		c = NewConfig(config.ProtocolAvro)
		require.NoError(t, c.Apply(sinkURI, sinkConfig))
		require.Error(t, c.Validate())
	}
}

func TestForceReplicateNotAllowedForAvro(t *testing.T) {
	t.Parallel()

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.ForceReplicate = util.AddressOf(true)

	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/abc?protocol=avro")
	require.NoError(t, err)

	c := NewConfig(config.ProtocolAvro)
	require.ErrorIs(t, c.Apply(sinkURI, sinkConfig), cerror.ErrCodecInvalidConfig)
}

func TestDeleteOnlyHandleKeyColumnsConflictsWithForceReplicate(t *testing.T) {
	t.Parallel()

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.DeleteOnlyOutputHandleKeyColumns = util.AddressOf(true)
	sinkConfig.ForceReplicate = util.AddressOf(true)

	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/abc?protocol=open-protocol")
	require.NoError(t, err)

	c := NewConfig(config.ProtocolOpen)
	require.ErrorIs(t, c.Apply(sinkURI, sinkConfig), cerror.ErrCodecInvalidConfig)
}

func TestCanalJSONContentCompatibleForcesOnlyOutputUpdatedColumns(t *testing.T) {
	t.Parallel()

	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/abc?protocol=canal-json&content-compatible=true")
	require.NoError(t, err)

	sinkConfig := config.GetDefaultReplicaConfig().Sink
	c := NewConfig(config.ProtocolCanalJSON)
	require.NoError(t, c.Apply(sinkURI, sinkConfig))
	require.True(t, c.ContentCompatible)
	require.True(t, c.OnlyOutputUpdatedColumns)
}

func TestConfig4Simple(t *testing.T) {
	t.Parallel()

	uri := "kafka://127.0.0.1:9092/abc?protocol=simple"
	sinkURL, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolSimple)
	require.NoError(t, codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig().Sink))
	require.Equal(t, EncodingFormatJSON, codecConfig.EncodingFormat)

	uri = "kafka://127.0.0.1:9092/abc?protocol=simple&encoding-format=avro"
	sinkURL, err = url.Parse(uri)
	require.NoError(t, err)

	codecConfig = NewConfig(config.ProtocolSimple)
	require.NoError(t, codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig().Sink))
	require.Equal(t, EncodingFormatAvro, codecConfig.EncodingFormat)

	uri = "kafka://127.0.0.1:9092/abc?protocol=simple&encoding-format=xxx"
	sinkURL, err = url.Parse(uri)
	require.NoError(t, err)

	codecConfig = NewConfig(config.ProtocolSimple)
	require.ErrorIs(t, codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig().Sink), cerror.ErrCodecInvalidConfig)
}

