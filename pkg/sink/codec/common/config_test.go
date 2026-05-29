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
	require.True(t, cfg.AvroIncludeBeforeValue)
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
	require.True(t, cfg.AvroIncludeBeforeValue)
}
