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

package config

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestParseSinkURI(t *testing.T) {
	// Scenario: A sink URI identifies both the sink implementation and downstream
	// system. SSL and storage aliases must retain the same classification.
	// Steps: Parse supported sink URIs and verify both types.
	testCases := []struct {
		sinkURI                string
		expectedSinkType       common.SinkType
		expectedDownstreamType string
	}{
		{sinkURI: "mysql://127.0.0.1:3306/", expectedSinkType: common.MysqlSinkType, expectedDownstreamType: "mysql"},
		{sinkURI: "mysql+ssl://127.0.0.1:3306/", expectedSinkType: common.MysqlSinkType, expectedDownstreamType: "mysql"},
		{sinkURI: "tidb://127.0.0.1:4000/", expectedSinkType: common.MysqlSinkType, expectedDownstreamType: "tidb"},
		{sinkURI: "tidb+ssl://127.0.0.1:4000/", expectedSinkType: common.MysqlSinkType, expectedDownstreamType: "tidb"},
		{sinkURI: "kafka://127.0.0.1:9092/topic", expectedSinkType: common.KafkaSinkType, expectedDownstreamType: "kafka"},
		{sinkURI: "kafka+ssl://127.0.0.1:9092/topic", expectedSinkType: common.KafkaSinkType, expectedDownstreamType: "kafka"},
		{sinkURI: "pulsar://127.0.0.1:6650/topic", expectedSinkType: common.PulsarSinkType, expectedDownstreamType: "pulsar"},
		{sinkURI: "pulsar+ssl://127.0.0.1:6651/topic", expectedSinkType: common.PulsarSinkType, expectedDownstreamType: "pulsar"},
		{sinkURI: "pulsar+http://127.0.0.1:8080/topic", expectedSinkType: common.PulsarSinkType, expectedDownstreamType: "pulsar"},
		{sinkURI: "pulsar+https://127.0.0.1:8443/topic", expectedSinkType: common.PulsarSinkType, expectedDownstreamType: "pulsar"},
		{sinkURI: "file:///tmp/ticdc", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "file"},
		{sinkURI: "s3://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "s3"},
		{sinkURI: "gs://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "gcs"},
		{sinkURI: "gcs://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "gcs"},
		{sinkURI: "azure://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "azblob"},
		{sinkURI: "azblob://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "azblob"},
		{sinkURI: "noop://bucket/prefix", expectedSinkType: common.CloudStorageSinkType, expectedDownstreamType: "noop"},
		{sinkURI: "blackhole:///", expectedSinkType: common.BlackHoleSinkType, expectedDownstreamType: "blackhole"},
	}

	for _, tc := range testCases {
		t.Run(tc.sinkURI, func(t *testing.T) {
			t.Parallel()
			sinkType, downstreamType := ParseSinkURI(tc.sinkURI)
			require.Equal(t, tc.expectedSinkType, sinkType)
			require.Equal(t, tc.expectedDownstreamType, downstreamType)
		})
	}
}
