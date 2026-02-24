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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownstreamTypeFromSinkURI(t *testing.T) {
	// Scenario: Dashboards need a stable changefeed -> downstream type mapping, but
	// should not depend on or expose the full sink-uri (which can contain secrets).
	// Steps: Feed representative sink-uri values and verify the normalized type.
	testCases := []struct {
		sinkURI       string
		expectedLabel string
	}{
		{sinkURI: "mysql://127.0.0.1:3306/", expectedLabel: "mysql"},
		{sinkURI: "mysql+ssl://127.0.0.1:3306/", expectedLabel: "mysql"},
		{sinkURI: "tidb://127.0.0.1:4000/", expectedLabel: "tidb"},
		{sinkURI: "tidb+ssl://127.0.0.1:4000/", expectedLabel: "tidb"},
		{sinkURI: "kafka://127.0.0.1:9092/topic", expectedLabel: "kafka"},
		{sinkURI: "kafka+ssl://127.0.0.1:9092/topic", expectedLabel: "kafka"},
		{sinkURI: "pulsar://127.0.0.1:6650/topic", expectedLabel: "pulsar"},
		{sinkURI: "pulsar+ssl://127.0.0.1:6651/topic", expectedLabel: "pulsar"},
		{sinkURI: "pulsar+http://127.0.0.1:8080/topic", expectedLabel: "pulsar"},
		{sinkURI: "pulsar+https://127.0.0.1:8443/topic", expectedLabel: "pulsar"},
		{sinkURI: "file:///tmp/ticdc", expectedLabel: "file"},
		{sinkURI: "s3://bucket/prefix", expectedLabel: "s3"},
		{sinkURI: "gs://bucket/prefix", expectedLabel: "gcs"},
		{sinkURI: "gcs://bucket/prefix", expectedLabel: "gcs"},
		{sinkURI: "azure://bucket/prefix", expectedLabel: "azblob"},
		{sinkURI: "azblob://bucket/prefix", expectedLabel: "azblob"},
		{sinkURI: "noop://bucket/prefix", expectedLabel: "noop"},
		{sinkURI: "blackhole:///", expectedLabel: "blackhole"},
		{sinkURI: "127.0.0.1:3306", expectedLabel: "unknown"},
		{sinkURI: "mysql://[::1", expectedLabel: "unknown"},
		{sinkURI: "", expectedLabel: "unknown"},
	}

	for _, tc := range testCases {
		tc := tc
		testName := tc.sinkURI
		if testName == "" {
			testName = "<empty>"
		}
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expectedLabel, DownstreamTypeFromSinkURI(tc.sinkURI))
		})
	}
}
