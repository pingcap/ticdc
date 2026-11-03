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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/goleak"
)

func TestConsumerInitializationCleanup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		timezone string
		dsn      string
		sink     string
		panic    string
	}{
		{name: "codec", timezone: "invalid/timezone", panic: "consumer time zone is invalid"},
		{name: "upstreamDB", dsn: "invalid-dsn", panic: "consumer upstream database open failed"},
		{name: "sink", panic: "consumer downstream sink initialization failed"},
		{name: "success", sink: "blackhole://"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "initialization"))
			defer cluster.Close()
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			opts := &options{
				upstreamURI:     "kafka://" + strings.Join(cluster.ListenAddrs(), ",") + "/initialization?protocol=open-protocol",
				downstreamURI:   tc.sink,
				upstreamTiDBDSN: tc.dsn,
				timezone:        "UTC",
				logLevel:        "error",
			}
			if tc.timezone != "" {
				opts.timezone = tc.timezone
			}
			if tc.panic != "" {
				require.PanicsWithValue(t, tc.panic, func() { newConsumer(ctx, opts) })
				return
			}
			c := newConsumer(ctx, opts)
			defer c.reader.close()
			defer c.conductor.close()
			defer c.writer.close()
			require.NoError(t, c.reader.client.(*kafkaReader).client.Ping(ctx))
		})
	}
}

func TestConsumerStorageConfigurationBeforeOpeningReader(t *testing.T) {
	upstream := &url.URL{Scheme: "file", Path: t.TempDir(), RawQuery: "protocol=csv"}
	c := newConsumer(t.Context(), &options{
		upstreamURI:    upstream.String(),
		downstreamURI:  "blackhole://",
		timezone:       "UTC",
		logLevel:       "error",
		fileIndexWidth: config.DefaultFileIndexWidth,
	})
	defer c.reader.close()
	defer c.conductor.close()
	defer c.writer.close()
	require.Equal(t, config.ProtocolCsv, c.parser.codecConfig.Protocol)
	require.Equal(t, config.DateSeparatorDay, c.reader.client.(*storageReader).dateSeparator)
	require.Equal(t, config.DefaultFileIndexWidth, c.reader.client.(*storageReader).fileIndexWidth)
}

func TestConsumerPulsarProtocolConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		uri        string
		configured string
		want       config.Protocol
		reject     bool
	}{
		{name: "default", uri: "pulsar://localhost/topic?enable-tidb-extension=true", want: config.ProtocolCanalJSON},
		{name: "configuration", uri: "pulsar://localhost/topic?enable-tidb-extension=true", configured: "canal-json", want: config.ProtocolCanalJSON},
		{name: "URI overrides configuration", uri: "pulsar://localhost/topic?protocol=canal-json&enable-tidb-extension=true", configured: "simple", want: config.ProtocolCanalJSON},
		{name: "unsupported URI", uri: "pulsar://localhost/topic?protocol=simple", reject: true},
		{name: "unsupported configuration", uri: "pulsar://localhost/topic", configured: "avro", reject: true},
		{name: "Kafka simple", uri: "kafka://localhost/topic?protocol=simple", want: config.ProtocolSimple},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replicaConfig := config.GetDefaultReplicaConfig()
			if tc.configured != "" {
				replicaConfig.Sink.Protocol = new(tc.configured)
			}
			upstream, err := url.Parse(tc.uri)
			require.NoError(t, err)
			if tc.reject {
				require.PanicsWithValue(t, "Pulsar consumer protocol is unsupported", func() {
					newDecodeConfig(upstream, "UTC", replicaConfig)
				})
				return
			}
			decoding := newDecodeConfig(upstream, "UTC", replicaConfig)
			require.Equal(t, tc.want, decoding.codec.Protocol)
		})
	}
}
