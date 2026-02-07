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

package kafka

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBuildFranzSaslMechanismGSSAPIUserAuth(t *testing.T) {
	t.Parallel()

	o := NewOptions()
	o.SASL = &security.SASL{
		SASLMechanism: security.GSSAPIMechanism,
		GSSAPI: security.GSSAPI{
			AuthType:           security.UserAuth,
			KerberosConfigPath: "/etc/krb5.conf",
			ServiceName:        "kafka",
			Username:           "alice",
			Password:           "pwd",
			Realm:              "EXAMPLE.COM",
		},
	}

	mechanism, err := buildFranzSaslMechanism(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, "GSSAPI", mechanism.Name())
}

func TestBuildFranzSaslMechanismGSSAPIKeytabAuth(t *testing.T) {
	t.Parallel()

	o := NewOptions()
	o.SASL = &security.SASL{
		SASLMechanism: security.GSSAPIMechanism,
		GSSAPI: security.GSSAPI{
			AuthType:           security.KeyTabAuth,
			KerberosConfigPath: "/etc/krb5.conf",
			ServiceName:        "kafka",
			Username:           "alice",
			KeyTabPath:         "/tmp/a.keytab",
			Realm:              "EXAMPLE.COM",
		},
	}

	mechanism, err := buildFranzSaslMechanism(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, "GSSAPI", mechanism.Name())
}

func TestFranzFactoryMetricsCollectorIsNotNoop(t *testing.T) {
	t.Parallel()

	f := &franzFactory{}
	collector := f.MetricsCollector(nil)

	_, isNoop := collector.(*noopMetricsCollector)
	require.False(t, isNoop)
}

func TestFranzMetricsCollectorCollectMetrics(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceNamme, "franz-metrics")
	hook := newFranzMetricsHook()
	collector := &franzMetricsCollector{
		changefeedID: changefeedID,
		hook:         hook,
	}
	t.Cleanup(func() {
		collector.cleanupMetrics()
	})

	meta := kgo.BrokerMetadata{NodeID: 1}
	hook.OnBrokerWrite(meta, 0, 128, 0, 0, nil)
	hook.OnProduceBatchWritten(meta, "topic", 0, kgo.ProduceBatchMetrics{
		NumRecords:        8,
		UncompressedBytes: 400,
		CompressedBytes:   200,
	})

	collector.collectMetrics()

	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	require.Equal(t, float64(1), testutil.ToFloat64(requestsInFlightGauge.WithLabelValues(keyspace, changefeed, "1")))
	require.Greater(t, testutil.ToFloat64(compressionRatioGauge.WithLabelValues(keyspace, changefeed, avg)), 0.0)
	require.Greater(t, testutil.ToFloat64(recordsPerRequestGauge.WithLabelValues(keyspace, changefeed, avg)), 0.0)
}
