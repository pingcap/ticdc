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

package kafka

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testOptions(brokers []string) *options {
	o := NewOptions()
	o.BrokerEndpoints = brokers
	o.MaxMessageBytes = 1 << 20
	o.MaxRetry = 1
	o.DialTimeout = time.Second
	o.ReadTimeout = time.Second
	o.WriteTimeout = time.Second
	return o
}

func testClientOptions(t *testing.T, o *options) []kgo.Opt {
	t.Helper()
	opts, err := clientOptions(o)
	require.NoError(t, err)
	return opts
}

func testProducerOptions(t *testing.T, o *options) []kgo.Opt {
	t.Helper()
	opts, err := producerOptions(o)
	require.NoError(t, err)
	return opts
}

func TestFranzRequiredAcks(t *testing.T) {
	for _, test := range []struct {
		required RequiredAcks
		expected kgo.Acks
	}{
		{required: WaitForAll, expected: kgo.AllISRAcks()},
		{required: WaitForLocal, expected: kgo.LeaderAck()},
		{required: NoResponse, expected: kgo.NoAck()},
		{required: RequiredAcks(2), expected: kgo.AllISRAcks()},
	} {
		require.Equal(t, test.expected, requiredAcks(test.required))
	}
}

func TestFranzRequestTimeoutUsesLargerTimeout(t *testing.T) {
	o := &options{ReadTimeout: time.Second, WriteTimeout: 2 * time.Second}
	require.Equal(t, 2*time.Second, requestTimeout(o))

	o.ReadTimeout = 3 * time.Second
	require.Equal(t, 3*time.Second, requestTimeout(o))
}

func TestFranzIgnoresConfiguredKafkaVersion(t *testing.T) {
	const topic = "version-negotiation"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	o := NewOptions()
	o.ClientID = "ticdc-test"
	o.BrokerEndpoints = cluster.ListenAddrs()
	o.Topic = topic
	o.Version = "invalid"
	o.IsAssignedVersion = true

	factory, err := NewFactory(
		context.Background(),
		o,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "version-negotiation"),
	)
	require.NoError(t, err)
	require.IsType(t, &franzFactory{}, factory)
	factory.CleanupMetrics()
}

func TestProducerOptionsBoundBufferAndBatch(t *testing.T) {
	const batchBytes = 1048588
	o := testOptions([]string{"127.0.0.1:9092"})
	o.MaxMessageBytes = batchBytes

	opts, err := clientOptions(o)
	require.NoError(t, err)

	producerOpts, err := producerOptions(o)
	require.NoError(t, err)

	client, err := kgo.NewClient(append(opts, producerOpts...)...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int32(batchBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
	require.Equal(t, int64(defaultMaxBufferedBytes), client.OptValue(kgo.MaxBufferedBytes))
	require.Equal(t, int64(10000), client.OptValue(kgo.MaxBufferedRecords))
	require.Equal(t, int64(1), client.OptValue(kgo.RecordRetries))
	require.Equal(t, int64(1), client.OptValue(kgo.UnknownTopicRetries))
	require.Equal(t, int32(defaultBrokerWriteBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
}

func TestProducerOptionsUseSingleNonIdempotentRequest(t *testing.T) {
	config := testOptions([]string{"127.0.0.1:9092"})

	producerOpts, err := producerOptions(config)
	require.NoError(t, err)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, true, client.OptValue(kgo.DisableIdempotentWrite))
	require.Equal(t, 1, client.OptValue(kgo.MaxProduceRequestsInflightPerBroker))
}

func TestProducerLimitsScaleWithConfiguredMessage(t *testing.T) {
	maxMessageBytes := defaultBrokerWriteBytes + 1
	config := testOptions([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = maxMessageBytes

	producerOpts, err := producerOptions(config)
	require.NoError(t, err)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int64(maxMessageBytes), client.OptValue(kgo.MaxBufferedBytes))
	require.Equal(t, int32(maxMessageBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
}

func TestProducerOptionsClampSmallBatch(t *testing.T) {
	config := testOptions([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = minProducerBatchBytes - 1

	producerOpts, err := producerOptions(config)
	require.NoError(t, err)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int32(minProducerBatchBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
}

func TestProducerOptionsRejectOversizedBatch(t *testing.T) {
	config := testOptions([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = maxProducerBatchBytes + 1

	_, err := producerOptions(config)
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestCompressionOptions(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression string
		expected    kgo.CompressionCodec
	}{
		{name: "none", compression: "none", expected: kgo.NoCompression()},
		{name: "gzip", compression: "gzip", expected: kgo.GzipCompression()},
		{name: "snappy", compression: "snappy", expected: kgo.SnappyCompression()},
		{name: "lz4", compression: "lz4", expected: kgo.Lz4Compression()},
		{name: "zstd", compression: "zstd", expected: kgo.ZstdCompression()},
		{name: "unknown falls back to none", compression: "unknown", expected: kgo.NoCompression()},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := testOptions([]string{"127.0.0.1:9092"})
			cfg.Compression = test.compression

			producerOpts, err := producerOptions(cfg)
			require.NoError(t, err)

			client, err := kgo.NewClient(producerOpts...)
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, []kgo.CompressionCodec{test.expected}, client.OptValue(kgo.ProducerBatchCompression))
		})
	}
}

func TestBuildFranzGSSAPIMechanism(t *testing.T) {
	for _, cfg := range []gssapiConfig{
		{authType: userAuth, password: "pwd"},
		{authType: keyTabAuth, keyTabPath: "/tmp/a.keytab"},
	} {
		cfg.kerberosConfigPath = "/etc/krb5.conf"
		cfg.serviceName = "kafka"
		cfg.username = "alice"
		cfg.realm = "EXAMPLE.COM"

		mechanism, err := buildSASLMechanism(&saslConfig{
			mechanism: gssapiMechanism,
			gssapi:    cfg,
		})
		require.NoError(t, err)
		require.Equal(t, "GSSAPI", mechanism.Name())
	}
}

func TestBuildFranzSASLMechanisms(t *testing.T) {
	for _, mechanism := range []saslMechanism{plainMechanism, scram256Mechanism, scram512Mechanism} {
		actual, err := buildSASLMechanism(&saslConfig{
			mechanism: mechanism,
			user:      "alice",
			password:  "secret",
		})
		require.NoError(t, err)
		require.Equal(t, string(mechanism), actual.Name())
	}

	_, err := buildSASLMechanism(&saslConfig{mechanism: "unknown"})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestFranzOAuthTokenSource(t *testing.T) {
	request := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse token request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		request <- r.PostForm

		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"access_token":"token","token_type":"bearer"}`); err != nil {
			t.Errorf("write token response: %v", err)
		}
	}))
	defer server.Close()

	mechanism, err := buildSASLMechanism(&saslConfig{
		mechanism: oauthMechanism,
		oauth2: oauth2Config{
			clientID:     "client",
			clientSecret: "secret",
			tokenURL:     server.URL,
			scopes:       []string{"scope-a", "scope-b"},
			grantType:    "custom",
			audience:     "audience",
		},
	})
	require.NoError(t, err)

	_, _, err = mechanism.Authenticate(context.Background(), "")
	require.NoError(t, err)

	form := <-request
	require.Equal(t, "custom", form.Get("grant_type"))
	require.Equal(t, "audience", form.Get("audience"))
	require.Equal(t, "scope-a scope-b", form.Get("scope"))
}

func TestFranzOAuthTokenSourceRejectsInvalidURL(t *testing.T) {
	_, err := buildSASLMechanism(&saslConfig{
		mechanism: oauthMechanism,
		oauth2:    oauth2Config{tokenURL: "http://example.com/%%"},
	})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}
