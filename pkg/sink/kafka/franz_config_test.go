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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
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
	opts, err := clientOptions(t.Context(), o)
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

func TestFranzProducerTimeouts(t *testing.T) {
	o := testOptions([]string{"127.0.0.1:9092"})
	o.ReadTimeout = 3 * time.Second
	o.WriteTimeout = 2 * time.Second

	opts := testClientOptions(t, o)
	client, err := kgo.NewClient(append(opts, producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, 2*time.Second, client.OptValue(kgo.RequestTimeoutOverhead))
	require.Equal(t, 3*time.Second, client.OptValue(kgo.ProduceRequestTimeout))
}

func TestProducerOptionsConfigureMessageAndBufferLimits(t *testing.T) {
	const maxMessageBytes = 1048588
	o := testOptions([]string{"127.0.0.1:9092"})
	o.MaxMessageBytes = maxMessageBytes

	opts, err := clientOptions(t.Context(), o)
	require.NoError(t, err)

	producerOpts := producerOptions(o)

	client, err := kgo.NewClient(append(opts, producerOpts...)...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int32(maxMessageBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
	require.Equal(t, int64(producerMaxBufferedBytes), client.OptValue(kgo.MaxBufferedBytes))
	require.Equal(t, int64(producerMaxBufferedRecords), client.OptValue(kgo.MaxBufferedRecords))
	require.Equal(t, int64(1), client.OptValue(kgo.RecordRetries))
	require.Equal(t, int64(1), client.OptValue(kgo.UnknownTopicRetries))
	require.Equal(t, int32(producerMaxRequestBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
}

func TestProducerOptionsUseSingleNonIdempotentRequest(t *testing.T) {
	config := testOptions([]string{"127.0.0.1:9092"})

	producerOpts := producerOptions(config)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, true, client.OptValue(kgo.DisableIdempotentWrite))
	require.Equal(t, 1, client.OptValue(kgo.MaxProduceRequestsInflightPerBroker))
}

func TestProducerLimitsDoNotScaleWithConfiguredMessage(t *testing.T) {
	maxMessageBytes := 32 << 20
	config := testOptions([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = maxMessageBytes

	producerOpts := producerOptions(config)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int64(producerMaxBufferedBytes), client.OptValue(kgo.MaxBufferedBytes))
	require.Equal(t, int32(producerMaxRequestBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
}

func TestProducerOptionsDoNotClampSmallBatch(t *testing.T) {
	config := testOptions([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = 511

	producerOpts := producerOptions(config)

	_, err := kgo.NewClient(producerOpts...)
	require.Error(t, err)
}

func TestProducerOptionsLimitBatchToProduceRequest(t *testing.T) {
	for _, test := range []struct {
		name            string
		maxMessageBytes int
	}{
		{name: "at request limit", maxMessageBytes: producerMaxRequestBytes},
		{name: "above request limit", maxMessageBytes: 128 << 20},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := testOptions([]string{"127.0.0.1:9092"})
			config.MaxMessageBytes = test.maxMessageBytes

			client, err := kgo.NewClient(producerOptions(config)...)
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, int32(producerMaxRequestBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
			require.Equal(t, int32(producerMaxRequestBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
		})
	}
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

			producerOpts := producerOptions(cfg)

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

		mechanism, err := buildSASLMechanism(t.Context(), &saslConfig{
			mechanism: gssapiMechanism,
			gssapi:    cfg,
		})
		require.NoError(t, err)
		require.Equal(t, "GSSAPI", mechanism.Name())
	}
}

func TestBuildFranzSASLMechanisms(t *testing.T) {
	for _, mechanism := range []saslMechanism{plainMechanism, scram256Mechanism, scram512Mechanism} {
		actual, err := buildSASLMechanism(t.Context(), &saslConfig{
			mechanism: mechanism,
			user:      "alice",
			password:  "secret",
		})
		require.NoError(t, err)
		require.Equal(t, string(mechanism), actual.Name())
	}

	_, err := buildSASLMechanism(t.Context(), &saslConfig{mechanism: "unknown"})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestFranzOAuthTokenSource(t *testing.T) {
	var tokenRequests atomic.Int32
	request := make(chan url.Values, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("parse token request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		tokenRequests.Add(1)
		select {
		case request <- r.PostForm:
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"access_token":"token","token_type":"bearer"}`); err != nil {
			t.Errorf("write token response: %v", err)
		}
	}))
	defer server.Close()

	mechanism, err := buildSASLMechanism(t.Context(), &saslConfig{
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
	_, _, err = mechanism.Authenticate(context.Background(), "")
	require.NoError(t, err)

	form := <-request
	require.Equal(t, int32(1), tokenRequests.Load())
	require.Equal(t, "custom", form.Get("grant_type"))
	require.Equal(t, "audience", form.Get("audience"))
	require.Equal(t, "scope-a scope-b", form.Get("scope"))
}

func TestFranzOAuthTokenSourceRejectsInvalidURL(t *testing.T) {
	_, err := buildSASLMechanism(t.Context(), &saslConfig{
		mechanism: oauthMechanism,
		oauth2:    oauth2Config{tokenURL: "http://example.com/%%"},
	})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}
