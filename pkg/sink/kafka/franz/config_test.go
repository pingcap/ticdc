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

package franz

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
	"github.com/twmb/franz-go/pkg/kgo"
)

func testConfig(brokers []string) Config {
	return Config{
		BrokerEndpoints: brokers,
		MaxMessageBytes: 1 << 20,
		MaxRetry:        1,
		RequiredAcks:    WaitForAll,
		DialTimeout:     time.Second,
		ReadTimeout:     time.Second,
		WriteTimeout:    time.Second,
	}
}

func TestRequiredAcks(t *testing.T) {
	for _, test := range []struct {
		required int16
		expected kgo.Acks
	}{
		{required: WaitForAll, expected: kgo.AllISRAcks()},
		{required: WaitForLocal, expected: kgo.LeaderAck()},
		{required: NoResponse, expected: kgo.NoAck()},
		{required: 2, expected: kgo.AllISRAcks()},
	} {
		require.Equal(t, test.expected, requiredAcks(test.required))
	}
}

func TestRequestTimeoutUsesLargerTimeout(t *testing.T) {
	cfg := Config{ReadTimeout: time.Second, WriteTimeout: 2 * time.Second}
	require.Equal(t, 2*time.Second, cfg.requestTimeout())

	cfg.ReadTimeout = 3 * time.Second
	require.Equal(t, 3*time.Second, cfg.requestTimeout())
}

func TestProducerOptionsBoundBufferAndBatch(t *testing.T) {
	const batchBytes = 1048588
	cfg := testConfig([]string{"127.0.0.1:9092"})
	cfg.MaxMessageBytes = batchBytes

	opts, err := newClientOptions(
		context.Background(),
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "config"),
		"test",
		cfg,
		nil,
	)
	require.NoError(t, err)

	producerOpts, err := producerOptions(cfg)
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
	config := testConfig([]string{"127.0.0.1:9092"})

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
	config := testConfig([]string{"127.0.0.1:9092"})
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
	config := testConfig([]string{"127.0.0.1:9092"})
	config.MaxMessageBytes = minProducerBatchBytes - 1

	producerOpts, err := producerOptions(config)
	require.NoError(t, err)

	client, err := kgo.NewClient(producerOpts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int32(minProducerBatchBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
}

func TestProducerOptionsRejectOversizedBatch(t *testing.T) {
	config := testConfig([]string{"127.0.0.1:9092"})
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
			cfg := testConfig([]string{"127.0.0.1:9092"})
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

func TestBuildGSSAPIMechanism(t *testing.T) {
	for _, cfg := range []GSSAPIConfig{
		{AuthType: userAuth, Password: "pwd"},
		{AuthType: keyTabAuth, KeyTabPath: "/tmp/a.keytab"},
	} {
		cfg.KerberosConfigPath = "/etc/krb5.conf"
		cfg.ServiceName = "kafka"
		cfg.Username = "alice"
		cfg.Realm = "EXAMPLE.COM"

		mechanism, err := buildSASLMechanism(context.Background(), SASLConfig{
			Mechanism: "GSSAPI",
			GSSAPI:    cfg,
		})
		require.NoError(t, err)
		require.Equal(t, "GSSAPI", mechanism.Name())
	}
}

func TestBuildSASLMechanisms(t *testing.T) {
	for _, mechanism := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		actual, err := buildSASLMechanism(context.Background(), SASLConfig{
			Mechanism: mechanism,
			User:      "alice",
			Password:  "secret",
		})
		require.NoError(t, err)
		require.Equal(t, mechanism, actual.Name())
	}

	_, err := buildSASLMechanism(context.Background(), SASLConfig{Mechanism: "unknown"})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestOAuthTokenSource(t *testing.T) {
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

	source, err := newOAuthTokenSource(context.Background(), OAuth2Config{
		ClientID:     "client",
		ClientSecret: "secret",
		TokenURL:     server.URL,
		Scopes:       []string{"scope-a", "scope-b"},
		GrantType:    "custom",
		Audience:     "audience",
	})
	require.NoError(t, err)

	token, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, "token", token.AccessToken)

	form := <-request
	require.Equal(t, "custom", form.Get("grant_type"))
	require.Equal(t, "audience", form.Get("audience"))
	require.Equal(t, "scope-a scope-b", form.Get("scope"))
}

func TestOAuthTokenSourceUsesHTTPClient(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(w, `{"access_token":"token","token_type":"bearer"}`)
		require.NoError(t, err)
	}))
	defer server.Close()

	source, err := newOAuthTokenSource(context.Background(), OAuth2Config{
		TokenURL:   server.URL,
		HTTPClient: server.Client(),
	})
	require.NoError(t, err)

	token, err := source.Token()
	require.NoError(t, err)
	require.Equal(t, "token", token.AccessToken)
}

func TestOAuthTokenSourceRejectsInvalidURL(t *testing.T) {
	_, err := newOAuthTokenSource(context.Background(), OAuth2Config{TokenURL: "http://example.com/%%"})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}
