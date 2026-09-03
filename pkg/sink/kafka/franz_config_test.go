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
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"golang.org/x/oauth2"
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

func TestClientOptions(t *testing.T) {
	t.Run("timeouts", func(t *testing.T) {
		o := testOptions([]string{"127.0.0.1:9092"})
		o.ReadTimeout = 3 * time.Second
		o.WriteTimeout = 2 * time.Second
		client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
		require.NoError(t, err)
		defer client.Close()

		require.Equal(t, 2*time.Second, client.OptValue(kgo.RequestTimeoutOverhead))
		require.Equal(t, 3*time.Second, client.OptValue(kgo.ProduceRequestTimeout))
	})

	t.Run("TLS", func(t *testing.T) {
		ca, err := security.NewCA()
		require.NoError(t, err)
		certPEM, keyPEM, err := ca.GenerateCerts("localhost")
		require.NoError(t, err)
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.TLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		}))
		defer cluster.Close()

		dir := t.TempDir()
		caPath := filepath.Join(dir, "ca.pem")
		certPath := filepath.Join(dir, "cert.pem")
		keyPath := filepath.Join(dir, "key.pem")
		require.NoError(t, os.WriteFile(caPath, ca.CAPEM, 0o600))
		require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))
		o := testOptions(cluster.ListenAddrs())
		o.EnableTLS = true
		o.Credential = &security.Credential{CAPath: caPath, CertPath: certPath, KeyPath: keyPath}
		client, err := kgo.NewClient(testClientOptions(t, o)...)
		require.NoError(t, err)
		defer client.Close()

		metadata, err := kadm.NewClient(client).Metadata(t.Context())
		require.NoError(t, err)
		require.Len(t, metadata.Brokers, 1)
	})

	t.Run("SASL", func(t *testing.T) {
		cluster := kfake.MustCluster(
			kfake.NumBrokers(1),
			kfake.EnableSASL(),
			kfake.Superuser("PLAIN", "alice", "secret"),
		)
		defer cluster.Close()
		o := testOptions(cluster.ListenAddrs())
		o.sasl = &saslConfig{mechanism: plainMechanism, user: "alice", password: "secret"}
		client, err := kgo.NewClient(testClientOptions(t, o)...)
		require.NoError(t, err)
		defer client.Close()

		metadata, err := kadm.NewClient(client).Metadata(t.Context())
		require.NoError(t, err)
		require.Len(t, metadata.Brokers, 1)
	})
}

func TestProducerLimits(t *testing.T) {
	for _, test := range []struct {
		name            string
		maxMessageBytes int
		expectedBatch   int32
	}{
		{name: "configured", maxMessageBytes: 1048588, expectedBatch: 1048588},
		{name: "at request limit", maxMessageBytes: producerMaxRequestBytes, expectedBatch: producerMaxRequestBytes},
		{name: "above request limit", maxMessageBytes: 128 << 20, expectedBatch: producerMaxRequestBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := testOptions([]string{"127.0.0.1:9092"})
			o.MaxMessageBytes = test.maxMessageBytes

			client, err := kgo.NewClient(producerOptions(o)...)
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, test.expectedBatch, client.OptValue(kgo.ProducerBatchMaxBytes))
			require.Equal(t, int64(producerMaxBufferedBytes), client.OptValue(kgo.MaxBufferedBytes))
			require.Equal(t, int64(producerMaxBufferedRecords), client.OptValue(kgo.MaxBufferedRecords))
			require.Equal(t, int64(o.MaxRetry), client.OptValue(kgo.RecordRetries))
			require.Equal(t, int64(o.MaxRetry), client.OptValue(kgo.UnknownTopicRetries))
			require.Equal(t, int32(producerMaxRequestBytes), client.OptValue(kgo.BrokerMaxWriteBytes))
			require.Equal(t, time.Duration(0), client.OptValue(kgo.ProducerLinger))
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

func TestGSSAPIMechanism(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "krb5.conf")
	require.NoError(t, os.WriteFile(configPath, []byte(`[libdefaults]
 default_realm = EXAMPLE.COM
[realms]
 EXAMPLE.COM = {
  kdc = localhost:88
 }
`), 0o600))
	keytabPath := filepath.Join(dir, "client.keytab")
	kt := keytab.New()
	require.NoError(t, kt.AddEntry("alice", "EXAMPLE.COM", "pwd", time.Now(), 1, etypeID.AES256_CTS_HMAC_SHA1_96))
	keytabBytes, err := kt.Marshal()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keytabPath, keytabBytes, 0o600))

	for _, cfg := range []gssapiConfig{
		{authType: userAuth, password: "pwd"},
		{authType: keyTabAuth, keyTabPath: keytabPath},
	} {
		cfg.kerberosConfigPath = configPath
		cfg.serviceName = "kafka"
		cfg.username = "alice"
		cfg.realm = "EXAMPLE.COM"

		mechanism, err := buildSASLMechanism(t.Context(), &saslConfig{
			mechanism: gssapiMechanism,
			gssapi:    cfg,
		})
		require.NoError(t, err)
		require.Equal(t, "GSSAPI", mechanism.Name())
		closing, ok := mechanism.(sasl.ClosingMechanism)
		require.True(t, ok)
		closing.Close()

		next, err := buildGSSAPIMechanism(cfg)
		require.NoError(t, err)
		require.NotSame(t, mechanism, next)
		nextClosing, ok := next.(sasl.ClosingMechanism)
		require.True(t, ok)
		nextClosing.Close()
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

func TestFranzOAuth(t *testing.T) {
	t.Run("token reuse", func(t *testing.T) {
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
	})

	t.Run("endpoint error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			if _, err := io.WriteString(w, `{"error":"invalid_client"}`); err != nil {
				t.Errorf("write token error response: %v", err)
			}
		}))
		defer server.Close()
		mechanism, err := buildOAuthMechanism(t.Context(), oauth2Config{tokenURL: server.URL})
		require.NoError(t, err)

		_, _, err = mechanism.Authenticate(context.Background(), "")
		require.ErrorIs(t, err, errors.ErrNewKafkaSink)
		var retrieveErr *oauth2.RetrieveError
		require.ErrorAs(t, err, &retrieveErr)
	})

	t.Run("invalid URL", func(t *testing.T) {
		_, err := buildOAuthMechanism(t.Context(), oauth2Config{tokenURL: "http://example.com/%%"})
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	})
}
