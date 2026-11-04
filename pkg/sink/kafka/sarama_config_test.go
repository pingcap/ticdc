// Copyright 2023 PingCAP, Inc.
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
	"net/http"
	"net/url"
	"testing"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin/binding"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestNewSaramaConfig(t *testing.T) {
	options := NewOptions()
	options.Version = "invalid"
	options.IsAssignedVersion = true
	ctx := context.Background()
	_, err := newSaramaConfig(ctx, options)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	options.Version = "2.6.0"

	options.ClientID = "test-kafka-client"
	compressionCases := []struct {
		algorithm string
		expected  sarama.CompressionCodec
	}{
		{"none", sarama.CompressionNone},
		{"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy},
		{"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
		{"others", sarama.CompressionNone},
	}
	for _, cc := range compressionCases {
		options.Compression = cc.algorithm
		cfg, err := newSaramaConfig(ctx, options)
		require.NoError(t, err)
		require.Equal(t, cc.expected, cfg.Producer.Compression)
	}
	cfg, err := newSaramaConfig(ctx, options)
	require.NoError(t, err)
	require.Equal(t, defaultMaxRetry, cfg.Producer.Retry.Max)

	options.EnableTLS = true
	options.Credential = &security.Credential{
		CAPath:   "/invalid/ca/path",
		CertPath: "/invalid/cert/path",
		KeyPath:  "/invalid/key/path",
	}
	_, err = newSaramaConfig(ctx, options)
	require.Regexp(t, ".*no such file or directory", errors.Cause(err))

	saslOptions := NewOptions()
	saslOptions.Version = "2.6.0"
	saslOptions.ClientID = "test-sasl-scram"
	saslOptions.sasl = &saslConfig{
		user:      "user",
		password:  "password",
		mechanism: scram256Mechanism,
	}

	cfg, err = newSaramaConfig(ctx, saslOptions)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "user", cfg.Net.SASL.User)
	require.Equal(t, "password", cfg.Net.SASL.Password)
	require.Equal(t, sarama.SASLMechanism("SCRAM-SHA-256"), cfg.Net.SASL.Mechanism)
}

func TestSelectKafkaVersion(t *testing.T) {
	tests := []struct {
		name            string
		detectedVersion sarama.KafkaVersion
		assignedVersion string
		expectedVersion sarama.KafkaVersion
		expectedErr     error
	}{
		{
			name:            "use detected version",
			detectedVersion: sarama.V2_4_0_0,
			expectedVersion: sarama.V2_4_0_0,
		},
		{
			name:            "use fallback version",
			detectedVersion: defaultKafkaVersion,
			expectedVersion: defaultKafkaVersion,
		},
		{
			name:            "assigned version overrides detected version",
			detectedVersion: sarama.V2_4_0_0,
			assignedVersion: "2.6.0",
			expectedVersion: sarama.V2_6_0_0,
		},
		{
			name:            "assigned version overrides fallback version",
			detectedVersion: defaultKafkaVersion,
			assignedVersion: "2.6.0",
			expectedVersion: sarama.V2_6_0_0,
		},
		{
			name:            "reject invalid assigned version",
			detectedVersion: sarama.V2_4_0_0,
			assignedVersion: "invalid",
			expectedErr:     errors.ErrKafkaInvalidConfig,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := NewOptions()
			if test.assignedVersion != "" {
				options.IsAssignedVersion = true
				options.Version = test.assignedVersion
			}

			version, err := selectKafkaVersion(test.detectedVersion, options)
			if test.expectedErr != nil {
				require.ErrorIs(t, err, test.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.expectedVersion, version)
		})
	}
}

func TestNewSaramaConfigInvalidOAuthTokenURL(t *testing.T) {
	options := NewOptions()
	options.sasl = &saslConfig{
		mechanism: oauthMechanism,
		oauth2: oauth2Config{
			tokenURL: "http://test.com/Segment%%2815197306101420000%29",
		},
	}

	_, err := newSaramaConfig(t.Context(), options)
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	var escapeErr url.EscapeError
	require.ErrorAs(t, err, &escapeErr)
}

func TestNewSaramaConfigMaxRetryFromSinkURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		sinkURI  string
		expected int
	}{
		{
			name:     "default max retry",
			sinkURI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&kafka-client-id=unit-test",
			expected: defaultMaxRetry,
		},
		{
			name: "set max retry",
			sinkURI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0" +
				"&kafka-client-id=unit-test&max-retry=7",
			expected: 7,
		},
		{
			name: "zero max retry",
			sinkURI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0" +
				"&kafka-client-id=unit-test&max-retry=0",
			expected: 0,
		},
		{
			name: "negative max retry",
			sinkURI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0" +
				"&kafka-client-id=unit-test&max-retry=-1",
			expected: defaultMaxRetry,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			options := NewOptions()
			sinkURI, err := url.Parse(test.sinkURI)
			require.NoError(t, err)
			err = options.Apply(
				common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
				sinkURI,
				config.GetDefaultReplicaConfig().Sink,
			)
			require.NoError(t, err)

			cfg, err := newSaramaConfig(context.Background(), options)
			require.NoError(t, err)
			require.Equal(t, test.expected, cfg.Producer.Retry.Max)
		})
	}
}

func TestApplySaramaSASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		URI           string
		replicaConfig func() *config.ReplicaConfig
		exceptErr     string
	}{
		{
			name:          "no params",
			URI:           "kafka://127.0.0.1:9092/abc",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid PLAIN SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid SCRAM SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=SCRAM-SHA-512",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid GSSAPI user auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=USER" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-password=pwd" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid GSSAPI keytab auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=keytab" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-keytab-path=/root/keytab" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "invalid mechanism",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=a",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "unknown SASL mechanism: a",
		},
		{
			name: "invalid GSSAPI auth type",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=gssapi&sasl-gssapi-auth-type=keyta1b",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "unknown auth type: keyta1b",
		},
		{
			name: "valid OAUTHBEARER SASL",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         new(string(oauthMechanism)),
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client id",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         new(string(oauthMechanism)),
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client id is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client secret",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientID := "client_id"
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:     new(string(oauthMechanism)),
					SASLOAuthClientID: &clientID,
					SASLOAuthTokenURL: &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client secret is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing token url",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         new(string(oauthMechanism)),
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
				}
				return cfg
			},
			exceptErr: "OAuth2 token url is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: non base64 client secret",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientID := "client_id"
				clientSecret := "client_secret"
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         new(string(oauthMechanism)),
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client secret is not base64 encoded",
		},
		{
			name: "invalid OAUTHBEARER SASL: wrong mechanism",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=GSSAPI",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         new(string(oauthMechanism)),
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 is only supported with SASL mechanism type OAUTHBEARER",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := NewOptions()
			sinkURI, err := url.Parse(test.URI)
			require.NoError(t, err)
			req := &http.Request{URL: sinkURI}
			urlParameter := &urlConfig{}
			err = binding.Query.Bind(req, urlParameter)
			require.NoError(t, err)
			if test.exceptErr == "" {
				require.Nil(t, options.applySASL(urlParameter, test.replicaConfig().Sink))
			} else {
				require.Regexp(t, test.exceptErr,
					options.applySASL(urlParameter, test.replicaConfig().Sink).Error())
			}
		})
	}
}

func TestApplySaramaTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		URI        string
		tlsEnabled bool
		exceptErr  string
	}{
		{
			name: "tls config with 'enable-tls' set to true",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=true",
			tlsEnabled: true,
			exceptErr:  "",
		},
		{
			name: "tls config with no 'enable-tls', and credential files are supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain" +
				"&ca=/root/ca.file&cert=/root/cert.file&key=/root/key.file",
			tlsEnabled: true,
			exceptErr:  "",
		},
		{
			name: "tls config with no 'enable-tls', and credential files are not supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			tlsEnabled: false,
			exceptErr:  "",
		},
		{
			name: "tls config with 'enable-tls' set to false, and credential files are supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=false" +
				"&ca=/root/ca&cert=/root/cert&key=/root/key",
			tlsEnabled: false,
			exceptErr:  "credential files are supplied, but 'enable-tls' is set to false",
		},
		{
			name: "tls config with 'enable-tls' set to true, and some of " +
				"the credential files are not supplied ",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=true" +
				"&ca=/root/ca&cert=/root/cert&",
			tlsEnabled: false,
			exceptErr:  "ca, cert and key files should all be supplied",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := NewOptions()
			sinkURI, err := url.Parse(test.URI)
			require.NoError(t, err)
			req := &http.Request{URL: sinkURI}
			urlParameter := &urlConfig{}
			err = binding.Query.Bind(req, urlParameter)
			require.NoError(t, err)
			if test.exceptErr == "" {
				require.Nil(t, options.applyTLS(urlParameter))
			} else {
				require.Regexp(t, test.exceptErr, options.applyTLS(urlParameter).Error())
			}
			require.Equal(t, test.tlsEnabled, options.EnableTLS)
		})
	}
}

func TestCompleteSaramaSASLConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sasl   *saslConfig
		verify func(*testing.T, *sarama.Config)
	}{
		{
			name: "disabled",
			sasl: &saslConfig{},
			verify: func(t *testing.T, config *sarama.Config) {
				require.False(t, config.Net.SASL.Enable)
			},
		},
		{
			name: "PLAIN",
			sasl: &saslConfig{user: "user", password: "password", mechanism: plainMechanism},
			verify: func(t *testing.T, config *sarama.Config) {
				require.Equal(t, "user", config.Net.SASL.User)
				require.Equal(t, "password", config.Net.SASL.Password)
				require.Nil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
			},
		},
		{
			name: "SCRAM-SHA-256",
			sasl: &saslConfig{user: "user", password: "password", mechanism: scram256Mechanism},
			verify: func(t *testing.T, config *sarama.Config) {
				require.Equal(t, "user", config.Net.SASL.User)
				require.Equal(t, "password", config.Net.SASL.Password)
				require.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
			},
		},
		{
			name: "SCRAM-SHA-512",
			sasl: &saslConfig{user: "user", password: "password", mechanism: scram512Mechanism},
			verify: func(t *testing.T, config *sarama.Config) {
				require.Equal(t, "user", config.Net.SASL.User)
				require.Equal(t, "password", config.Net.SASL.Password)
				require.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)
			},
		},
		{
			name: "GSSAPI user auth",
			sasl: &saslConfig{mechanism: gssapiMechanism, gssapi: gssapiConfig{
				authType:           userAuth,
				kerberosConfigPath: "/etc/krb5.conf",
				serviceName:        "kafka",
				username:           "user",
				password:           "password",
				realm:              "EXAMPLE.COM",
				disablePAFXFAST:    true,
			}},
			verify: func(t *testing.T, config *sarama.Config) {
				require.Equal(t, int(userAuth), config.Net.SASL.GSSAPI.AuthType)
				require.Equal(t, "/etc/krb5.conf", config.Net.SASL.GSSAPI.KerberosConfigPath)
				require.Equal(t, "kafka", config.Net.SASL.GSSAPI.ServiceName)
				require.Equal(t, "user", config.Net.SASL.GSSAPI.Username)
				require.Equal(t, "password", config.Net.SASL.GSSAPI.Password)
				require.Empty(t, config.Net.SASL.GSSAPI.KeyTabPath)
				require.Equal(t, "EXAMPLE.COM", config.Net.SASL.GSSAPI.Realm)
				require.True(t, config.Net.SASL.GSSAPI.DisablePAFXFAST)
			},
		},
		{
			name: "GSSAPI keytab auth",
			sasl: &saslConfig{mechanism: gssapiMechanism, gssapi: gssapiConfig{
				authType:           keyTabAuth,
				keyTabPath:         "/tmp/user.keytab",
				kerberosConfigPath: "/etc/krb5.conf",
				serviceName:        "kafka",
				username:           "user",
				password:           "unused",
				realm:              "EXAMPLE.COM",
			}},
			verify: func(t *testing.T, config *sarama.Config) {
				require.Equal(t, int(keyTabAuth), config.Net.SASL.GSSAPI.AuthType)
				require.Equal(t, "/tmp/user.keytab", config.Net.SASL.GSSAPI.KeyTabPath)
				require.Empty(t, config.Net.SASL.GSSAPI.Password)
			},
		},
		{
			name: "OAUTHBEARER",
			sasl: &saslConfig{mechanism: oauthMechanism, oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     "http://127.0.0.1/token",
			}},
			verify: func(t *testing.T, config *sarama.Config) {
				require.NotNil(t, config.Net.SASL.TokenProvider)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := sarama.NewConfig()
			options := NewOptions()
			options.sasl = test.sasl
			err := completeSaramaSASLConfig(t.Context(), config, options)
			require.NoError(t, err)
			if test.sasl.mechanism != "" {
				require.True(t, config.Net.SASL.Enable)
				require.Equal(t, sarama.SASLMechanism(test.sasl.mechanism), config.Net.SASL.Mechanism)
			}
			test.verify(t, config)
		})
	}
}

func TestSaramaTimeout(t *testing.T) {
	options := NewOptions()
	saramaConfig, err := newSaramaConfig(context.Background(), options)
	require.NoError(t, err)
	require.Equal(t, options.DialTimeout, saramaConfig.Net.DialTimeout)
	require.Equal(t, options.WriteTimeout, saramaConfig.Net.WriteTimeout)
	require.Equal(t, options.ReadTimeout, saramaConfig.Net.ReadTimeout)
}
