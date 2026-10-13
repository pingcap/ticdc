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
	"net/url"
	"testing"

	"github.com/IBM/sarama"
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
	require.Equal(t, options.MaxMessageBytes, cfg.Producer.MaxMessageBytes)

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
	saslOptions.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: sarama.SASLTypeSCRAMSHA256,
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

func TestCompleteSaramaSASLConfig(t *testing.T) {
	t.Parallel()

	// Test that SASL is turned on correctly.
	options := NewOptions()
	options.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "",
		GSSAPI:        security.GSSAPI{},
	}
	ctx := context.Background()
	saramaConfig := sarama.NewConfig()
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.False(t, saramaConfig.Net.SASL.Enable)
	options.SASL.SASLMechanism = "plain"
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.True(t, saramaConfig.Net.SASL.Enable)
	// Test that the SCRAMClientGeneratorFunc is set up correctly.
	options = NewOptions()
	options.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "plain",
		GSSAPI:        security.GSSAPI{},
	}
	saramaConfig = sarama.NewConfig()
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.Nil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
	options.SASL.SASLMechanism = "SCRAM-SHA-512"
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.NotNil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
}

func TestSaramaTimeout(t *testing.T) {
	options := NewOptions()
	saramaConfig, err := newSaramaConfig(context.Background(), options)
	require.NoError(t, err)
	require.Equal(t, options.DialTimeout, saramaConfig.Net.DialTimeout)
	require.Equal(t, options.WriteTimeout, saramaConfig.Net.WriteTimeout)
	require.Equal(t, options.ReadTimeout, saramaConfig.Net.ReadTimeout)
}
