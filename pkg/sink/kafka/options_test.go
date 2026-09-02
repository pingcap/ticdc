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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

const (
	defaultMockTopicName = "mock_topic"

	// These values model Kafka admin responses, not TiCDC option defaults.
	mockBrokerMessageMaxBytes = "1048588"
	mockTopicMessageMaxBytes  = "1048588"
)

func TestKafkaClientSelection(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "client-selection")
	require.Equal(t, KafkaClientFranz, NewOptions().Client)

	for _, test := range []struct {
		name     string
		uri      string
		expected string
		wantErr  bool
	}{
		{
			name:     "URI selects sarama",
			uri:      "kafka://127.0.0.1:9092/topic?kafka-client=sarama",
			expected: KafkaClientSarama,
		},
		{
			name:     "URI value is case insensitive",
			uri:      "kafka://127.0.0.1:9092/topic?kafka-client=FRANZ",
			expected: KafkaClientFranz,
		},
		{
			name:    "invalid client",
			uri:     "kafka://127.0.0.1:9092/topic?kafka-client=other",
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sinkURI, err := url.Parse(test.uri)
			require.NoError(t, err)

			options := NewOptions()
			err = options.Apply(changefeedID, sinkURI, &config.SinkConfig{})
			if test.wantErr {
				require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, options.Client)
		})
	}
}

func TestFactorySelection(t *testing.T) {
	const topic = "factory-selection"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "factory-selection")
	for _, test := range []struct {
		client   string
		expected Factory
	}{
		{client: KafkaClientFranz, expected: &franzFactory{}},
		{client: KafkaClientSarama, expected: &saramaFactory{}},
	} {
		t.Run(test.client, func(t *testing.T) {
			o := NewOptions()
			o.Client = test.client
			o.ClientID = "ticdc-test"
			o.BrokerEndpoints = cluster.ListenAddrs()
			o.Topic = topic

			factory, err := NewFactory(context.Background(), o, changefeedID)
			require.NoError(t, err)
			require.IsType(t, test.expected, factory)

			factory.CleanupMetrics()
		})
	}
}

func TestCompleteOptions(t *testing.T) {
	options := NewOptions()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&required-acks=1"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)
	require.Equal(t, int16(3), options.ReplicationFactor)
	require.Equal(t, "2.6.0", options.Version)
	require.Equal(t, 4096, options.MaxMessageBytes)
	require.Equal(t, 4096, options.MaxBatchedBytes)
	require.Equal(t, WaitForLocal, options.RequiredAcks)
	require.Equal(t, defaultMaxRetry, options.MaxRetry)

	// multiple kafka broker endpoints
	uri = "kafka://127.0.0.1:9092,127.0.0.1:9091,127.0.0.1:9090/kafka-test?"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
		sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Len(t, options.BrokerEndpoints, 3)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))
	for _, replicationFactor := range []string{"0", "-1"} {
		uri = "kafka://127.0.0.1:9092/abc?replication-factor=" + replicationFactor
		sinkURI, err = url.Parse(uri)
		require.NoError(t, err)
		options = NewOptions()
		err = options.Apply(
			common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
			sinkURI,
			config.GetDefaultReplicaConfig().Sink,
		)
		require.ErrorContains(t, err, "invalid replication-factor "+replicationFactor)
	}

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-retry.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-retry=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid partition num.*", errors.Cause(err))

	// Unknown required-acks.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&required-acks=3"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid required acks 3.*", errors.Cause(err))

	// invalid kafka client id
	uri = "kafka://127.0.0.1:9092/abc?kafka-client-id=^invalid$"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.True(t, errors.ErrKafkaInvalidConfig.Equal(err))

	// max-retry accepts non-negative sink-uri values.
	uri = "kafka://127.0.0.1:9092/abc?max-retry=7"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, 7, options.MaxRetry)

	uri = "kafka://127.0.0.1:9092/abc?max-retry=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, 0, options.MaxRetry)

	// Negative max-retry values are ignored.
	uri = "kafka://127.0.0.1:9092/abc?max-retry=-1"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, defaultMaxRetry, options.MaxRetry)
}

func TestApplySASL(t *testing.T) {
	t.Parallel()

	const baseURI = "kafka://127.0.0.1:9092/abc"
	tests := []struct {
		name        string
		uri         string
		kafkaConfig *config.KafkaConfig
		expected    saslConfig
		expectErr   string
	}{
		{name: "no params", uri: baseURI},
		{
			name: "valid PLAIN SASL",
			uri:  baseURI + "?sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			expected: saslConfig{
				user:      "user",
				password:  "password",
				mechanism: plainMechanism,
			},
		},
		{
			name: "valid SCRAM-SHA-256 SASL",
			uri:  baseURI + "?sasl-user=user&sasl-password=password&sasl-mechanism=scram-sha-256",
			expected: saslConfig{
				user:      "user",
				password:  "password",
				mechanism: scram256Mechanism,
			},
		},
		{
			name: "valid SCRAM-SHA-512 SASL",
			uri:  baseURI + "?sasl-user=user&sasl-password=password&sasl-mechanism=SCRAM-SHA-512",
			expected: saslConfig{
				user:      "user",
				password:  "password",
				mechanism: scram512Mechanism,
			},
		},
		{
			name: "valid GSSAPI user auth SASL",
			uri: baseURI + "?sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=USER" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-password=pwd&sasl-gssapi-realm=realm" +
				"&sasl-gssapi-disable-pafxfast=false",
			expected: saslConfig{
				mechanism: gssapiMechanism,
				gssapi: gssapiConfig{
					authType:           userAuth,
					kerberosConfigPath: "/root/config",
					serviceName:        "a",
					username:           "user",
					password:           "pwd",
					realm:              "realm",
				},
			},
		},
		{
			name: "valid GSSAPI keytab auth SASL",
			uri: baseURI + "?sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=keytab" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-keytab-path=/root/keytab&sasl-gssapi-realm=realm" +
				"&sasl-gssapi-disable-pafxfast=false",
			expected: saslConfig{
				mechanism: gssapiMechanism,
				gssapi: gssapiConfig{
					authType:           keyTabAuth,
					keyTabPath:         "/root/keytab",
					kerberosConfigPath: "/root/config",
					serviceName:        "a",
					username:           "user",
					realm:              "realm",
				},
			},
		},
		{
			name:      "invalid mechanism",
			uri:       baseURI + "?sasl-mechanism=a",
			expectErr: "unknown SASL mechanism: a",
		},
		{
			name:      "invalid GSSAPI auth type",
			uri:       baseURI + "?sasl-mechanism=gssapi&sasl-gssapi-auth-type=keyta1b",
			expectErr: "unknown auth type: keyta1b",
		},
		{
			name: "valid OAUTHBEARER SASL",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID:     aws.String("client_id"),
				SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
				SASLOAuthTokenURL:     aws.String("127.0.0.1:9093/token"),
				SASLOAuthCA:           aws.String("/etc/ssl/oauth-ca.pem"),
			},
			expected: saslConfig{
				mechanism: oauthMechanism,
				oauth2: oauth2Config{
					clientID:     "client_id",
					clientSecret: "client_secret",
					tokenURL:     "127.0.0.1:9093/token",
					caPath:       "/etc/ssl/oauth-ca.pem",
					grantType:    "client_credentials",
				},
			},
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client id",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
				SASLOAuthTokenURL:     aws.String("127.0.0.1:9093/token"),
			},
			expectErr: "OAuth2 client id is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client secret",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID: aws.String("client_id"),
				SASLOAuthTokenURL: aws.String("127.0.0.1:9093/token"),
			},
			expectErr: "OAuth2 client secret is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing token url",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID:     aws.String("client_id"),
				SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
			},
			expectErr: "OAuth2 token url is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: non base64 client secret",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID:     aws.String("client_id"),
				SASLOAuthClientSecret: aws.String("client_secret"),
				SASLOAuthTokenURL:     aws.String("127.0.0.1:9093/token"),
			},
			expectErr: "OAuth2 client secret is not base64 encoded",
		},
		{
			name: "invalid OAUTHBEARER SASL: wrong mechanism",
			uri:  baseURI + "?sasl-mechanism=GSSAPI",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID:     aws.String("client_id"),
				SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
				SASLOAuthTokenURL:     aws.String("127.0.0.1:9093/token"),
			},
			expectErr: "OAuth2 is only supported with SASL mechanism type OAUTHBEARER",
		},
		{
			name: "invalid OAUTHBEARER SASL: empty CA path",
			uri:  baseURI + "?sasl-mechanism=OAUTHBEARER",
			kafkaConfig: &config.KafkaConfig{
				SASLOAuthClientID:     aws.String("client_id"),
				SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
				SASLOAuthTokenURL:     aws.String("127.0.0.1:9093/token"),
				SASLOAuthCA:           aws.String(""),
			},
			expectErr: "OAuth2 CA path cannot be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sinkURI, err := url.Parse(test.uri)
			require.NoError(t, err)
			replicaConfig := config.GetDefaultReplicaConfig()
			replicaConfig.Sink.KafkaConfig = test.kafkaConfig
			options := NewOptions()
			err = options.Apply(
				common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
				sinkURI,
				replicaConfig.Sink,
			)
			if test.expectErr != "" {
				require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
				require.ErrorContains(t, err, test.expectErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expected, *options.sasl)
		})
	}
}

func TestOAuthCAIsIndependentFromBrokerTLS(t *testing.T) {
	t.Parallel()

	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/abc?sasl-mechanism=OAUTHBEARER")
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		SASLOAuthClientID:     aws.String("client_id"),
		SASLOAuthClientSecret: aws.String("Y2xpZW50X3NlY3JldA=="),
		SASLOAuthTokenURL:     aws.String("https://oauth.example.com/token"),
		SASLOAuthCA:           aws.String("/etc/ssl/oauth-ca.pem"),
		CA:                    aws.String("/etc/ssl/broker-ca.pem"),
		Cert:                  aws.String("/etc/ssl/broker-cert.pem"),
		Key:                   aws.String("/etc/ssl/broker-key.pem"),
	}

	options := NewOptions()
	require.NoError(t, options.Apply(
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
		sinkURI,
		replicaConfig.Sink,
	))
	require.Equal(t, "/etc/ssl/oauth-ca.pem", options.sasl.oauth2.caPath)
	require.Equal(t, "/etc/ssl/broker-ca.pem", options.Credential.CAPath)
	require.Equal(t, "/etc/ssl/broker-cert.pem", options.Credential.CertPath)
	require.Equal(t, "/etc/ssl/broker-key.pem", options.Credential.KeyPath)
}

func TestApplyTLS(t *testing.T) {
	t.Parallel()

	const baseURI = "kafka://127.0.0.1:9092/abc"
	tests := []struct {
		name               string
		uri                string
		expectedTLS        bool
		expectedCredential security.Credential
		expectErr          string
	}{
		{
			name:        "tls config with enable-tls set to true",
			uri:         baseURI + "?enable-tls=true",
			expectedTLS: true,
		},
		{
			name: "tls config with no enable-tls and credential files supplied",
			uri:  baseURI + "?ca=/root/ca.file&cert=/root/cert.file&key=/root/key.file",
			expectedCredential: security.Credential{
				CAPath:   "/root/ca.file",
				CertPath: "/root/cert.file",
				KeyPath:  "/root/key.file",
			},
			expectedTLS: true,
		},
		{name: "tls config with no enable-tls and no credential files", uri: baseURI},
		{
			name: "tls config with enable-tls false and credential files supplied",
			uri:  baseURI + "?enable-tls=false&ca=/root/ca&cert=/root/cert&key=/root/key",
			expectedCredential: security.Credential{
				CAPath:   "/root/ca",
				CertPath: "/root/cert",
				KeyPath:  "/root/key",
			},
			expectErr: "credential files are supplied, but 'enable-tls' is set to false",
		},
		{
			name: "tls config with incomplete credential files",
			uri:  baseURI + "?enable-tls=true&ca=/root/ca&cert=/root/cert",
			expectedCredential: security.Credential{
				CAPath:   "/root/ca",
				CertPath: "/root/cert",
			},
			expectErr: "ca, cert and key files should all be supplied",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sinkURI, err := url.Parse(test.uri)
			require.NoError(t, err)
			options := NewOptions()
			err = options.Apply(
				common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
				sinkURI,
				config.GetDefaultReplicaConfig().Sink,
			)
			if test.expectErr != "" {
				require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
				require.ErrorContains(t, err, test.expectErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, test.expectedTLS, options.EnableTLS)
			require.Equal(t, test.expectedCredential, *options.Credential)
		})
	}
}

func TestApplyRejectsNonPositiveMaxMessageBytes(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		configValue *int
		expected    int
	}{
		{
			name:     "zero from URI",
			uri:      "kafka://127.0.0.1:9092/test-topic?max-message-bytes=0",
			expected: 0,
		},
		{
			name:     "negative from URI",
			uri:      "kafka://127.0.0.1:9092/test-topic?max-message-bytes=-1",
			expected: -1,
		},
		{
			name:        "zero from sink config",
			uri:         "kafka://127.0.0.1:9092/test-topic",
			configValue: aws.Int(0),
			expected:    0,
		},
		{
			name:        "negative from sink config",
			uri:         "kafka://127.0.0.1:9092/test-topic",
			configValue: aws.Int(-1),
			expected:    -1,
		},
	}

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sinkURI, err := url.Parse(test.uri)
			require.NoError(t, err)

			sinkConfig := config.GetDefaultReplicaConfig().Sink
			if test.configValue != nil {
				sinkConfig.KafkaConfig = &config.KafkaConfig{
					MaxMessageBytes: test.configValue,
				}
			}

			options := NewOptions()
			err = options.Apply(changefeedID, sinkURI, sinkConfig)
			require.ErrorContains(
				t, err, fmt.Sprintf("invalid max-message-bytes %d", test.expected))
			errCode, ok := errors.RFCCode(err)
			require.True(t, ok)
			require.Equal(t, errors.ErrKafkaInvalidConfig.RFCCode(), errCode)
		})
	}
}

func TestSetPartitionNum(t *testing.T) {
	options := NewOptions()
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
	err := options.setPartitionNum(changefeedID, 2)
	require.NoError(t, err)
	require.Equal(t, int32(2), options.PartitionNum)

	options.PartitionNum = 1
	err = options.setPartitionNum(changefeedID, 2)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)

	options.PartitionNum = 3
	err = options.setPartitionNum(changefeedID, 2)
	require.True(t, errors.ErrKafkaInvalidConfig.Equal(err))
}

func TestClientID(t *testing.T) {
	testCases := []struct {
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{
			"domain:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_domain_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234?:,\"", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_____default_123-121-121-121",
		},
		{
			"中文", "123-121-121-121",
			"", true, "",
		},
		{
			"127.0.0.1:1234",
			"123-121-121-121", "cdc-changefeed-1", false,
			"cdc-changefeed-1",
		},
	}
	for _, tc := range testCases {
		id, err := NewKafkaClientID(tc.addr,
			common.NewChangefeedID4Test(common.DefaultKeyspaceName, tc.changefeedID), tc.configuredID)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, id)
		}
	}
}

func TestTimeout(t *testing.T) {
	options := NewOptions()
	require.Equal(t, 10*time.Second, options.DialTimeout)
	require.Equal(t, 10*time.Second, options.ReadTimeout)
	require.Equal(t, 10*time.Second, options.WriteTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)

	require.Equal(t, 5*time.Second, options.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, options.ReadTimeout)
	require.Equal(t, 2*time.Minute, options.WriteTimeout)
}

func TestApplyRejectsNonPositiveTimeout(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
	for _, parameter := range []string{"dial-timeout", "read-timeout", "write-timeout"} {
		for _, value := range []string{"0s", "-1s"} {
			t.Run(parameter+"="+value, func(t *testing.T) {
				t.Parallel()

				sinkURI, err := url.Parse(
					"kafka://127.0.0.1:9092/kafka-test?" + parameter + "=" + value)
				require.NoError(t, err)

				err = NewOptions().Apply(
					changefeedID, sinkURI, config.GetDefaultReplicaConfig().Sink)
				require.ErrorContains(t, err, parameter+" must be greater than zero")
				errCode, ok := errors.RFCCode(err)
				require.True(t, ok)
				require.Equal(t, errors.ErrKafkaInvalidConfig.RFCCode(), errCode)
			})
		}
	}
}

func TestAdjustConfigFallsBackToBrokerMessageMaxBytesWhenTopicConfigMissing(t *testing.T) {
	brokerMessageMaxBytes, err := strconv.Atoi(mockBrokerMessageMaxBytes)
	require.NoError(t, err)

	tests := []struct {
		name                      string
		configuredMaxMessageBytes int
	}{
		{
			name:                      "uses broker limit when configured value is below broker",
			configuredMaxMessageBytes: 1024,
		},
		{
			name:                      "uses broker limit when configured value is below broker by one byte",
			configuredMaxMessageBytes: brokerMessageMaxBytes - 1,
		},
		{
			name:                      "uses broker limit when configured value is above broker",
			configuredMaxMessageBytes: brokerMessageMaxBytes + 1,
		},
	}

	topicName := "test-topic"
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			adminClient := NewMockAdminClient(ctrl)
			gomock.InOrder(
				adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{topicName}, true).Return(
					map[string]TopicDetail{
						topicName: {Name: topicName, NumPartitions: 3},
					}, nil),
				adminClient.EXPECT().GetTopicConfig(gomock.Any(), topicName, TopicMaxMessageBytesConfigName).
					Return("", false, nil),
				adminClient.EXPECT().GetBrokerConfig(gomock.Any(), BrokerMessageMaxBytesConfigName).
					Return(mockBrokerMessageMaxBytes, true, nil),
			)
			sinkURI, err := url.Parse(fmt.Sprintf(
				"kafka://127.0.0.1:9092/%s?max-message-bytes=%d",
				topicName, test.configuredMaxMessageBytes,
			))
			require.NoError(t, err)

			options := NewOptions()
			err = options.Apply(changefeedID, sinkURI, config.GetDefaultReplicaConfig().Sink)
			require.NoError(t, err)
			require.Equal(t, test.configuredMaxMessageBytes, options.MaxMessageBytes)
			require.Equal(t, test.configuredMaxMessageBytes, options.MaxBatchedBytes)

			err = adjustOptions(t.Context(), changefeedID, adminClient, options, topicName)
			require.NoError(t, err)

			require.NotEqual(t, test.configuredMaxMessageBytes, options.MaxMessageBytes)
			require.Equal(t, brokerMessageMaxBytes, options.MaxMessageBytes)
			require.Equal(
				t,
				min(test.configuredMaxMessageBytes, brokerMessageMaxBytes),
				options.MaxBatchedBytes,
			)
		})
	}
}

func TestValidateReplicationFactor(t *testing.T) {
	ctrl := gomock.NewController(t)
	adminClient := NewMockAdminClient(ctrl)
	gomock.InOrder(
		adminClient.EXPECT().GetBrokerConfig(gomock.Any(), MinInsyncReplicasConfigName).
			Return("2", true, nil),
		adminClient.EXPECT().GetBrokerConfig(gomock.Any(), MinInsyncReplicasConfigName).
			Return("", false, nil),
	)

	topicConfig := &AutoCreateTopicConfig{
		AutoCreate:        true,
		ReplicationFactor: 1,
		RequiredAcks:      WaitForAll,
	}
	err := topicConfig.ValidateReplicationFactor(t.Context(), adminClient)
	require.Regexp(
		t,
		".*`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker.*",
		errors.Cause(err),
	)

	localAcksConfig := &AutoCreateTopicConfig{
		AutoCreate:        true,
		ReplicationFactor: 1,
		RequiredAcks:      WaitForLocal,
	}
	err = localAcksConfig.ValidateReplicationFactor(t.Context(), adminClient)
	require.NoError(t, err)

	missingBrokerConfig := &AutoCreateTopicConfig{
		AutoCreate:        true,
		ReplicationFactor: 1,
		RequiredAcks:      WaitForAll,
	}
	err = missingBrokerConfig.ValidateReplicationFactor(t.Context(), adminClient)
	require.NoError(t, err)

	t.Run("replication factor satisfies min insync replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetBrokerConfig(gomock.Any(), MinInsyncReplicasConfigName).
			Return("2", true, nil)

		topicConfig := &AutoCreateTopicConfig{
			ReplicationFactor: 3,
			RequiredAcks:      WaitForAll,
		}

		err := topicConfig.ValidateReplicationFactor(t.Context(), adminClient)
		require.NoError(t, err)
	})

	t.Run("invalid min insync replicas", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetBrokerConfig(gomock.Any(), MinInsyncReplicasConfigName).
			Return("invalid", true, nil)

		topicConfig := &AutoCreateTopicConfig{
			ReplicationFactor: 3,
			RequiredAcks:      WaitForAll,
		}

		err := topicConfig.ValidateReplicationFactor(t.Context(), adminClient)
		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
	})

	t.Run("broker config lookup failure skips validation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		adminClient := NewMockAdminClient(ctrl)
		lookupErr := errors.ErrKafkaAdminAPI.GenWithStackByArgs(
			"describe-config",
			MinInsyncReplicasConfigName,
		)
		adminClient.EXPECT().GetBrokerConfig(gomock.Any(), MinInsyncReplicasConfigName).
			Return("", false, lookupErr)

		topicConfig := &AutoCreateTopicConfig{
			ReplicationFactor: 1,
			RequiredAcks:      WaitForAll,
		}

		err := topicConfig.ValidateReplicationFactor(t.Context(), adminClient)
		require.NoError(t, err)
	})
}

func TestConfigurationCombinations(t *testing.T) {
	combinations := []struct {
		name                  string
		uriTemplate           string
		uriParams             []any
		brokerMessageMaxBytes string
		topicMaxMessageBytes  string
	}{
		{
			"new topic default limited by broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"not-exist-topic"},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"new topic default equals broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"not-exist-topic"},
			strconv.Itoa(config.DefaultMaxMessageBytes),
			mockTopicMessageMaxBytes,
		},
		{
			"new topic default below broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			mockTopicMessageMaxBytes,
		},
		{
			"new topic user below broker and default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(1024*1024 - 1)},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"new topic user below default below broker",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			mockTopicMessageMaxBytes,
		},
		{
			"new topic broker below user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(1024*1024 + 1)},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"new topic broker below default and user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"new topic user below broker above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			mockTopicMessageMaxBytes,
		},
		{
			"new topic broker below user above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			mockTopicMessageMaxBytes,
		},
		{
			"existing topic default limited by topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"existing topic default equals topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			mockBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		{
			"existing topic default below topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			mockBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		{
			"existing topic user below topic and default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{defaultMockTopicName, strconv.Itoa(1024*1024 - 1)},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"existing topic user below default below topic",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			},
			mockBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		{
			"existing topic topic below user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{defaultMockTopicName, strconv.Itoa(1024*1024 + 1)},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"existing topic topic below default and user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			mockBrokerMessageMaxBytes,
			mockTopicMessageMaxBytes,
		},
		{
			"existing topic user below topic above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			mockBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
		},
		{
			"existing topic topic below user above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			},
			mockBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
	}

	for _, a := range combinations {
		t.Run(a.name, func(t *testing.T) {
			uri := fmt.Sprintf(a.uriTemplate, a.uriParams...)
			sinkURI, err := url.Parse(uri)
			require.Nil(t, err)

			topic, ok := a.uriParams[0].(string)
			require.True(t, ok)
			require.NotEqual(t, "", topic)

			ctrl := gomock.NewController(t)
			adminClient := NewMockAdminClient(ctrl)
			metadataCall := adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{topic}, true)
			sourceMaxMessageBytes := a.brokerMessageMaxBytes
			if topic == defaultMockTopicName {
				metadataCall.Return(map[string]TopicDetail{
					topic: {Name: topic, NumPartitions: defaultPartitionNum},
				}, nil)
				gomock.InOrder(
					metadataCall,
					adminClient.EXPECT().GetTopicConfig(gomock.Any(), topic, TopicMaxMessageBytesConfigName).
						Return(a.topicMaxMessageBytes, true, nil),
				)
				sourceMaxMessageBytes = a.topicMaxMessageBytes
			} else {
				metadataCall.Return(map[string]TopicDetail{}, nil)
				gomock.InOrder(
					metadataCall,
					adminClient.EXPECT().GetBrokerConfig(gomock.Any(), BrokerMessageMaxBytesConfigName).
						Return(a.brokerMessageMaxBytes, true, nil),
				)
			}

			options := NewOptions()
			err = options.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
			require.Nil(t, err)
			configuredMaxMessageBytes := options.MaxMessageBytes

			expectedMaxMessageBytes, err := strconv.Atoi(sourceMaxMessageBytes)
			require.NoError(t, err)
			changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test")
			err = adjustOptions(t.Context(), changefeedID, adminClient, options, topic)
			require.Nil(t, err)
			require.Equal(t, expectedMaxMessageBytes, options.MaxMessageBytes)
			require.Equal(
				t,
				min(configuredMaxMessageBytes, expectedMaxMessageBytes),
				options.MaxBatchedBytes,
			)
		})
	}
}

func TestMerge(t *testing.T) {
	uri := "kafka://topic/prefix"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(12),
		ReplicationFactor:         aws.Int16(5),
		KafkaVersion:              aws.String("3.1.2"),
		MaxMessageBytes:           aws.Int(1024 * 1024),
		Compression:               aws.String("gzip"),
		KafkaClientID:             aws.String("test-id"),
		AutoCreateTopic:           aws.Bool(true),
		DialTimeout:               aws.String("1m1s"),
		WriteTimeout:              aws.String("2m1s"),
		RequiredAcks:              aws.Int(1),
		SASLUser:                  aws.String("abc"),
		SASLPassword:              aws.String("123"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("keytab"),
		SASLGssAPIKeytabPath:      aws.String("SASLGssAPIKeytabPath"),
		SASLGssAPIServiceName:     aws.String("service"),
		SASLGssAPIUser:            aws.String("user"),
		SASLGssAPIPassword:        aws.String("pass"),
		SASLGssAPIRealm:           aws.String("realm"),
		SASLGssAPIDisablePafxfast: aws.Bool(true),
		EnableTLS:                 aws.Bool(true),
		CA:                        aws.String("ca.pem"),
		Cert:                      aws.String("cert.pem"),
		Key:                       aws.String("key.pem"),
	}
	c := NewOptions()
	err = c.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, replicaConfig.Sink)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, 1024*1024, c.MaxBatchedBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.sasl.user)
	require.Equal(t, "123", c.sasl.password)
	require.Equal(t, "plain", strings.ToLower(string(c.sasl.mechanism)))
	require.Equal(t, 2, int(c.sasl.gssapi.authType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.sasl.gssapi.keyTabPath)
	require.Equal(t, "service", c.sasl.gssapi.serviceName)
	require.Equal(t, "user", c.sasl.gssapi.username)
	require.Equal(t, "pass", c.sasl.gssapi.password)
	require.Equal(t, "realm", c.sasl.gssapi.realm)
	require.Equal(t, true, c.sasl.gssapi.disablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)

	// test override
	uri = "kafka://topic?partition-num=12" +
		"&replication-factor=5" +
		"&kafka-version=3.1.2" +
		"&max-message-bytes=1048576" +
		"&compression=gzip" +
		"&kafka-client-id=test-id" +
		"&auto-create-topic=true" +
		"&dial-timeout=1m1s" +
		"&write-timeout=2m1s" +
		"&required-acks=1" +
		"&sasl-user=abc" +
		"&sasl-password=123" +
		"&sasl-mechanism=plain" +
		"&sasl-gssapi-auth-type=keytab" +
		"&sasl-gssapi-keytab-path=SASLGssAPIKeytabPath" +
		"&sasl-gssapi-service-name=service" +
		"&sasl-gssapi-user=user" +
		"&sasl-gssapi-password=pass" +
		"&sasl-gssapi-realm=realm" +
		"&sasl-gssapi-disable-pafxfast=true" +
		"&enable-tls=true" +
		"&ca=ca.pem" +
		"&cert=cert.pem" +
		"&key=key.pem"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(11),
		ReplicationFactor:         aws.Int16(3),
		KafkaVersion:              aws.String("3.2.2"),
		MaxMessageBytes:           aws.Int(1023 * 1024),
		Compression:               aws.String("none"),
		KafkaClientID:             aws.String("test2-id"),
		AutoCreateTopic:           aws.Bool(false),
		DialTimeout:               aws.String("1m2s"),
		WriteTimeout:              aws.String("2m3s"),
		RequiredAcks:              aws.Int(-1),
		SASLUser:                  aws.String("abcd"),
		SASLPassword:              aws.String("1234"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("user"),
		SASLGssAPIKeytabPath:      aws.String("path"),
		SASLGssAPIServiceName:     aws.String("service2"),
		SASLGssAPIUser:            aws.String("usera"),
		SASLGssAPIPassword:        aws.String("pass2"),
		SASLGssAPIRealm:           aws.String("realm2"),
		SASLGssAPIDisablePafxfast: aws.Bool(false),
		EnableTLS:                 aws.Bool(false),
		CA:                        aws.String("ca2.pem"),
		Cert:                      aws.String("cert2.pem"),
		Key:                       aws.String("key2.pem"),
	}
	c = NewOptions()
	err = c.Apply(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"), sinkURI, replicaConfig.Sink)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, 1024*1024, c.MaxBatchedBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.sasl.user)
	require.Equal(t, "123", c.sasl.password)
	require.Equal(t, "plain", strings.ToLower(string(c.sasl.mechanism)))
	require.Equal(t, 2, int(c.sasl.gssapi.authType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.sasl.gssapi.keyTabPath)
	require.Equal(t, "service", c.sasl.gssapi.serviceName)
	require.Equal(t, "user", c.sasl.gssapi.username)
	require.Equal(t, "pass", c.sasl.gssapi.password)
	require.Equal(t, "realm", c.sasl.gssapi.realm)
	require.Equal(t, true, c.sasl.gssapi.disablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)
}
