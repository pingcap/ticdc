// Copyright 2025 PingCAP, Inc.
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
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin/binding"
	"github.com/imdario/mergo"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

const (
	// defaultPartitionNum specifies the default number of partitions when we create the topic.
	defaultPartitionNum = 3
	// defaultMaxRetry is the default retry budget for Kafka producers.
	defaultMaxRetry = 5
)

const (
	// BrokerMessageMaxBytesConfigName specifies the largest record batch size allowed by
	// Kafka brokers.
	// See: https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes
	BrokerMessageMaxBytesConfigName = "message.max.bytes"
	// TopicMaxMessageBytesConfigName specifies the largest record batch size allowed by
	// Kafka topics.
	// See: https://kafka.apache.org/documentation/#topicconfigs_max.message.bytes
	TopicMaxMessageBytesConfigName = "max.message.bytes"
	// MinInsyncReplicasConfigName the minimum number of replicas that must acknowledge a write
	// for the write to be considered successful.
	// Only works if the producer's acks is "all" (or "-1").
	// See: https://kafka.apache.org/documentation/#brokerconfigs_min.insync.replicas and
	// https://kafka.apache.org/documentation/#topicconfigs_min.insync.replicas
	MinInsyncReplicasConfigName = "min.insync.replicas"
)

const (
	// SASLTypePlaintext represents the plain mechanism
	SASLTypePlaintext = "PLAIN"
	// SASLTypeSCRAMSHA256 represents the SCRAM-SHA-256 mechanism.
	SASLTypeSCRAMSHA256 = "SCRAM-SHA-256"
	// SASLTypeSCRAMSHA512 represents the SCRAM-SHA-512 mechanism.
	SASLTypeSCRAMSHA512 = "SCRAM-SHA-512"
	// SASLTypeGSSAPI represents the gssapi mechanism.
	SASLTypeGSSAPI = "GSSAPI"
	// SASLTypeOAuth represents the SASL/OAUTHBEARER mechanism (Kafka 2.0.0+)
	SASLTypeOAuth = "OAUTHBEARER"
)

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
	// Unknown should never have been use in real config.
	Unknown RequiredAcks = 2
)

func requireAcksFromString(acks int) (RequiredAcks, error) {
	switch acks {
	case int(WaitForAll):
		return WaitForAll, nil
	case int(WaitForLocal):
		return WaitForLocal, nil
	case int(NoResponse):
		return NoResponse, nil
	default:
		return Unknown, errors.ErrKafkaInvalidRequiredAcks.GenWithStackByArgs(acks)
	}
}

type urlConfig struct {
	PartitionNum                 *int32  `form:"partition-num"`
	ReplicationFactor            *int16  `form:"replication-factor"`
	KafkaVersion                 *string `form:"kafka-version"`
	MaxMessageBytes              *int    `form:"max-message-bytes"`
	MaxRetry                     *int    `form:"max-retry"`
	Compression                  *string `form:"compression"`
	KafkaClientID                *string `form:"kafka-client-id"`
	AutoCreateTopic              *bool   `form:"auto-create-topic"`
	DialTimeout                  *string `form:"dial-timeout"`
	WriteTimeout                 *string `form:"write-timeout"`
	ReadTimeout                  *string `form:"read-timeout"`
	RequiredAcks                 *int    `form:"required-acks"`
	SASLUser                     *string `form:"sasl-user"`
	SASLPassword                 *string `form:"sasl-password"`
	SASLMechanism                *string `form:"sasl-mechanism"`
	SASLGssAPIAuthType           *string `form:"sasl-gssapi-auth-type"`
	SASLGssAPIKeytabPath         *string `form:"sasl-gssapi-keytab-path"`
	SASLGssAPIKerberosConfigPath *string `form:"sasl-gssapi-kerberos-config-path"`
	SASLGssAPIServiceName        *string `form:"sasl-gssapi-service-name"`
	SASLGssAPIUser               *string `form:"sasl-gssapi-user"`
	SASLGssAPIPassword           *string `form:"sasl-gssapi-password"`
	SASLGssAPIRealm              *string `form:"sasl-gssapi-realm"`
	SASLGssAPIDisablePafxfast    *bool   `form:"sasl-gssapi-disable-pafxfast"`
	EnableTLS                    *bool   `form:"enable-tls"`
	CA                           *string `form:"ca"`
	Cert                         *string `form:"cert"`
	Key                          *string `form:"key"`
	InsecureSkipVerify           *bool   `form:"insecure-skip-verify"`
}

// options stores Kafka sink configurations
type options struct {
	Topic           string
	BrokerEndpoints []string

	// control whether to create topic
	AutoCreate   bool
	PartitionNum int32
	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16
	Version           string
	IsAssignedVersion bool
	RequestVersion    int16

	// MaxMessageBytes controls the byte size limit of the producer.
	MaxMessageBytes int
	// MaxBatchedBytes controls the byte size limit when batching messages.
	MaxBatchedBytes int

	MaxRetry     int
	Compression  string
	ClientID     string
	RequiredAcks RequiredAcks

	// Credential is used to connect to kafka cluster.
	EnableTLS          bool
	Credential         *security.Credential
	InsecureSkipVerify bool
	SASL               *security.SASL

	// Timeout for network configurations, default to `10s`
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// NewOptions returns a default Kafka configuration
func NewOptions() *options {
	return &options{
		Version:            "2.4.0",
		MaxMessageBytes:    config.DefaultMaxMessageBytes,
		MaxBatchedBytes:    config.DefaultMaxMessageBytes,
		MaxRetry:           defaultMaxRetry,
		ReplicationFactor:  1,
		Compression:        "none",
		RequiredAcks:       WaitForAll,
		Credential:         &security.Credential{},
		InsecureSkipVerify: false,
		SASL:               &security.SASL{},
		AutoCreate:         true,
		DialTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		ReadTimeout:        10 * time.Second,
	}
}

// setPartitionNum set the partition-num by the topic's partition count.
func (o *options) setPartitionNum(changefeedID common.ChangeFeedID, realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if o.PartitionNum == 0 {
		o.PartitionNum = realPartitionCount
		log.Info("partitionNum is not set, set by topic's partition-num",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.Int32("partitionNum", realPartitionCount))
		return nil
	}

	if o.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.Int32("sinkUriPartitions", o.PartitionNum), zap.Int32("topicPartitions", realPartitionCount))
		return nil
	}

	// Make sure that the user-specified `partition-num` is not greater than
	// the real partition count, since messages would be dispatched to different
	// partitions, this could prevent potential correctness problems.
	if o.PartitionNum > realPartitionCount {
		return errors.ErrKafkaInvalidPartitionNum.GenWithStack(
			"the number of partition (%d) specified in sink-uri is more than that of actual topic (%d)",
			o.PartitionNum, realPartitionCount)
	}
	return nil
}

// Apply the sinkURI to update options
func (o *options) Apply(changefeedID common.ChangeFeedID,
	sinkURI *url.URL, sinkConfig *config.SinkConfig,
) error {
	o.BrokerEndpoints = strings.Split(sinkURI.Host, ",")

	var err error
	req := &http.Request{URL: sinkURI}
	urlParameter := &urlConfig{}
	if err = binding.Query.Bind(req, urlParameter); err != nil {
		return errors.WrapError(errors.ErrMySQLInvalidConfig, err)
	}
	if urlParameter, err = mergeConfig(sinkConfig, urlParameter); err != nil {
		return err
	}
	if urlParameter.PartitionNum != nil {
		o.PartitionNum = *urlParameter.PartitionNum
		if o.PartitionNum <= 0 {
			return errors.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(o.PartitionNum)
		}
	}

	if urlParameter.ReplicationFactor != nil {
		o.ReplicationFactor = *urlParameter.ReplicationFactor
	}

	if urlParameter.KafkaVersion != nil {
		o.Version = *urlParameter.KafkaVersion
		o.IsAssignedVersion = true
	}

	if urlParameter.MaxMessageBytes != nil {
		o.MaxMessageBytes = *urlParameter.MaxMessageBytes
	}
	o.MaxBatchedBytes = o.MaxMessageBytes

	if urlParameter.MaxRetry != nil && *urlParameter.MaxRetry >= 0 {
		o.MaxRetry = *urlParameter.MaxRetry
	}

	if urlParameter.Compression != nil {
		o.Compression = *urlParameter.Compression
	}

	var kafkaClientID string
	if urlParameter.KafkaClientID != nil {
		kafkaClientID = *urlParameter.KafkaClientID
	}
	clientID, err := NewKafkaClientID(
		config.GetGlobalServerConfig().AdvertiseAddr,
		changefeedID,
		kafkaClientID)
	if err != nil {
		return err
	}
	o.ClientID = clientID

	if urlParameter.AutoCreateTopic != nil {
		o.AutoCreate = *urlParameter.AutoCreateTopic
	}

	if urlParameter.DialTimeout != nil && *urlParameter.DialTimeout != "" {
		a, err := time.ParseDuration(*urlParameter.DialTimeout)
		if err != nil {
			return err
		}
		o.DialTimeout = a
	}

	if urlParameter.WriteTimeout != nil && *urlParameter.WriteTimeout != "" {
		a, err := time.ParseDuration(*urlParameter.WriteTimeout)
		if err != nil {
			return err
		}
		o.WriteTimeout = a
	}

	if urlParameter.ReadTimeout != nil && *urlParameter.ReadTimeout != "" {
		a, err := time.ParseDuration(*urlParameter.ReadTimeout)
		if err != nil {
			return err
		}
		o.ReadTimeout = a
	}

	if urlParameter.RequiredAcks != nil {
		r, err := requireAcksFromString(*urlParameter.RequiredAcks)
		if err != nil {
			return err
		}
		o.RequiredAcks = r
	}

	err = o.applySASL(urlParameter, sinkConfig)
	if err != nil {
		return err
	}

	err = o.applyTLS(urlParameter)
	if err != nil {
		return err
	}

	return nil
}

func mergeConfig(
	sinkConfig *config.SinkConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	if sinkConfig != nil && sinkConfig.KafkaConfig != nil {
		fileConifg := sinkConfig.KafkaConfig
		dest.PartitionNum = fileConifg.PartitionNum
		dest.ReplicationFactor = fileConifg.ReplicationFactor
		dest.KafkaVersion = fileConifg.KafkaVersion
		dest.MaxMessageBytes = fileConifg.MaxMessageBytes
		dest.Compression = fileConifg.Compression
		dest.KafkaClientID = fileConifg.KafkaClientID
		dest.AutoCreateTopic = fileConifg.AutoCreateTopic
		dest.DialTimeout = fileConifg.DialTimeout
		dest.WriteTimeout = fileConifg.WriteTimeout
		dest.ReadTimeout = fileConifg.ReadTimeout
		dest.RequiredAcks = fileConifg.RequiredAcks
		dest.SASLUser = fileConifg.SASLUser
		dest.SASLPassword = fileConifg.SASLPassword
		dest.SASLMechanism = fileConifg.SASLMechanism
		dest.SASLGssAPIDisablePafxfast = fileConifg.SASLGssAPIDisablePafxfast
		dest.SASLGssAPIAuthType = fileConifg.SASLGssAPIAuthType
		dest.SASLGssAPIKeytabPath = fileConifg.SASLGssAPIKeytabPath
		dest.SASLGssAPIServiceName = fileConifg.SASLGssAPIServiceName
		dest.SASLGssAPIKerberosConfigPath = fileConifg.SASLGssAPIKerberosConfigPath
		dest.SASLGssAPIRealm = fileConifg.SASLGssAPIRealm
		dest.SASLGssAPIUser = fileConifg.SASLGssAPIUser
		dest.SASLGssAPIPassword = fileConifg.SASLGssAPIPassword
		dest.EnableTLS = fileConifg.EnableTLS
		dest.CA = fileConifg.CA
		dest.Cert = fileConifg.Cert
		dest.Key = fileConifg.Key
		dest.InsecureSkipVerify = fileConifg.InsecureSkipVerify
	}
	if err := mergo.Merge(dest, urlParameters, mergo.WithOverride); err != nil {
		return nil, err
	}
	return dest, nil
}

func (o *options) applyTLS(params *urlConfig) error {
	if params.CA != nil && *params.CA != "" {
		o.Credential.CAPath = *params.CA
	}

	if params.Cert != nil && *params.Cert != "" {
		o.Credential.CertPath = *params.Cert
	}

	if params.Key != nil && *params.Key != "" {
		o.Credential.KeyPath = *params.Key
	}

	if o.Credential != nil && !o.Credential.IsEmpty() &&
		!o.Credential.IsTLSEnabled() {
		return errors.WrapError(errors.ErrKafkaInvalidConfig,
			errors.New("ca, cert and key files should all be supplied"))
	}

	// if enable-tls is not set, but credential files are set,
	//    then tls should be enabled, and the self-signed CA certificate is used.
	// if enable-tls is set to true, and credential files are not set,
	//	  then tls should be enabled, and the trusted CA certificate on OS is used.
	// if enable-tls is set to false, and credential files are set,
	//	  then an error is returned.
	if params.EnableTLS != nil {
		enableTLS := *params.EnableTLS

		if o.Credential != nil && o.Credential.IsTLSEnabled() && !enableTLS {
			return errors.WrapError(errors.ErrKafkaInvalidConfig,
				errors.New("credential files are supplied, but 'enable-tls' is set to false"))
		}
		o.EnableTLS = enableTLS
	} else {
		if o.Credential != nil && o.Credential.IsTLSEnabled() {
			o.EnableTLS = true
		}
	}

	// Only set InsecureSkipVerify when enable the TLS.
	if o.EnableTLS && params.InsecureSkipVerify != nil {
		o.InsecureSkipVerify = *params.InsecureSkipVerify
	}

	return nil
}

func (o *options) applySASL(urlParameter *urlConfig, sinkConfig *config.SinkConfig) error {
	if urlParameter.SASLUser != nil && *urlParameter.SASLUser != "" {
		o.SASL.SASLUser = *urlParameter.SASLUser
	}

	if urlParameter.SASLPassword != nil && *urlParameter.SASLPassword != "" {
		o.SASL.SASLPassword = *urlParameter.SASLPassword
	}

	if urlParameter.SASLMechanism != nil && *urlParameter.SASLMechanism != "" {
		mechanism, err := security.SASLMechanismFromString(*urlParameter.SASLMechanism)
		if err != nil {
			return errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
		o.SASL.SASLMechanism = mechanism
	}

	if urlParameter.SASLGssAPIAuthType != nil && *urlParameter.SASLGssAPIAuthType != "" {
		authType, err := security.AuthTypeFromString(*urlParameter.SASLGssAPIAuthType)
		if err != nil {
			return errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
		o.SASL.GSSAPI.AuthType = authType
	}

	if urlParameter.SASLGssAPIKeytabPath != nil && *urlParameter.SASLGssAPIKeytabPath != "" {
		o.SASL.GSSAPI.KeyTabPath = *urlParameter.SASLGssAPIKeytabPath
	}

	if urlParameter.SASLGssAPIKerberosConfigPath != nil &&
		*urlParameter.SASLGssAPIKerberosConfigPath != "" {
		o.SASL.GSSAPI.KerberosConfigPath = *urlParameter.SASLGssAPIKerberosConfigPath
	}

	if urlParameter.SASLGssAPIServiceName != nil && *urlParameter.SASLGssAPIServiceName != "" {
		o.SASL.GSSAPI.ServiceName = *urlParameter.SASLGssAPIServiceName
	}

	if urlParameter.SASLGssAPIUser != nil && *urlParameter.SASLGssAPIUser != "" {
		o.SASL.GSSAPI.Username = *urlParameter.SASLGssAPIUser
	}

	if urlParameter.SASLGssAPIPassword != nil && *urlParameter.SASLGssAPIPassword != "" {
		o.SASL.GSSAPI.Password = *urlParameter.SASLGssAPIPassword
	}

	if urlParameter.SASLGssAPIRealm != nil && *urlParameter.SASLGssAPIRealm != "" {
		o.SASL.GSSAPI.Realm = *urlParameter.SASLGssAPIRealm
	}

	if urlParameter.SASLGssAPIDisablePafxfast != nil {
		o.SASL.GSSAPI.DisablePAFXFAST = *urlParameter.SASLGssAPIDisablePafxfast
	}

	if sinkConfig != nil && sinkConfig.KafkaConfig != nil {
		if sinkConfig.KafkaConfig.SASLOAuthClientID != nil {
			clientID := *sinkConfig.KafkaConfig.SASLOAuthClientID
			if clientID == "" {
				return errors.ErrKafkaInvalidConfig.GenWithStack("OAuth2 client ID cannot be empty")
			}
			o.SASL.OAuth2.ClientID = clientID
		}

		if sinkConfig.KafkaConfig.SASLOAuthClientSecret != nil {
			clientSecret := *sinkConfig.KafkaConfig.SASLOAuthClientSecret
			if clientSecret == "" {
				return errors.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 client secret cannot be empty")
			}

			// BASE64 decode the client secret
			decodedClientSecret, err := base64.StdEncoding.DecodeString(clientSecret)
			if err != nil {
				log.Error("OAuth2 client secret is not base64 encoded", zap.Error(err))
				return errors.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 client secret is not base64 encoded")
			}
			o.SASL.OAuth2.ClientSecret = string(decodedClientSecret)
		}

		if sinkConfig.KafkaConfig.SASLOAuthTokenURL != nil {
			tokenURL := *sinkConfig.KafkaConfig.SASLOAuthTokenURL
			if tokenURL == "" {
				return errors.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 token URL cannot be empty")
			}
			o.SASL.OAuth2.TokenURL = tokenURL
		}

		if o.SASL.OAuth2.IsEnable() {
			if o.SASL.SASLMechanism != security.OAuthMechanism {
				return errors.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 is only supported with SASL mechanism type OAUTHBEARER, but got %s",
					o.SASL.SASLMechanism)
			}

			if err := o.SASL.OAuth2.Validate(); err != nil {
				return errors.ErrKafkaInvalidConfig.Wrap(err)
			}
			o.SASL.OAuth2.SetDefault()
		}

		if sinkConfig.KafkaConfig.SASLOAuthScopes != nil {
			o.SASL.OAuth2.Scopes = sinkConfig.KafkaConfig.SASLOAuthScopes
		}

		if sinkConfig.KafkaConfig.SASLOAuthGrantType != nil {
			o.SASL.OAuth2.GrantType = *sinkConfig.KafkaConfig.SASLOAuthGrantType
		}

		if sinkConfig.KafkaConfig.SASLOAuthAudience != nil {
			o.SASL.OAuth2.Audience = *sinkConfig.KafkaConfig.SASLOAuthAudience
		}
	}

	return nil
}

// AutoCreateTopicConfig is used to create topic configuration.
type AutoCreateTopicConfig struct {
	AutoCreate        bool
	PartitionNum      int32
	ReplicationFactor int16
}

func (o *options) DeriveTopicConfig() *AutoCreateTopicConfig {
	return &AutoCreateTopicConfig{
		AutoCreate:        o.AutoCreate,
		PartitionNum:      o.PartitionNum,
		ReplicationFactor: o.ReplicationFactor,
	}
}

var (
	validClientID     = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)
	commonInvalidChar = regexp.MustCompile(`[\?:,"]`)
)

// NewKafkaClientID generates kafka client id
func NewKafkaClientID(captureAddr string,
	changefeedID common.ChangeFeedID,
	configuredClientID string,
) (clientID string, err error) {
	if configuredClientID != "" {
		clientID = configuredClientID
	} else {
		clientID = fmt.Sprintf("TiCDC_producer_%s_%s_%s",
			captureAddr, changefeedID.Keyspace(), changefeedID.Name())
		clientID = commonInvalidChar.ReplaceAllString(clientID, "_")
	}
	if !validClientID.MatchString(clientID) {
		return "", errors.ErrKafkaInvalidClientID.GenWithStackByArgs(clientID)
	}
	return
}

// adjustOptions adjusts options with Kafka runtime metadata.
// It overwrites MaxMessageBytes with the final producer message limit derived
// from the topic or broker configuration.
func adjustOptions(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	admin ClusterAdminClient,
	options *options,
	topic string,
) error {
	topics, err := admin.GetTopicsMeta([]string{topic}, true)
	if err != nil {
		return errors.Trace(err)
	}

	if err = validateRequiredAcks(ctx, admin, topics, topic, options); err != nil {
		return errors.Trace(err)
	}
	return adjustTopicOptions(ctx, changefeedID, admin, options, topic, topics)
}

func adjustTopicOptions(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	admin ClusterAdminClient,
	options *options,
	topic string,
	topics map[string]TopicDetail,
) error {
	info, exists := topics[topic]
	// once we have found the topic, no matter `auto-create-topic`,
	// make sure user input parameters are valid.
	var err error
	if exists {
		err = adjustExistingTopicOption(ctx, changefeedID, admin, options, topic, info)
	} else {
		adjustNewTopicOptions(admin, changefeedID, options, topic)
	}
	if err != nil {
		return err
	}

	options.MaxBatchedBytes = min(options.MaxBatchedBytes, options.MaxMessageBytes)
	return nil
}

func validateRequiredAcks(
	ctx context.Context,
	admin ClusterAdminClient,
	topics map[string]TopicDetail,
	topic string,
	options *options,
) error {
	// Only check replicationFactor >= minInsyncReplicas when producer's required acks is -1.
	// If we don't check it, the producer probably can not send message to the topic.
	// Because it will wait for the ack from all replicas. But we do not have enough replicas.
	if options.RequiredAcks != WaitForAll {
		return nil
	}
	return validateMinInsyncReplicas(ctx, admin, topics, topic, int(options.ReplicationFactor))
}

func adjustExistingTopicOption(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	admin ClusterAdminClient,
	options *options,
	topic string,
	info TopicDetail,
) error {
	maxMessageBytes, err := getTopicMaxMessageBytes(ctx, admin, info.Name)
	if err != nil {
		log.Warn("`max.message.bytes` not found from topic's configuration, use the option `MaxMessageBytes` as default",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.Int("maxMessageBytes", options.MaxMessageBytes), zap.Error(err))
		maxMessageBytes = options.MaxMessageBytes
	}
	options.MaxMessageBytes = maxMessageBytes

	// no need to create the topic,
	// but we would have to log user if they found enter wrong topic name later
	if options.AutoCreate {
		log.Warn("topic already exist, TiCDC will not create the topic",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.String("topic", topic), zap.Any("detail", info))
	}

	if err = options.setPartitionNum(changefeedID, info.NumPartitions); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func adjustNewTopicOptions(
	admin ClusterAdminClient,
	changefeedID common.ChangeFeedID,
	options *options,
	topic string,
) {
	// when create the topic, `max.message.bytes` is decided by the broker,
	// it would use broker's `message.max.bytes` to set topic's `max.message.bytes`.
	messageMaxBytes, err := getBrokerMaxMessageBytes(admin)
	if err != nil {
		log.Warn("`message.max.bytes` not found from broker's configuration, use the option `MaxMessageBytes` as default",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.Int("maxMessageBytes", options.MaxMessageBytes), zap.Error(err))
		messageMaxBytes = options.MaxMessageBytes
	}
	options.MaxMessageBytes = messageMaxBytes

	// topic not exists yet, and user does not specify the `partition-num` in the sink uri.
	if options.PartitionNum == 0 {
		options.PartitionNum = defaultPartitionNum
		log.Warn("partition-num is not set, use the default partition count",
			zap.String("namespace", changefeedID.Keyspace()), zap.String("changefeed", changefeedID.Name()),
			zap.String("topic", topic), zap.Int32("partitions", options.PartitionNum))
	}
}

func getTopicMaxMessageBytes(
	ctx context.Context,
	admin ClusterAdminClient,
	topic string,
) (int, error) {
	maxMessageBytesStr, err := getTopicConfig(
		ctx, admin, topic,
		TopicMaxMessageBytesConfigName,
		BrokerMessageMaxBytesConfigName,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	maxMessageBytes, err := strconv.Atoi(maxMessageBytesStr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return maxMessageBytes, nil
}

func getBrokerMaxMessageBytes(admin ClusterAdminClient) (int, error) {
	maxMessageBytesStr, err := admin.GetBrokerConfig(BrokerMessageMaxBytesConfigName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	maxMessageBytes, err := strconv.Atoi(maxMessageBytesStr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return maxMessageBytes, nil
}

func validateMinInsyncReplicas(
	ctx context.Context,
	admin ClusterAdminClient,
	topics map[string]TopicDetail,
	topic string,
	replicationFactor int,
) error {
	minInsyncReplicasConfigGetter := func() (string, bool, error) {
		info, exists := topics[topic]
		if exists {
			minInsyncReplicasStr, err := getTopicConfig(
				ctx, admin, info.Name,
				MinInsyncReplicasConfigName,
				MinInsyncReplicasConfigName)
			if err != nil {
				return "", true, err
			}
			return minInsyncReplicasStr, true, nil
		}

		minInsyncReplicasStr, err := admin.GetBrokerConfig(MinInsyncReplicasConfigName)
		if err != nil {
			return "", false, err
		}

		return minInsyncReplicasStr, false, nil
	}

	minInsyncReplicasStr, exists, err := minInsyncReplicasConfigGetter()
	if err != nil {
		// 'min.insync.replica' is invisible to us in Confluent Cloud Kafka.
		if errors.ErrKafkaConfigNotFound.Equal(err) {
			log.Warn("TiCDC cannot find `min.insync.replicas` from broker's configuration, " +
				"please make sure that the replication factor is greater than or equal " +
				"to the minimum number of in-sync replicas" +
				"if you want to use `required-acks` = -1." +
				"Otherwise, TiCDC will not be able to send messages to the topic.")
		}
		log.Warn("TiCDC meets error when get `min.insync.replicas` from broker's configuration, assume the config is valid")
		return nil
	}
	minInsyncReplicas, err := strconv.Atoi(minInsyncReplicasStr)
	if err != nil {
		return err
	}

	configFrom := "topic"
	if !exists {
		configFrom = "broker"
	}

	if replicationFactor < minInsyncReplicas {
		msg := fmt.Sprintf("`replication-factor` cannot be smaller than the `%s` of %s",
			MinInsyncReplicasConfigName, configFrom)
		log.Error(msg, zap.Int("replication-factor", replicationFactor),
			zap.Int("min.insync.replicas", minInsyncReplicas))
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"TiCDC Kafka sink's `request.required.acks` defaults to -1, "+
				"TiCDC cannot deliver messages when the `replication-factor` %d "+
				"is smaller than the `min.insync.replicas` %d of %s",
			replicationFactor, minInsyncReplicas, configFrom,
		)
	}

	return nil
}

// getTopicConfig gets topic config by name.
// If the topic does not have this configuration,
// we will try to get it from the broker's configuration.
// NOTICE: The configuration names of topic and broker may be different for the same configuration.
func getTopicConfig(
	ctx context.Context,
	admin ClusterAdminClient,
	topicName string,
	topicConfigName string,
	brokerConfigName string,
) (string, error) {
	if c, err := admin.GetTopicConfig(topicName, topicConfigName); err == nil {
		return c, nil
	}

	log.Info("kafka sink cannot find the configuration from topic, try to get it from broker",
		zap.String("topic", topicName), zap.String("config", topicConfigName))
	return admin.GetBrokerConfig(brokerConfigName)
}
