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
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/kafka/franz"
	"go.uber.org/zap"
)

type franzFactoryAdapter struct {
	inner *franz.Factory
}

// NewFranzFactory constructs a franz-go Kafka client factory.
func NewFranzFactory(ctx context.Context, o *options, changefeedID common.ChangeFeedID) (Factory, error) {
	config, err := newFranzConfig(o)
	if err != nil {
		return nil, err
	}

	innerAdmin, err := franz.NewAdmin(ctx, changefeedID, config)
	if err != nil {
		return nil, err
	}

	admin := &franzAdminAdapter{inner: innerAdmin}
	defer admin.Close()

	if err := adjustOptions(changefeedID, admin, o, o.Topic); err != nil {
		return nil, err
	}

	config, err = newFranzConfig(o)
	if err != nil {
		return nil, err
	}

	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	if compression == "" {
		compression = "none"
	}

	log.Info("kafka sink configuration resolved",
		zap.String("namespace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
		zap.String("client", KafkaClientFranz),
		zap.String("topic", o.Topic),
		zap.Int32("partitionNum", o.PartitionNum),
		zap.Int("maxMessageBytes", o.MaxMessageBytes),
		zap.Int("maxBatchedBytes", o.MaxBatchedBytes),
		zap.String("compression", compression),
		zap.Int16("requiredAcks", int16(o.RequiredAcks)),
		zap.Int("maxRetry", o.MaxRetry),
		zap.Duration("dialTimeout", o.DialTimeout),
		zap.Duration("readTimeout", o.ReadTimeout),
		zap.Duration("writeTimeout", o.WriteTimeout))

	return &franzFactoryAdapter{inner: franz.NewFactory(config, changefeedID)}, nil
}

func (f *franzFactoryAdapter) AdminClient(ctx context.Context) (AdminClient, error) {
	admin, err := f.inner.Admin(ctx)
	if err != nil {
		return nil, err
	}
	return &franzAdminAdapter{inner: admin}, nil
}

func (f *franzFactoryAdapter) SyncProducer(ctx context.Context) (SyncProducer, error) {
	return f.inner.SyncProducer(ctx)
}

func (f *franzFactoryAdapter) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	return f.inner.AsyncProducer(ctx)
}

func (f *franzFactoryAdapter) MetricsCollector(AdminClient) MetricsCollector {
	return franzMetricsCollector{}
}

func (f *franzFactoryAdapter) CleanupMetrics() { f.inner.CleanupMetrics() }

type franzMetricsCollector struct{}

func (franzMetricsCollector) Run(ctx context.Context) { <-ctx.Done() }

type franzAdminAdapter struct{ inner *franz.Admin }

func (a *franzAdminAdapter) GetAllBrokers() []Broker {
	inner := a.inner.GetAllBrokers()
	brokers := make([]Broker, 0, len(inner))
	for _, broker := range inner {
		brokers = append(brokers, Broker{ID: broker.ID})
	}
	return brokers
}

func (a *franzAdminAdapter) GetBrokerConfig(name string) (string, bool, error) {
	return a.inner.GetBrokerConfig(name)
}

func (a *franzAdminAdapter) GetTopicConfig(topic, name string) (string, bool, error) {
	return a.inner.GetTopicConfig(topic, name)
}

func (a *franzAdminAdapter) GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]TopicDetail, error) {
	inner, err := a.inner.GetTopicsMeta(topics, ignoreTopicError)
	if err != nil {
		return nil, err
	}

	details := make(map[string]TopicDetail, len(inner))
	for topic, detail := range inner {
		details[topic] = TopicDetail{
			Name:              detail.Name,
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
		}
	}

	return details, nil
}

func (a *franzAdminAdapter) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	return a.inner.GetTopicsPartitionsNum(topics)
}

func (a *franzAdminAdapter) CreateTopic(detail *TopicDetail) error {
	return a.inner.CreateTopic(&franz.TopicDetail{
		Name:              detail.Name,
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	})
}

func (a *franzAdminAdapter) Close() { a.inner.Close() }

func newFranzConfig(o *options) (franz.Config, error) {
	config := franz.Config{
		BrokerEndpoints: append([]string(nil), o.BrokerEndpoints...),
		ClientID:        o.ClientID,
		MaxMessageBytes: o.MaxMessageBytes,
		MaxRetry:        o.MaxRetry,
		Compression:     o.Compression,
		RequiredAcks:    int16(o.RequiredAcks),
		DialTimeout:     o.DialTimeout,
		ReadTimeout:     o.ReadTimeout,
		WriteTimeout:    o.WriteTimeout,
	}

	if o.EnableTLS {
		config.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}

		if o.Credential != nil && o.Credential.IsTLSEnabled() {
			tlsConfig, err := o.Credential.ToTLSConfig()
			if err != nil {
				return franz.Config{}, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
			}

			config.TLSConfig = tlsConfig
		}

		config.TLSConfig.InsecureSkipVerify = o.InsecureSkipVerify
	}

	if o.sasl != nil && o.sasl.mechanism != "" {
		config.SASL = &franz.SASLConfig{
			Mechanism: string(o.sasl.mechanism),
			User:      o.sasl.user,
			Password:  o.sasl.password,
			GSSAPI: franz.GSSAPIConfig{
				AuthType:           int(o.sasl.gssapi.authType),
				KeyTabPath:         o.sasl.gssapi.keyTabPath,
				KerberosConfigPath: o.sasl.gssapi.kerberosConfigPath,
				ServiceName:        o.sasl.gssapi.serviceName,
				Username:           o.sasl.gssapi.username,
				Password:           o.sasl.gssapi.password,
				Realm:              o.sasl.gssapi.realm,
				DisablePAFXFAST:    o.sasl.gssapi.disablePAFXFAST,
			},
			OAuth2: franz.OAuth2Config{
				ClientID:     o.sasl.oauth2.clientID,
				ClientSecret: o.sasl.oauth2.clientSecret,
				TokenURL:     o.sasl.oauth2.tokenURL,
				Scopes:       append([]string(nil), o.sasl.oauth2.scopes...),
				GrantType:    o.sasl.oauth2.grantType,
				Audience:     o.sasl.oauth2.audience,
			},
		}
	}

	return config, nil
}
