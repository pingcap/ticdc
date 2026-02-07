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
	"crypto/tls"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type franzFactory struct {
	changefeedID common.ChangeFeedID
	option       *options
}

// NewFranzFactory constructs a Factory with franz-go implementation.
func NewFranzFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	admin, err := newFranzAdminClient(ctx, changefeedID, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer admin.Close()

	if err = adjustOptions(ctx, admin, o, o.Topic); err != nil {
		return nil, errors.Trace(err)
	}

	return &franzFactory{
		changefeedID: changefeedID,
		option:       o,
	}, nil
}

func (f *franzFactory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	admin, err := newFranzAdminClient(ctx, f.changefeedID, f.option)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return admin, nil
}

func (f *franzFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	producer, err := newFranzSyncProducer(ctx, f.changefeedID, f.option)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	producer, err := newFranzAsyncProducer(ctx, f.changefeedID, f.option)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaNewProducer, err)
	}
	return producer, nil
}

func (f *franzFactory) MetricsCollector(_ ClusterAdminClient) MetricsCollector {
	return &noopMetricsCollector{}
}

type noopMetricsCollector struct{}

func (m *noopMetricsCollector) Run(_ context.Context) {}

func buildFranzBaseOptions(
	ctx context.Context,
	o *options,
) ([]kgo.Opt, error) {
	timeoutOverhead := o.ReadTimeout
	if o.WriteTimeout > timeoutOverhead {
		timeoutOverhead = o.WriteTimeout
	}
	if timeoutOverhead <= 0 {
		timeoutOverhead = 10 * time.Second
	}

	opts := []kgo.Opt{
		kgo.WithContext(ctx),
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		kgo.RequestTimeoutOverhead(timeoutOverhead),
	}

	if o.IsAssignedVersion {
		versions := kversion.FromString(o.Version)
		if versions == nil {
			return nil, errors.ErrKafkaInvalidVersion.GenWithStack("invalid kafka version %s", o.Version)
		}
		opts = append(opts, kgo.MaxVersions(versions))
	}

	if o.EnableTLS {
		tlsConfig, err := buildFranzTLSConfig(o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if o.SASL != nil && o.SASL.SASLMechanism != "" {
		mechanism, err := buildFranzSaslMechanism(ctx, o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func buildFranzTLSConfig(o *options) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}

	if o.Credential != nil && o.Credential.IsTLSEnabled() {
		credentialTlsConfig, err := o.Credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
		tlsConfig = credentialTlsConfig
		if tlsConfig.MinVersion == 0 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
		if len(tlsConfig.NextProtos) == 0 {
			tlsConfig.NextProtos = []string{"h2", "http/1.1"}
		}
	}

	tlsConfig.InsecureSkipVerify = o.InsecureSkipVerify
	return tlsConfig, nil
}

func buildFranzSaslMechanism(ctx context.Context, o *options) (sasl.Mechanism, error) {
	if o.SASL == nil {
		return nil, nil
	}

	switch security.SASLMechanism(strings.ToUpper(string(o.SASL.SASLMechanism))) {
	case security.PlainMechanism:
		auth := plain.Auth{
			User: o.SASL.SASLUser,
			Pass: o.SASL.SASLPassword,
		}
		return auth.AsMechanism(), nil
	case security.SCRAM256Mechanism:
		auth := scram.Auth{
			User: o.SASL.SASLUser,
			Pass: o.SASL.SASLPassword,
		}
		return auth.AsSha256Mechanism(), nil
	case security.SCRAM512Mechanism:
		auth := scram.Auth{
			User: o.SASL.SASLUser,
			Pass: o.SASL.SASLPassword,
		}
		return auth.AsSha512Mechanism(), nil
	case security.OAuthMechanism:
		tokenSource, err := buildFranzOauthTokenSource(ctx, o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			token, err := tokenSource.Token()
			if err != nil {
				return oauth.Auth{}, errors.Trace(err)
			}
			return oauth.Auth{Token: token.AccessToken}, nil
		}), nil
	case security.GSSAPIMechanism:
		return buildFranzGSSAPIMechanism(o.SASL.GSSAPI)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", o.SASL.SASLMechanism)
	}
}

func buildFranzOauthTokenSource(ctx context.Context, o *options) (oauth2.TokenSource, error) {
	endpointParams := url.Values{}
	if o.SASL.OAuth2.GrantType != "" {
		endpointParams.Set("grant_type", o.SASL.OAuth2.GrantType)
	}
	if o.SASL.OAuth2.Audience != "" {
		endpointParams.Set("audience", o.SASL.OAuth2.Audience)
	}

	tokenURL, err := url.Parse(o.SASL.OAuth2.TokenURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg := &clientcredentials.Config{
		ClientID:       o.SASL.OAuth2.ClientID,
		ClientSecret:   o.SASL.OAuth2.ClientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         o.SASL.OAuth2.Scopes,
	}
	return cfg.TokenSource(ctx), nil
}

func buildFranzProducerOptions(
	o *options,
	recordRetries int,
) ([]kgo.Opt, error) {
	var acks kgo.Acks
	switch o.RequiredAcks {
	case WaitForAll:
		acks = kgo.AllISRAcks()
	case WaitForLocal:
		acks = kgo.LeaderAck()
	case NoResponse:
		acks = kgo.NoAck()
	default:
		acks = kgo.AllISRAcks()
		log.Warn("unknown required acks, use all isr acks", zap.Int16("requiredAcks", int16(o.RequiredAcks)))
	}

	compressionOpt, err := buildFranzCompressionOption(o)
	if err != nil {
		return nil, errors.Trace(err)
	}

	produceTimeout := o.ReadTimeout
	if produceTimeout < 100*time.Millisecond {
		produceTimeout = 10 * time.Second
	}

	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(acks),
		kgo.DisableIdempotentWrite(),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(recordRetries),
		kgo.ProducerBatchMaxBytes(int32(o.MaxMessageBytes)),
		kgo.ProduceRequestTimeout(produceTimeout),
		kgo.ProducerLinger(0),
		compressionOpt,
	}, nil
}

func buildFranzCompressionOption(o *options) (kgo.Opt, error) {
	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	switch compression {
	case "none":
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	case "":
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	default:
		log.Warn("unsupported compression algorithm", zap.String("compression", o.Compression))
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	}
}
