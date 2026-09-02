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
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	// defaultMaxBufferedBytes bounds the producer's per-client byte buffer under normal configurations.
	defaultMaxBufferedBytes = 64 << 20
	// defaultBrokerWriteBytes matches Kafka's default socket.request.max.bytes limit.
	defaultBrokerWriteBytes = 100 << 20
	// minProducerBatchBytes and maxProducerBatchBytes are franz-go's accepted batch-size bounds.
	minProducerBatchBytes = 512
	maxProducerBatchBytes = 1 << 30
)

func requestTimeout(o *options) time.Duration { return max(o.ReadTimeout, o.WriteTimeout) }

func newClientOptions(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	role string,
	o *options,
	hook *metricsHook,
) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.WithContext(ctx),
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		kgo.RequestTimeoutOverhead(requestTimeout(o)),
		kgo.WithLogger(newClientLogger(changefeedID, role)),
	}
	if hook != nil {
		opts = append(opts, kgo.WithHooks(hook))
	}

	if o.EnableTLS {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}
		if o.Credential != nil && o.Credential.IsTLSEnabled() {
			var err error
			tlsConfig, err = o.Credential.ToTLSConfig()
			if err != nil {
				return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
			}
		}
		tlsConfig.InsecureSkipVerify = o.InsecureSkipVerify
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if o.sasl != nil && o.sasl.mechanism != "" {
		mechanism, err := buildSASLMechanism(ctx, o.sasl)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func buildSASLMechanism(ctx context.Context, cfg *saslConfig) (sasl.Mechanism, error) {
	switch cfg.mechanism {
	case plainMechanism:
		return plain.Auth{User: cfg.user, Pass: cfg.password}.AsMechanism(), nil
	case scram256Mechanism:
		return scram.Auth{User: cfg.user, Pass: cfg.password}.AsSha256Mechanism(), nil
	case scram512Mechanism:
		return scram.Auth{User: cfg.user, Pass: cfg.password}.AsSha512Mechanism(), nil
	case oauthMechanism:
		tokenSource, err := newOAuthTokenSource(ctx, cfg.oauth2)
		if err != nil {
			return nil, err
		}
		return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			token, err := tokenSource.Token()
			if err != nil {
				return oauth.Auth{}, errors.WrapError(errors.ErrNewKafkaSink, err)
			}
			return oauth.Auth{Token: token.AccessToken}, nil
		}), nil
	case gssapiMechanism:
		return buildGSSAPIMechanism(cfg.gssapi)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl mechanism %s", cfg.mechanism)
	}
}

func newOAuthTokenSource(ctx context.Context, cfg oauth2Config) (oauth2.TokenSource, error) {
	if cfg.caPath != "" {
		httpClient, err := oauthHTTPClient(cfg.caPath)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	}

	endpointParams := url.Values{}
	if cfg.grantType != "" {
		endpointParams.Set("grant_type", cfg.grantType)
	}
	if cfg.audience != "" {
		endpointParams.Set("audience", cfg.audience)
	}
	tokenURL, err := url.Parse(cfg.tokenURL)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	config := &clientcredentials.Config{
		ClientID:       cfg.clientID,
		ClientSecret:   cfg.clientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         cfg.scopes,
	}
	return config.TokenSource(ctx), nil
}

func newProducerClient(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	role string,
	o *options,
) (*kgo.Client, error) {
	opts, err := newClientOptions(ctx, changefeedID, role, o, newMetricsHook(changefeedID))
	if err != nil {
		return nil, err
	}

	producerOpts, err := producerOptions(o)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(append(opts, producerOpts...)...)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return client, nil
}

func producerOptions(o *options) ([]kgo.Opt, error) {
	if o.MaxMessageBytes > maxProducerBatchBytes {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"max-message-bytes %d exceeds franz-go limit %d",
			o.MaxMessageBytes,
			maxProducerBatchBytes,
		)
	}

	// Use 64 MiB as the default budget, but never make it smaller than the configured message limit.
	// Keep franz-go's 10,000-record default as a second bound.
	maxBufferedBytes := max(defaultMaxBufferedBytes, o.MaxMessageBytes)
	maxBatchBytes := max(minProducerBatchBytes, o.MaxMessageBytes)
	maxBrokerWriteBytes := max(defaultBrokerWriteBytes, maxBatchBytes)

	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(requiredAcks(o.RequiredAcks)),
		// Retried requests may create duplicates because broker-side producer ID deduplication is disabled.
		kgo.DisableIdempotentWrite(),
		// More than one in-flight request can reorder records when an earlier request is retried.
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(o.MaxRetry),
		kgo.UnknownTopicRetries(o.MaxRetry),
		kgo.MaxBufferedBytes(maxBufferedBytes),
		kgo.ProducerBatchMaxBytes(int32(maxBatchBytes)),
		kgo.BrokerMaxWriteBytes(int32(maxBrokerWriteBytes)),
		kgo.ProduceRequestTimeout(requestTimeout(o)),
		kgo.ProducerLinger(0),
		compressionOption(o.Compression),
	}, nil
}

func requiredAcks(required RequiredAcks) kgo.Acks {
	switch required {
	case WaitForAll:
		return kgo.AllISRAcks()
	case WaitForLocal:
		return kgo.LeaderAck()
	case NoResponse:
		return kgo.NoAck()
	default:
		log.Warn("unsupported required acks", zap.Int16("requiredAcks", int16(required)))
		return kgo.AllISRAcks()
	}
}

func compressionOption(compression string) kgo.Opt {
	var codec kgo.CompressionCodec
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "", "none":
		codec = kgo.NoCompression()
	case "gzip":
		codec = kgo.GzipCompression()
	case "snappy":
		codec = kgo.SnappyCompression()
	case "lz4":
		codec = kgo.Lz4Compression()
	case "zstd":
		codec = kgo.ZstdCompression()
	default:
		log.Warn("unsupported kafka compression algorithm", zap.String("compression", compression))
		codec = kgo.NoCompression()
	}
	return kgo.ProducerBatchCompression(codec)
}
