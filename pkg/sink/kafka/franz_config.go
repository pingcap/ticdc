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
	"net/http"
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

// producerMaxBufferedBytes bounds each producer client's buffered payload.
// Produce blocks when the buffer is full and resumes when records complete
// or its context is canceled. A larger single record fails with MessageTooLarge.
const producerMaxBufferedBytes = 64 << 20

// Kafka defaults socket.request.max.bytes to 100 MiB. Keeping franz-go at the
// same limit prevents a Topic's batch configuration from increasing request memory.
const franzDefaultMaxRequestBytes = 100 << 20

func requestTimeout(o *options) time.Duration { return max(o.ReadTimeout, o.WriteTimeout) }

// Admin and producer clients share connection options. Producer delivery and
// resource limits stay separate so they cannot affect admin operations.
func clientOptions(o *options) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		kgo.RequestTimeoutOverhead(requestTimeout(o)),
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
		mechanism, err := buildSASLMechanism(o.sasl)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

func producerOptions(o *options) []kgo.Opt {
	maxMessageBytes := int32(min(o.MaxMessageBytes, franzDefaultMaxRequestBytes))
	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(requiredAcks(o.RequiredAcks)),
		// Retried requests may create duplicates because broker-side producer ID deduplication is disabled.
		kgo.DisableIdempotentWrite(),
		// More than one in-flight request can reorder records when an earlier request is retried.
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(o.MaxRetry),
		kgo.UnknownTopicRetries(o.MaxRetry),
		// Limit each client to 64 MiB of buffered payload. The in-flight limit
		// applies per broker and does not bound records queued for other brokers,
		// metadata, or retries, so the producer needs a separate byte limit.
		// 64 MiB leaves room above TiCDC's default 10 MiB message limit while
		// bounding the memory hidden by the 10,000-record default.
		kgo.MaxBufferedBytes(producerMaxBufferedBytes),
		// ProducerBatchMaxBytes is franz-go's name for Kafka's max.message.bytes limit.
		kgo.ProducerBatchMaxBytes(maxMessageBytes),
		kgo.ProduceRequestTimeout(requestTimeout(o)),
		kgo.ProducerLinger(0),
		compressionOption(o.Compression),
	}
}

func buildSASLMechanism(cfg *saslConfig) (sasl.Mechanism, error) {
	switch cfg.mechanism {
	case plainMechanism:
		return plain.Auth{User: cfg.user, Pass: cfg.password}.AsMechanism(), nil
	case scram256Mechanism:
		return scram.Auth{User: cfg.user, Pass: cfg.password}.AsSha256Mechanism(), nil
	case scram512Mechanism:
		return scram.Auth{User: cfg.user, Pass: cfg.password}.AsSha512Mechanism(), nil
	case oauthMechanism:
		return buildOAuthMechanism(cfg.oauth2)
	case gssapiMechanism:
		return buildGSSAPIMechanism(cfg.gssapi)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", cfg.mechanism)
	}
}

func buildOAuthMechanism(cfg oauth2Config) (sasl.Mechanism, error) {
	var httpClient *http.Client
	if cfg.caPath != "" {
		var err error
		httpClient, err = oauthHTTPClient(cfg.caPath)
		if err != nil {
			return nil, err
		}
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
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		if httpClient != nil {
			ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
		}
		token, err := config.TokenSource(ctx).Token()
		if err != nil {
			return oauth.Auth{}, errors.WrapError(errors.ErrNewKafkaSink, err)
		}
		return oauth.Auth{Token: token.AccessToken}, nil
	}), nil
}

func newProducerClient(ctx context.Context, changefeedID common.ChangeFeedID, role string, clientOpts []kgo.Opt, producerOpts []kgo.Opt) (*kgo.Client, error) {
	opts := make([]kgo.Opt, 0, len(clientOpts)+len(producerOpts)+3)
	opts = append(opts, clientOpts...)
	opts = append(opts, kgo.WithContext(ctx), kgo.WithLogger(newClientLogger(changefeedID, role)), kgo.WithHooks(newMetricsHook(changefeedID)))
	opts = append(opts, producerOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}
	return client, nil
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
