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

// franz-go counts a record from Produce acceptance until its delivery callback
// returns, including metadata lookup, batching, sending, Broker response, and
// retries. A record larger than the byte limit fails immediately; otherwise,
// either limit blocks later Produce calls. Their ratio is 1 KiB per record, so
// bytes govern larger records while count bounds smaller record objects.
const (
	producerMaxBufferedBytes   = 64 << 20
	producerMaxBufferedRecords = 1 << 16
)

// producerMaxRequestBytes matches franz-go's default BrokerMaxWriteBytes and Kafka's default socket.request.max.bytes.
const producerMaxRequestBytes = 100 << 20

// Admin and producer clients share connection options. Producer delivery and
// resource limits stay separate so they cannot affect admin operations.
func clientOptions(ctx context.Context, o *options) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		// franz-go does not expose an independent socket read timeout. This value
		// sets the socket write deadline and is added to each request-specific
		// Broker processing timeout to form the socket read deadline.
		kgo.RequestTimeoutOverhead(o.WriteTimeout),
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

func producerOptions(o *options) []kgo.Opt {
	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(requiredAcks(o.RequiredAcks)),
		// Retried requests may create duplicates because broker-side producer ID deduplication is disabled.
		kgo.DisableIdempotentWrite(),
		// More than one in-flight request can reorder records when an earlier request is retried.
		kgo.MaxProduceRequestsInflightPerBroker(1),
		// The default of five retries allows six Produce attempts. franz-go's
		// default jittered backoff adds about 6.2s to 9.3s across five retries.
		kgo.RecordRetries(o.MaxRetry),
		kgo.UnknownTopicRetries(o.MaxRetry),
		// Limit each client to 64 MiB of buffered payload. The in-flight limit
		// applies per broker and does not bound records queued for other brokers,
		// metadata, or retries, so the producer needs a separate byte limit.
		// 64 MiB leaves room above TiCDC's default 10 MiB message limit while
		// bounding buffered payload memory.
		kgo.MaxBufferedBytes(producerMaxBufferedBytes),
		kgo.MaxBufferedRecords(producerMaxBufferedRecords),
		// A record batch must fit in the 100 MiB Produce request limit.
		kgo.ProducerBatchMaxBytes(int32(min(o.MaxMessageBytes, producerMaxRequestBytes))),
		// This value limits how long the Broker may process a Produce request;
		// read-timeout is therefore not an exact socket read deadline. Together
		// with RequestTimeoutOverhead above, the socket write deadline is
		// write-timeout, the Broker processing timeout is read-timeout, and the
		// socket read deadline is read-timeout plus write-timeout.
		// With the default 10s timeout, the Broker may process a Produce request
		// for 10s, the socket write deadline is 10s, and the socket read deadline
		// is 20s. Across six attempts, consecutive timeouts take about 66s-69s
		// when the Broker returns on its processing deadline, 126s-129s when it
		// never replies, or 186s-189s if every write and read reaches its deadline.
		// Buffering, metadata lookup, connection setup, and Broker throttling are
		// not included; the caller context is the end-to-end bound.
		// A Broker processing timeout returns REQUEST_TIMED_OUT, which franz-go
		// retries. The original record may already be stored, so retries may create
		// duplicates while idempotent writes are disabled. Exhausting the retry
		// budget fails the record and reports the error through its callback.
		kgo.ProduceRequestTimeout(o.ReadTimeout),
		kgo.ProducerLinger(0),
		compressionOption(o.Compression),
	}
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
		return buildOAuthMechanism(ctx, cfg.oauth2)
	case gssapiMechanism:
		return buildGSSAPIMechanism(cfg.gssapi)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", cfg.mechanism)
	}
}

func buildOAuthMechanism(ctx context.Context, cfg oauth2Config) (sasl.Mechanism, error) {
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
	if httpClient != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	}
	// One token source shares cached credentials across broker connections and refreshes them on expiry.
	tokenSource := config.TokenSource(ctx)
	return oauth.Oauth(func(context.Context) (oauth.Auth, error) {
		token, err := tokenSource.Token()
		if err != nil {
			return oauth.Auth{}, errors.WrapError(errors.ErrNewKafkaSink, err)
		}
		return oauth.Auth{Token: token.AccessToken}, nil
	}), nil
}

func newProducerClient(
	ctx context.Context, changefeedID common.ChangeFeedID, role string, clientOpts []kgo.Opt, producerOpts []kgo.Opt,
) (*kgo.Client, error) {
	opts := make([]kgo.Opt, 0, len(clientOpts)+len(producerOpts)+3)
	opts = append(opts, clientOpts...)
	opts = append(opts,
		kgo.WithContext(ctx),
		kgo.WithLogger(newClientLogger(changefeedID, role)),
		kgo.WithHooks(newMetricsHook(changefeedID)))
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
