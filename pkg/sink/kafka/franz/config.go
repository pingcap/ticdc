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

package franz

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

	// NoResponse requests no broker acknowledgement. A send completes after the
	// request is written. Broker-side failures are not reported, so messages can be lost.
	NoResponse = int16(0)
	// WaitForLocal requests acknowledgement from the partition leader. A send completes
	// after the leader writes the message locally. An acknowledged message can be lost
	// if the leader fails before follower replication.
	WaitForLocal = int16(1)
	// WaitForAll requests acknowledgement from all in-sync replicas. A send completes
	// after the replication requirement is met. It is the default and provides the
	// strongest durability, at the cost of higher latency or failed sends when too few
	// replicas are in sync.
	WaitForAll = int16(-1)
)

type Config struct {
	BrokerEndpoints []string
	ClientID        string
	MaxMessageBytes int
	MaxRetry        int
	Compression     string
	RequiredAcks    int16
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	TLSConfig       *tls.Config
	SASL            *SASLConfig
}

type SASLConfig struct {
	Mechanism string
	User      string
	Password  string
	GSSAPI    GSSAPIConfig
	OAuth2    OAuth2Config
}

type GSSAPIConfig struct {
	AuthType           int
	KeyTabPath         string
	KerberosConfigPath string
	ServiceName        string
	Username           string
	Password           string
	Realm              string
	DisablePAFXFAST    bool
}

type OAuth2Config struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
	GrantType    string
	Audience     string
}

func (c Config) requestTimeout() time.Duration { return max(c.ReadTimeout, c.WriteTimeout) }

func newClientOptions(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	role string,
	cfg Config,
	hook *metricsHook,
) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.WithContext(ctx),
		kgo.SeedBrokers(cfg.BrokerEndpoints...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.RequestTimeoutOverhead(cfg.requestTimeout()),
		kgo.WithLogger(newLogger(changefeedID, role)),
	}
	if hook != nil {
		opts = append(opts, kgo.WithHooks(hook))
	}

	if cfg.TLSConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig))
	}

	if cfg.SASL != nil && cfg.SASL.Mechanism != "" {
		mechanism, err := buildSASLMechanism(ctx, *cfg.SASL)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func buildSASLMechanism(ctx context.Context, cfg SASLConfig) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.Mechanism) {
	case "PLAIN":
		return plain.Auth{User: cfg.User, Pass: cfg.Password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.User, Pass: cfg.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.User, Pass: cfg.Password}.AsSha512Mechanism(), nil
	case "OAUTHBEARER":
		tokenSource, err := newOAuthTokenSource(ctx, cfg.OAuth2)
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
	case "GSSAPI":
		return buildGSSAPIMechanism(cfg.GSSAPI)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", cfg.Mechanism)
	}
}

func newOAuthTokenSource(ctx context.Context, cfg OAuth2Config) (oauth2.TokenSource, error) {
	endpointParams := url.Values{}
	if cfg.GrantType != "" {
		endpointParams.Set("grant_type", cfg.GrantType)
	}
	if cfg.Audience != "" {
		endpointParams.Set("audience", cfg.Audience)
	}
	tokenURL, err := url.Parse(cfg.TokenURL)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	config := &clientcredentials.Config{
		ClientID:       cfg.ClientID,
		ClientSecret:   cfg.ClientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         cfg.Scopes,
	}
	return config.TokenSource(ctx), nil
}

func producerOptions(cfg Config) ([]kgo.Opt, error) {
	if cfg.MaxMessageBytes > maxProducerBatchBytes {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"max-message-bytes %d exceeds franz-go limit %d",
			cfg.MaxMessageBytes,
			maxProducerBatchBytes,
		)
	}

	// Use 64 MiB as the default budget, but never make it smaller than the configured message limit.
	// Keep franz-go's 10,000-record default as a second bound.
	maxBufferedBytes := max(defaultMaxBufferedBytes, cfg.MaxMessageBytes)
	maxBatchBytes := max(minProducerBatchBytes, cfg.MaxMessageBytes)
	maxBrokerWriteBytes := max(defaultBrokerWriteBytes, maxBatchBytes)

	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(requiredAcks(cfg.RequiredAcks)),
		// Retried requests may create duplicates because broker-side producer ID deduplication is disabled.
		kgo.DisableIdempotentWrite(),
		// More than one in-flight request can reorder records when an earlier request is retried.
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(cfg.MaxRetry),
		kgo.UnknownTopicRetries(cfg.MaxRetry),
		kgo.MaxBufferedBytes(maxBufferedBytes),
		kgo.ProducerBatchMaxBytes(int32(maxBatchBytes)),
		kgo.BrokerMaxWriteBytes(int32(maxBrokerWriteBytes)),
		kgo.ProduceRequestTimeout(cfg.requestTimeout()),
		kgo.ProducerLinger(0),
		compressionOption(cfg.Compression),
	}, nil
}

func requiredAcks(required int16) kgo.Acks {
	switch required {
	case WaitForAll:
		return kgo.AllISRAcks()
	case WaitForLocal:
		return kgo.LeaderAck()
	case NoResponse:
		return kgo.NoAck()
	default:
		log.Warn("unsupported required acks", zap.Int16("requiredAcks", required))
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
