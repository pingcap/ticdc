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

type clientOptions struct {
	BrokerEndpoints []string
	ClientID        string

	Version           string
	IsAssignedVersion bool

	MaxMessageBytes       int
	ProducerBatchMaxBytes int
	MaxRetry              int
	Compression           string
	RequiredAcks          int16

	EnableTLS          bool
	Credential         *security.Credential
	InsecureSkipVerify bool
	sasl               *saslConfig

	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

const (
	defaultRequestTimeout = 10 * time.Second
)

func maxTimeoutWithDefault(readTimeout, writeTimeout time.Duration) time.Duration {
	timeout := max(readTimeout, writeTimeout)
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return timeout
}

func newOptions(
	ctx context.Context,
	o *clientOptions,
	hook kgo.Hook,
) ([]kgo.Opt, error) {
	timeoutOverhead := maxTimeoutWithDefault(o.ReadTimeout, o.WriteTimeout)

	opts := []kgo.Opt{
		kgo.WithContext(ctx),
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		kgo.RequestTimeoutOverhead(timeoutOverhead),
	}
	if hook != nil {
		opts = append(opts, kgo.WithHooks(hook))
	}

	if o.IsAssignedVersion {
		versions := kversion.FromString(o.Version)
		if versions == nil {
			return nil, errors.ErrKafkaInvalidVersion.GenWithStack("invalid kafka version %s", o.Version)
		}
		opts = append(opts, kgo.MaxVersions(versions))
	}

	if o.EnableTLS {
		tlsConfig, err := newTLSConfig(o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if o.sasl != nil && o.sasl.mechanism != "" {
		mechanism, err := buildSaslMechanism(ctx, o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func newTLSConfig(o *clientOptions) (*tls.Config, error) {
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

func buildSaslMechanism(ctx context.Context, o *clientOptions) (sasl.Mechanism, error) {
	if o.sasl == nil {
		return nil, nil
	}

	switch saslMechanism(strings.ToUpper(string(o.sasl.mechanism))) {
	case plainMechanism:
		auth := plain.Auth{
			User: o.sasl.user,
			Pass: o.sasl.password,
		}
		return auth.AsMechanism(), nil
	case scram256Mechanism:
		auth := scram.Auth{
			User: o.sasl.user,
			Pass: o.sasl.password,
		}
		return auth.AsSha256Mechanism(), nil
	case scram512Mechanism:
		auth := scram.Auth{
			User: o.sasl.user,
			Pass: o.sasl.password,
		}
		return auth.AsSha512Mechanism(), nil
	case oauthMechanism:
		tokenSource, err := newOauthTokenSource(ctx, o)
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
	case gssapiMechanismName:
		return buildGSSAPIMechanism(o.sasl.gssapi)
	default:
	}
	return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", o.sasl.mechanism)
}

func newOauthTokenSource(ctx context.Context, o *clientOptions) (oauth2.TokenSource, error) {
	endpointParams := url.Values{}
	if o.sasl.oauth2.grantType != "" {
		endpointParams.Set("grant_type", o.sasl.oauth2.grantType)
	}
	if o.sasl.oauth2.audience != "" {
		endpointParams.Set("audience", o.sasl.oauth2.audience)
	}

	tokenURL, err := url.Parse(o.sasl.oauth2.tokenURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg := &clientcredentials.Config{
		ClientID:       o.sasl.oauth2.clientID,
		ClientSecret:   o.sasl.oauth2.clientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         o.sasl.oauth2.scopes,
	}
	return cfg.TokenSource(ctx), nil
}

func newProducerOptions(
	o *clientOptions,
) []kgo.Opt {
	produceTimeout := o.ReadTimeout
	if produceTimeout < 100*time.Millisecond {
		produceTimeout = defaultRequestTimeout
	}
	producerBatchMaxBytes := o.ProducerBatchMaxBytes
	if producerBatchMaxBytes <= 0 {
		producerBatchMaxBytes = o.MaxMessageBytes
	}

	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(newRequiredAcks(o)),
		kgo.DisableIdempotentWrite(),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(o.MaxRetry),
		kgo.ProducerBatchMaxBytes(int32(producerBatchMaxBytes)),
		kgo.ProduceRequestTimeout(produceTimeout),
		kgo.ProducerLinger(0),
		newCompressionOption(o),
	}
}

func newRequiredAcks(o *clientOptions) kgo.Acks {
	switch o.RequiredAcks {
	case -1:
		return kgo.AllISRAcks()
	case 1:
		return kgo.LeaderAck()
	case 0:
		return kgo.NoAck()
	default:
		log.Warn("unsupported required acks", zap.Int16("requiredAcks", o.RequiredAcks))
		return kgo.AllISRAcks()
	}
}

func newCompressionOption(o *clientOptions) kgo.Opt {
	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	var codec kgo.CompressionCodec
	switch compression {
	case "none":
		codec = kgo.NoCompression()
	case "gzip":
		codec = kgo.GzipCompression()
	case "snappy":
		codec = kgo.SnappyCompression()
	case "lz4":
		codec = kgo.Lz4Compression()
	case "zstd":
		codec = kgo.ZstdCompression()
	case "":
		codec = kgo.NoCompression()
	default:
		log.Warn("unsupported compression algorithm", zap.String("compression", o.Compression))
		codec = kgo.NoCompression()
	}
	if codec != kgo.NoCompression() {
		log.Info("Kafka producer uses " + compression + " compression algorithm")
	}
	return kgo.ProducerBatchCompression(codec)
}
