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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
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

func newOptions(
	ctx context.Context,
	o *options,
	hook kgo.Hook,
) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.WithContext(ctx),
		kgo.SeedBrokers(o.BrokerEndpoints...),
		kgo.ClientID(o.ClientID),
		kgo.DialTimeout(o.DialTimeout),
		kgo.RequestTimeoutOverhead(o.requestTimeout()),
	}
	if hook != nil {
		opts = append(opts, kgo.WithHooks(hook))
	}

	if o.IsAssignedVersion {
		versions := kversion.FromString(o.Version)
		if versions == nil {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack("invalid kafka version %s", o.Version)
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

func newTLSConfig(o *options) (*tls.Config, error) {
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
	}

	tlsConfig.InsecureSkipVerify = o.InsecureSkipVerify
	return tlsConfig, nil
}

func buildSaslMechanism(ctx context.Context, o *options) (sasl.Mechanism, error) {
	switch o.sasl.mechanism {
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

func newOauthTokenSource(ctx context.Context, o *options) (oauth2.TokenSource, error) {
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
	o *options,
) []kgo.Opt {
	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(newRequiredAcks(o)),
		kgo.DisableIdempotentWrite(),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(o.MaxRetry),
		kgo.ProducerBatchMaxBytes(int32(o.MaxMessageBytes)),
		kgo.ProduceRequestTimeout(o.requestTimeout()),
		kgo.ProducerLinger(0),
		newCompressionOption(o),
	}
}

func newRequiredAcks(o *options) kgo.Acks {
	switch o.RequiredAcks {
	case WaitForAll:
		return kgo.AllISRAcks()
	case WaitForLocal:
		return kgo.LeaderAck()
	case NoResponse:
		return kgo.NoAck()
	default:
		log.Warn("unsupported required acks", zap.Int16("requiredAcks", int16(o.RequiredAcks)))
		return kgo.AllISRAcks()
	}
}

func newCompressionOption(o *options) kgo.Opt {
	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	var codec kgo.CompressionCodec
	switch compression {
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
		log.Warn("unsupported kafka compression algorithm", zap.String("compression", o.Compression))
		codec = kgo.NoCompression()
	}
	return kgo.ProducerBatchCompression(codec)
}
