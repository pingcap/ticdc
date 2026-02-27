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

package franz

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

type Options struct {
	BrokerEndpoints []string
	ClientID        string

	Version           string
	IsAssignedVersion bool

	MaxMessageBytes int
	Compression     string
	RequiredAcks    int16

	EnableTLS          bool
	Credential         *security.Credential
	InsecureSkipVerify bool
	SASL               *security.SASL

	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

const defaultRequestTimeout = 10 * time.Second

func maxTimeoutWithDefault(readTimeout, writeTimeout time.Duration) time.Duration {
	timeout := readTimeout
	if writeTimeout > timeout {
		timeout = writeTimeout
	}
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return timeout
}

func newOptions(
	ctx context.Context,
	o *Options,
	hook kgo.Hook,
) ([]kgo.Opt, error) {
	if o == nil {
		o = &Options{}
	}

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

	if o.SASL != nil && o.SASL.SASLMechanism != "" {
		mechanism, err := buildFranzSaslMechanism(ctx, o)
		if err != nil {
			return nil, errors.Trace(err)
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

func newTLSConfig(o *Options) (*tls.Config, error) {
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

func buildFranzSaslMechanism(ctx context.Context, o *Options) (sasl.Mechanism, error) {
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
	case security.GSSAPIMechanism:
		return buildGSSAPIMechanism(o.SASL.GSSAPI)
	default:
	}
	return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl mechanism %s", o.SASL.SASLMechanism)
}

func newOauthTokenSource(ctx context.Context, o *Options) (oauth2.TokenSource, error) {
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

func newProducerOptions(
	o *Options,
) []kgo.Opt {
	if o == nil {
		o = &Options{}
	}

	produceTimeout := o.ReadTimeout
	if produceTimeout < 100*time.Millisecond {
		produceTimeout = defaultRequestTimeout
	}

	return []kgo.Opt{
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(newRequiredAcks(o)),
		kgo.DisableIdempotentWrite(),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordRetries(5),
		kgo.ProducerBatchMaxBytes(int32(o.MaxMessageBytes)),
		kgo.ProduceRequestTimeout(produceTimeout),
		kgo.ProducerLinger(0),
		newCompressionOption(o),
	}
}

func newRequiredAcks(o *Options) kgo.Acks {
	if o == nil {
		return kgo.AllISRAcks()
	}

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

func newCompressionOption(o *Options) kgo.Opt {
	if o == nil {
		return kgo.ProducerBatchCompression(kgo.NoCompression())
	}

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
	}
	return kgo.ProducerBatchCompression(codec)
}
