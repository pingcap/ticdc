// Copyright 2023 PingCAP, Inc.
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
	"crypto/x509"
	"net/http"
	"net/url"
	"os"

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// tokenProvider is a user-defined callback for generating
// access tokens for SASL/OAUTHBEARER auth.
type tokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*tokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.
// Token returns an access token. The implementation should ensure token
// reuse so that multiple calls at connect time do not create multiple
// tokens. The implementation should also periodically refresh the token in
// order to guarantee that each call returns an unexpired token.  This
// method should not block indefinitely--a timeout error should be returned
// after a short period of inactivity so that the broker connection logic
// can log debugging information and retry.
func (t *tokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}

func newTokenProvider(ctx context.Context, o *options) (sarama.AccessTokenProvider, error) {
	// grant_type is by default going to be set to 'client_credentials' by the
	// client credentials library as defined by the spec, however non-compliant
	// auth server implementations may want a custom type
	endpointParams := url.Values{}
	if o.sasl.oauth2.grantType != "" {
		endpointParams.Set("grant_type", o.sasl.oauth2.grantType)
	}

	// audience is an optional parameter that can be used to specify the
	// intended audience of the token.
	if o.sasl.oauth2.audience != "" {
		endpointParams.Set("audience", o.sasl.oauth2.audience)
	}

	tokenURL, err := url.Parse(o.sasl.oauth2.tokenURL)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	if o.sasl.oauth2.caPath != "" {
		ctx, err = contextWithOAuthCA(ctx, o.sasl.oauth2.caPath)
		if err != nil {
			return nil, err
		}
	}

	cfg := clientcredentials.Config{
		ClientID:       o.sasl.oauth2.clientID,
		ClientSecret:   o.sasl.oauth2.clientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         o.sasl.oauth2.scopes,
	}
	return &tokenProvider{
		tokenSource: cfg.TokenSource(ctx),
	}, nil
}

func contextWithOAuthCA(ctx context.Context, caPath string) (context.Context, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"OAuth2 CA file %q does not contain a valid certificate", caPath)
	}

	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"cannot configure OAuth2 CA file %q with HTTP transport type %T",
			caPath, http.DefaultTransport)
	}
	transport := defaultTransport.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.RootCAs = rootCAs
	return context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Transport: transport}), nil
}
