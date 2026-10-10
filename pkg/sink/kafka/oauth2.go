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
	"crypto/x509"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func newOAuthTokenSource(ctx context.Context, cfg oauth2Config) (oauth2.TokenSource, error) {
	endpointParams := url.Values{}
	if cfg.grantType != "" {
		endpointParams.Set("grant_type", cfg.grantType)
	}
	if cfg.audience != "" {
		endpointParams.Set("audience", cfg.audience)
	}

	tokenURL, err := url.Parse(cfg.tokenURL)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, util.MaskSensitiveDataInURLError(err))
	}
	if cfg.caPath != "" {
		httpClient, err := oauthHTTPClient(cfg.caPath)
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	}

	config := clientcredentials.Config{
		ClientID:       cfg.clientID,
		ClientSecret:   cfg.clientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         cfg.scopes,
	}
	return &redactedOAuthTokenSource{TokenSource: config.TokenSource(ctx)}, nil
}

// OAuth2 servers can echo credentials in error bodies, descriptions and URLs.
// Keep the status and protocol error code for diagnostics without exposing them.
type redactedOAuthTokenSource struct {
	oauth2.TokenSource
}

func (s *redactedOAuthTokenSource) Token() (*oauth2.Token, error) {
	token, err := s.TokenSource.Token()
	if err == nil {
		return token, nil
	}
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		status := retrieveErr.Response.StatusCode
		err = &oauth2.RetrieveError{
			Response: &http.Response{
				StatusCode: status,
				Status:     strconv.Itoa(status) + " " + http.StatusText(status),
			},
			ErrorCode: retrieveErr.ErrorCode,
		}
	} else {
		err = util.MaskSensitiveDataInURLError(err)
	}
	return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
}

func oauthHTTPClient(caPath string) (*http.Client, error) {
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
	return &http.Client{Transport: transport}, nil
}
