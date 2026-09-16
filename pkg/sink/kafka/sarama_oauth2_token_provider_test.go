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
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestNewTokenProviderRejectsInvalidTokenURL(t *testing.T) {
	t.Parallel()

	options := &options{
		sasl: &saslConfig{
			oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     "http://test.com/Segment%%2815197306101420000%29",
				scopes:       []string{"scope1", "scope2"},
				grantType:    "client_credentials",
			},
		},
	}

	_, err := newTokenProvider(t.Context(), options)
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	var escapeErr url.EscapeError
	require.ErrorAs(t, err, &escapeErr)
	require.ErrorContains(t, err, "invalid URL escape")
}

func TestTokenProviderRequestsToken(t *testing.T) {
	t.Parallel()

	type tokenRequest struct {
		method string
		path   string
		form   url.Values
		err    error
	}
	requestCh := make(chan tokenRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseForm()
		requestCh <- tokenRequest{
			method: r.Method,
			path:   r.URL.Path,
			form:   r.PostForm,
			err:    err,
		}

		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"access_token":"access-token","token_type":"bearer"}`); err != nil {
			t.Errorf("write token response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	options := &options{
		sasl: &saslConfig{
			oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     server.URL + "/oauth2/token",
				scopes:       []string{"scope1", "scope2"},
				grantType:    "custom_grant",
				audience:     "test-audience",
			},
		},
	}

	provider, err := newTokenProvider(t.Context(), options)
	require.NoError(t, err)
	token, err := provider.Token()
	require.NoError(t, err)
	require.Equal(t, "access-token", token.Token)

	request := <-requestCh
	require.NoError(t, request.err)
	require.Equal(t, http.MethodPost, request.method)
	require.Equal(t, "/oauth2/token", request.path)
	require.Equal(t, "custom_grant", request.form.Get("grant_type"))
	require.Equal(t, "test-audience", request.form.Get("audience"))
	require.Equal(t, "scope1 scope2", request.form.Get("scope"))
}

func TestTokenProviderPropagatesEndpointError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		if _, err := io.WriteString(w, `{"error":"invalid_client","error_description":"bad credentials"}`); err != nil {
			t.Errorf("write token error response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	options := &options{
		sasl: &saslConfig{
			oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     server.URL,
			},
		},
	}

	provider, err := newTokenProvider(t.Context(), options)
	require.NoError(t, err)
	_, err = provider.Token()
	var retrieveErr *oauth2.RetrieveError
	require.ErrorAs(t, err, &retrieveErr)
	require.Equal(t, http.StatusUnauthorized, retrieveErr.Response.StatusCode)
	require.Equal(t, "invalid_client", retrieveErr.ErrorCode)
	require.Equal(t, "bad credentials", retrieveErr.ErrorDescription)
}

func TestTokenProviderUsesOAuthCA(t *testing.T) {
	t.Parallel()

	server := newTLSTokenServer(t)
	caPath := writeServerCA(t, server)
	options := newOAuthOptions(server.URL, caPath)

	provider, err := newTokenProvider(t.Context(), options)
	require.NoError(t, err)
	token, err := provider.Token()
	require.NoError(t, err)
	require.Equal(t, "access-token", token.Token)
}

func TestTokenProviderRejectsInvalidOAuthCA(t *testing.T) {
	t.Parallel()

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		caPath := filepath.Join(t.TempDir(), "missing-ca.pem")
		_, err := newTokenProvider(t.Context(), newOAuthOptions("https://example.com/token", caPath))
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
		require.ErrorContains(t, err, caPath)
		require.ErrorContains(t, err, "no such file")
	})

	t.Run("invalid PEM", func(t *testing.T) {
		t.Parallel()

		caPath := filepath.Join(t.TempDir(), "invalid-ca.pem")
		require.NoError(t, os.WriteFile(caPath, []byte("not a certificate"), 0o600))
		_, err := newTokenProvider(t.Context(), newOAuthOptions("https://example.com/token", caPath))
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
		require.ErrorContains(t, err, caPath)
		require.ErrorContains(t, err, "does not contain a valid certificate")
	})
}

func TestTokenProviderRejectsMismatchedOAuthCA(t *testing.T) {
	t.Parallel()

	tokenServer := newTLSTokenServer(t)
	unrelatedCA, err := security.NewCA()
	require.NoError(t, err)
	caPath := filepath.Join(t.TempDir(), "unrelated-ca.pem")
	require.NoError(t, os.WriteFile(caPath, unrelatedCA.CAPEM, 0o600))
	options := newOAuthOptions(tokenServer.URL, caPath)

	provider, err := newTokenProvider(t.Context(), options)
	require.NoError(t, err)
	_, err = provider.Token()
	require.ErrorContains(t, err, "certificate signed by unknown authority")
}

func TestTokenProviderWithoutOAuthCAKeepsContextHTTPClient(t *testing.T) {
	t.Parallel()

	server := newTLSTokenServer(t)
	ctx := context.WithValue(t.Context(), oauth2.HTTPClient, server.Client())
	provider, err := newTokenProvider(ctx, newOAuthOptions(server.URL, ""))
	require.NoError(t, err)
	token, err := provider.Token()
	require.NoError(t, err)
	require.Equal(t, "access-token", token.Token)
}

func newTLSTokenServer(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if _, err := io.WriteString(w, `{"access_token":"access-token","token_type":"bearer"}`); err != nil {
			t.Errorf("write token response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func writeServerCA(t *testing.T, server *httptest.Server) string {
	t.Helper()

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	require.NotNil(t, caPEM)
	require.NoError(t, os.WriteFile(caPath, caPEM, 0o600))
	return caPath
}

func newOAuthOptions(tokenURL, caPath string) *options {
	return &options{
		sasl: &saslConfig{
			oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     tokenURL,
				caPath:       caPath,
			},
		},
	}
}
