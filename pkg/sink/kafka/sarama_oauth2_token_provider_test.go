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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
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
				tokenURL:     "http://user:password-sentinel@test.com/Segment%%2815197306101420000%29",
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
	require.NotContains(t, err.Error(), "password-sentinel")
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

	for _, tc := range []struct {
		name          string
		errorCode     string
		wantErrorCode string
	}{
		{name: "invalid request", errorCode: "invalid_request", wantErrorCode: "invalid_request"},
		{name: "invalid client", errorCode: "invalid_client", wantErrorCode: "invalid_client"},
		{name: "invalid grant", errorCode: "invalid_grant", wantErrorCode: "invalid_grant"},
		{name: "unauthorized client", errorCode: "unauthorized_client", wantErrorCode: "unauthorized_client"},
		{name: "unsupported grant type", errorCode: "unsupported_grant_type", wantErrorCode: "unsupported_grant_type"},
		{name: "invalid scope", errorCode: "invalid_scope", wantErrorCode: "invalid_scope"},
		{name: "echoed credential", errorCode: "client-secret-sentinel"},
		{name: "credential appended to code", errorCode: "invalid_client client-secret-sentinel"},
		{name: "missing code"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				if err := json.NewEncoder(w).Encode(map[string]string{
					"error":             tc.errorCode,
					"error_description": "bad credentials client-secret-sentinel",
					"error_uri":         "https://example.com/client-secret-sentinel",
				}); err != nil {
					t.Errorf("write token error response: %v", err)
				}
			}))
			t.Cleanup(server.Close)

			options := &options{
				sasl: &saslConfig{
					oauth2: oauth2Config{
						clientID:     "client-id",
						clientSecret: "client-secret-sentinel",
						tokenURL:     server.URL,
					},
				},
			}

			provider, err := newTokenProvider(t.Context(), options)
			require.NoError(t, err)
			_, err = provider.Token()
			require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
			var retrieveErr *oauth2.RetrieveError
			require.ErrorAs(t, err, &retrieveErr)
			require.Equal(t, http.StatusUnauthorized, retrieveErr.Response.StatusCode)
			require.Equal(t, tc.wantErrorCode, retrieveErr.ErrorCode)
			require.Empty(t, retrieveErr.ErrorDescription)
			require.Empty(t, retrieveErr.ErrorURI)
			require.Empty(t, retrieveErr.Body)
			require.NotContains(t, err.Error(), "client-secret-sentinel")
		})
	}
}
