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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestNewTokenProviderRejectsInvalidTokenURL(t *testing.T) {
	t.Parallel()

	options := &options{
		SASL: &security.SASL{
			OAuth2: security.OAuth2{
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				TokenURL:     "http://test.com/Segment%%2815197306101420000%29",
				Scopes:       []string{"scope1", "scope2"},
				GrantType:    "client_credentials",
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
		SASL: &security.SASL{
			OAuth2: security.OAuth2{
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				TokenURL:     server.URL + "/oauth2/token",
				Scopes:       []string{"scope1", "scope2"},
				GrantType:    "custom_grant",
				Audience:     "test-audience",
			},
		},
	}

	provider, err := newTokenProvider(t.Context(), options)
	require.NoError(t, err)
	_, err = provider.Token()
	require.NoError(t, err)

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
		SASL: &security.SASL{
			OAuth2: security.OAuth2{
				ClientID:     "client-id",
				ClientSecret: "client-secret",
				TokenURL:     server.URL,
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
