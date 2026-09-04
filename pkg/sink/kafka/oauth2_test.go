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
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestOAuthCA(t *testing.T) {
	server := newTLSTokenServer(t)
	tokenSource, err := newOAuthTokenSource(t.Context(), newOAuthConfig(server.URL, writeServerCA(t, server)))
	require.NoError(t, err)
	token, err := tokenSource.Token()
	require.NoError(t, err)
	require.Equal(t, "access-token", token.AccessToken)
}

func TestOAuthCAErrors(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		caPath := filepath.Join(t.TempDir(), "missing-ca.pem")
		_, err := newOAuthTokenSource(t.Context(), newOAuthConfig("https://example.com/token", caPath))
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
		require.ErrorContains(t, err, caPath)
	})

	t.Run("invalid PEM", func(t *testing.T) {
		caPath := filepath.Join(t.TempDir(), "invalid-ca.pem")
		require.NoError(t, os.WriteFile(caPath, []byte("not a certificate"), 0o600))
		_, err := newOAuthTokenSource(t.Context(), newOAuthConfig("https://example.com/token", caPath))
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
		require.ErrorContains(t, err, "does not contain a valid certificate")
	})

	t.Run("mismatched certificate", func(t *testing.T) {
		server := newTLSTokenServer(t)
		unrelatedCA, err := security.NewCA()
		require.NoError(t, err)
		caPath := filepath.Join(t.TempDir(), "unrelated-ca.pem")
		require.NoError(t, os.WriteFile(caPath, unrelatedCA.CAPEM, 0o600))
		tokenSource, err := newOAuthTokenSource(t.Context(), newOAuthConfig(server.URL, caPath))
		require.NoError(t, err)
		_, err = tokenSource.Token()
		require.ErrorContains(t, err, "certificate signed by unknown authority")
	})
}

func TestOAuthContextClient(t *testing.T) {
	server := newTLSTokenServer(t)
	ctx := context.WithValue(t.Context(), oauth2.HTTPClient, server.Client())
	tokenSource, err := newOAuthTokenSource(ctx, newOAuthConfig(server.URL, ""))
	require.NoError(t, err)
	token, err := tokenSource.Token()
	require.NoError(t, err)
	require.Equal(t, "access-token", token.AccessToken)
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

func newOAuthConfig(tokenURL, caPath string) oauth2Config {
	return oauth2Config{
		clientID:     "client-id",
		clientSecret: "client-secret",
		tokenURL:     tokenURL,
		caPath:       caPath,
	}
}
