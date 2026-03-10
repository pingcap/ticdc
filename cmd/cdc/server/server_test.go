// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTiFlowServerOptionsPropagatesTLSFlags(t *testing.T) {
	// Scenario: TiCDC runs in old architecture mode and delegates to tiflow's
	// server command, but reuses TiCDC's cobra.Command (flags are bound to TiCDC's
	// options, not tiflow's). If we don't copy TLS flags to tiflowServer.Options,
	// tiflow will see TLS flags as visited but with empty values, clear the
	// security config, and fail when PD endpoints are https.
	o := newOptions()
	o.caPath = "/path/to/ca.crt"
	o.certPath = "/path/to/server.crt"
	o.keyPath = "/path/to/server.key"
	o.allowedCertCN = "cn1,cn2"
	o.pdEndpoints = []string{"https://127.0.0.1:2379"}

	oldOptions, err := buildTiFlowServerOptions(o)
	require.NoError(t, err)

	// Ensure tiflow options carry TLS flags, so tiflow can rebuild credentials
	// regardless of cobra flag binding.
	require.Equal(t, "/path/to/ca.crt", oldOptions.CaPath)
	require.Equal(t, "/path/to/server.crt", oldOptions.CertPath)
	require.Equal(t, "/path/to/server.key", oldOptions.KeyPath)
	require.Equal(t, "cn1,cn2", oldOptions.AllowedCertCN)

	// Ensure the converted tiflow ServerConfig also contains the TLS credential,
	// which is used for logging and downstream config consumers.
	require.Equal(t, "/path/to/ca.crt", oldOptions.ServerConfig.Security.CAPath)
	require.Equal(t, "/path/to/server.crt", oldOptions.ServerConfig.Security.CertPath)
	require.Equal(t, "/path/to/server.key", oldOptions.ServerConfig.Security.KeyPath)
	require.Equal(t, []string{"cn1", "cn2"}, oldOptions.ServerConfig.Security.CertAllowedCN)

	require.Equal(t, strings.Join(o.pdEndpoints, ","), oldOptions.ServerPdAddr)
}

func TestGetCredential(t *testing.T) {
	testCases := []struct {
		name           string
		caPath         string
		certPath       string
		keyPath        string
		allowedCertCN  string
		expectedCAPath string
		expectedCert   string
		expectedKey    string
		expectedCertCN []string
	}{
		{
			name:           "all fields populated",
			caPath:         "/ca/path",
			certPath:       "/cert/path",
			keyPath:        "/key/path",
			allowedCertCN:  "test-cn",
			expectedCAPath: "/ca/path",
			expectedCert:   "/cert/path",
			expectedKey:    "/key/path",
			expectedCertCN: []string{"test-cn"},
		},
		{
			name:           "multiple CNs",
			allowedCertCN:  "cn1,cn2,cn3",
			expectedCertCN: []string{"cn1", "cn2", "cn3"},
		},
		{
			name:           "empty CN",
			allowedCertCN:  "",
			expectedCertCN: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			o := newOptions()
			o.caPath = tc.caPath
			o.certPath = tc.certPath
			o.keyPath = tc.keyPath
			o.allowedCertCN = tc.allowedCertCN

			cred := o.getCredential()

			require.Equal(t, tc.expectedCAPath, cred.CAPath)
			require.Equal(t, tc.expectedCert, cred.CertPath)
			require.Equal(t, tc.expectedKey, cred.KeyPath)
			require.Equal(t, tc.expectedCertCN, cred.CertAllowedCN)
		})
	}
}

func TestNewOptionsDefaultSecurity(t *testing.T) {
	o := newOptions()

	// serverConfig should be initialized with default values
	require.NotNil(t, o.serverConfig)
	require.NotNil(t, o.serverConfig.Security)

	// Security should have empty paths by default
	require.Empty(t, o.serverConfig.Security.CAPath)
	require.Empty(t, o.serverConfig.Security.CertPath)
	require.Empty(t, o.serverConfig.Security.KeyPath)
}
