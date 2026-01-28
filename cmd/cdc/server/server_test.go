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
	"encoding/json"
	"testing"

	tiflowConfig "github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRunTiFlowServerPopulatesSecurityConfig(t *testing.T) {
	// This test verifies that TLS credentials from CLI flags are properly
	// transferred to serverConfig.Security before being passed to tiflow.
	// See https://github.com/pingcap/ticdc/issues/3718

	o := newOptions()
	o.caPath = "/path/to/ca.crt"
	o.certPath = "/path/to/server.crt"
	o.keyPath = "/path/to/server.key"
	o.allowedCertCN = "cn1,cn2"

	// Verify that before calling getCredential, serverConfig.Security is empty
	require.Empty(t, o.serverConfig.Security.CAPath)
	require.Empty(t, o.serverConfig.Security.CertPath)
	require.Empty(t, o.serverConfig.Security.KeyPath)

	// Simulate what runTiFlowServer does: populate Security from CLI flags
	o.serverConfig.Security = o.getCredential()

	// Verify Security is now populated
	require.Equal(t, "/path/to/ca.crt", o.serverConfig.Security.CAPath)
	require.Equal(t, "/path/to/server.crt", o.serverConfig.Security.CertPath)
	require.Equal(t, "/path/to/server.key", o.serverConfig.Security.KeyPath)
	require.Equal(t, []string{"cn1", "cn2"}, o.serverConfig.Security.CertAllowedCN)

	// Verify that JSON marshaling preserves the Security config
	cfgData, err := json.Marshal(o.serverConfig)
	require.NoError(t, err)

	var oldCfg tiflowConfig.ServerConfig
	err = json.Unmarshal(cfgData, &oldCfg)
	require.NoError(t, err)

	// This is the critical assertion: tiflow's ServerConfig.Security
	// should have the TLS credentials after unmarshaling
	require.Equal(t, "/path/to/ca.crt", oldCfg.Security.CAPath)
	require.Equal(t, "/path/to/server.crt", oldCfg.Security.CertPath)
	require.Equal(t, "/path/to/server.key", oldCfg.Security.KeyPath)
	require.Equal(t, []string{"cn1", "cn2"}, oldCfg.Security.CertAllowedCN)
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
