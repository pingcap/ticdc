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

package mysql

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
)

func TestParseSPIFFEIDPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pattern   string
		matches   []string
		notMatch  []string
		wantError bool
	}{
		{
			name:     "exact",
			pattern:  "spiffe://example.org/v1/prod/123/ns/tidb/server",
			matches:  []string{"spiffe://example.org/v1/prod/123/ns/tidb/server"},
			notMatch: []string{"spiffe://example.org/v1/prod/123/ns/tidb/other"},
		},
		{
			name:    "single final wildcard segment",
			pattern: "spiffe://example.org/v1/prod/123/ns/tidb/*",
			matches: []string{
				"spiffe://example.org/v1/prod/123/ns/tidb/server",
				"spiffe://example.org/v1/prod/123/ns/tidb/abc-123",
			},
			notMatch: []string{
				"spiffe://other.example/v1/prod/123/ns/tidb/server",
				"spiffe://example.org/v1/prod/123/ns/tidb",
				"spiffe://example.org/v1/prod/123/ns/tidb/server/extra",
			},
		},
		{name: "empty", pattern: "", wantError: true},
		{name: "wrong scheme", pattern: "https://example.org/path", wantError: true},
		{name: "uppercase scheme", pattern: "SPIFFE://example.org/path", wantError: true},
		{name: "uppercase trust domain", pattern: "spiffe://EXAMPLE.org/path", wantError: true},
		{name: "trust domain port", pattern: "spiffe://example.org:443/path", wantError: true},
		{name: "percent encoding", pattern: "spiffe://example.org/%2Fpath", wantError: true},
		{name: "wildcard in middle", pattern: "spiffe://example.org/v1/*/tidb", wantError: true},
		{name: "partial wildcard", pattern: "spiffe://example.org/v1/tidb/foo*", wantError: true},
		{name: "multiple wildcards", pattern: "spiffe://example.org/v1/*/*", wantError: true},
		{name: "trailing slash", pattern: "spiffe://example.org/v1/tidb/", wantError: true},
		{name: "query", pattern: "spiffe://example.org/v1/tidb?x=y", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pattern, err := parseSPIFFEIDPattern(tt.pattern)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			for _, raw := range tt.matches {
				require.True(t, pattern.matches(spiffeid.RequireFromString(raw)), raw)
			}
			for _, raw := range tt.notMatch {
				require.False(t, pattern.matches(spiffeid.RequireFromString(raw)), raw)
			}
		})
	}
}

func TestConfigureSPIFFETLSRejectsInvalidAndMixedInputs(t *testing.T) {
	t.Parallel()

	validClient := "spiffe://example.org/v1/prod/source/ns/ticdc/*"
	validServer := "spiffe://example.org/v1/prod/destination/ns/tidb/*"
	tests := []struct {
		name      string
		values    url.Values
		configure func(*Config)
	}{
		{
			name: "missing client pattern",
			values: url.Values{
				spiffeServerIDPatternKey: {validServer},
			},
		},
		{
			name: "missing server pattern",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
			},
		},
		{
			name: "duplicate client pattern",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient, validClient},
				spiffeServerIDPatternKey: {validServer},
			},
		},
		{
			name: "duplicate server pattern",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer, validServer},
			},
		},
		{
			name: "malformed client pattern",
			values: url.Values{
				spiffeClientIDPatternKey: {"spiffe://example.org/v1/*/ticdc"},
				spiffeServerIDPatternKey: {validServer},
			},
		},
		{
			name: "query CA",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer},
				"ssl-ca":                 {"/ca.pem"},
			},
		},
		{
			name: "query certificate",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer},
				"ssl-cert":               {"/cert.pem"},
			},
		},
		{
			name: "query key",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer},
				"ssl-key":                {"/key.pem"},
			},
		},
		{
			name: "driver TLS query",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer},
				"tls":                    {"skip-verify"},
			},
		},
		{
			name: "configured certificate files",
			values: url.Values{
				spiffeClientIDPatternKey: {validClient},
				spiffeServerIDPatternKey: {validServer},
			},
			configure: func(cfg *Config) {
				cfg.SSLCa = "/ca.pem"
				cfg.SSLCert = "/cert.pem"
				cfg.SSLKey = "/key.pem"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := New()
			if tt.configure != nil {
				tt.configure(cfg)
			}
			err := cfg.configureTLS(
				context.Background(), tt.values, common.NewChangefeedID4Test("default", "spiffe-test"))
			require.Error(t, err)
		})
	}
}

func TestMatchingX509SVIDPickerFailsClosed(t *testing.T) {
	t.Parallel()

	ca, caKey := newTestCA(t, "client-ca")
	pattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")
	matchingOne := newTestSVID(t, ca, caKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/one", 2)
	matchingTwo := newTestSVID(t, ca, caKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/two", 3)
	other := newTestSVID(t, ca, caKey,
		"spiffe://example.org/v1/prod/source/ns/other/one", 4)
	picker := matchingX509SVIDPicker(pattern)

	require.Nil(t, picker(nil))
	require.Nil(t, picker([]*x509svid.SVID{other}))
	require.Same(t, matchingOne, picker([]*x509svid.SVID{other, matchingOne}))
	require.Nil(t, picker([]*x509svid.SVID{matchingOne, matchingTwo}))
}

func TestValidateMatchingX509SVIDRejectsInvalidValidity(t *testing.T) {
	t.Parallel()

	ca, caKey := newTestCA(t, "client-ca")
	pattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")
	valid := newTestSVID(t, ca, caKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 5)
	require.ErrorContains(t, validateMatchingX509SVID(nil, pattern), "nil X509-SVID")
	require.ErrorContains(t, validateMatchingX509SVID(&x509svid.SVID{}, pattern), "without certificates")

	expiredCert := *valid.Certificates[0]
	expiredCert.NotAfter = time.Now().Add(-time.Minute)
	expired := *valid
	expired.Certificates = []*x509.Certificate{&expiredCert}
	require.ErrorContains(t, validateMatchingX509SVID(&expired, pattern), "expired")

	futureCert := *valid.Certificates[0]
	futureCert.NotBefore = time.Now().Add(time.Minute)
	future := *valid
	future.Certificates = []*x509.Certificate{&futureCert}
	require.ErrorContains(t, validateMatchingX509SVID(&future, pattern), "not valid yet")
}

func TestSPIFFEClientTLSHandshakeAndRotation(t *testing.T) {
	t.Parallel()

	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, serverCAKey := newTestCA(t, "server-ca")
	rotatedServerCA, rotatedServerCAKey := newTestCA(t, "rotated-server-ca")
	unrelatedCA, _ := newTestCA(t, "unrelated-ca")
	clientOne := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/one", 11)
	clientTwo := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/two", 12)
	invalidClient := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/other/invalid", 13)
	server := newTestSVID(t, serverCA, serverCAKey,
		"spiffe://example.org/v1/prod/destination/ns/tidb/server", 21)
	rotatedServer := newTestSVID(t, rotatedServerCA, rotatedServerCAKey,
		"spiffe://example.org/v1/prod/destination/ns/tidb/server", 22)

	clientSource := &testSPIFFESource{
		svid: clientOne,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			spiffeid.RequireTrustDomainFromString("example.org"): x509bundle.FromX509Authorities(
				spiffeid.RequireTrustDomainFromString("example.org"), []*x509.Certificate{unrelatedCA, serverCA}),
		},
	}
	serverSource := &testSPIFFESource{
		svid: server,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			spiffeid.RequireTrustDomainFromString("example.org"): x509bundle.FromX509Authorities(
				spiffeid.RequireTrustDomainFromString("example.org"), []*x509.Certificate{clientCA}),
		},
	}
	clientPattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")
	serverPattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/destination/ns/tidb/*")
	clientTLS, err := newSPIFFEClientTLSConfig(clientSource, clientPattern, serverPattern)
	require.NoError(t, err)
	serverTLS := tlsconfig.MTLSServerConfig(
		serverSource, serverSource, authorizeSPIFFEIDPattern(clientPattern))

	firstPeer := handshakePeerSerial(t, clientTLS, serverTLS)
	require.Equal(t, clientOne.Certificates[0].SerialNumber, firstPeer)

	clientSource.setSVID(clientTwo)
	secondPeer := handshakePeerSerial(t, clientTLS, serverTLS)
	require.Equal(t, clientTwo.Certificates[0].SerialNumber, secondPeer)
	require.NotEqual(t, firstPeer, secondPeer)

	clientSource.setSVID(invalidClient)
	require.Error(t, handshake(t, clientTLS, serverTLS), "an invalid rotated client SVID must fail closed")

	clientSource.setSVID(clientTwo)
	serverSource.setSVID(rotatedServer)
	require.Error(t, handshake(t, clientTLS, serverTLS),
		"a server using the new issuer must fail before the bundle rotates")
	clientSource.setBundle(
		spiffeid.RequireTrustDomainFromString("example.org"),
		x509bundle.FromX509Authorities(
			spiffeid.RequireTrustDomainFromString("example.org"), []*x509.Certificate{rotatedServerCA}),
	)
	require.NoError(t, handshake(t, clientTLS, serverTLS),
		"a server using the new issuer must succeed after the bundle rotates")
	serverSource.setSVID(server)
	require.Error(t, handshake(t, clientTLS, serverTLS),
		"a server using the old issuer must fail after the bundle rotates")
}

func TestSPIFFEClientTLSRejectsUnexpectedServer(t *testing.T) {
	t.Parallel()

	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, serverCAKey := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/one", 31)
	clientPattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")
	expectedServerPattern := requireSPIFFEPattern(t,
		"spiffe://example.org/v1/prod/destination/ns/tidb/*")

	for _, tt := range []struct {
		name     string
		serverID string
	}{
		{
			name:     "wrong path",
			serverID: "spiffe://example.org/v1/prod/destination/ns/other/server",
		},
		{
			name:     "wrong trust domain",
			serverID: "spiffe://other.example/v1/prod/destination/ns/tidb/server",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := newTestSVID(t, serverCA, serverCAKey, tt.serverID, 32)
			serverTD := server.ID.TrustDomain()
			clientSource := &testSPIFFESource{
				svid: client,
				bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
					serverTD: x509bundle.FromX509Authorities(serverTD, []*x509.Certificate{serverCA}),
					expectedServerPattern.trustDomain: x509bundle.FromX509Authorities(
						expectedServerPattern.trustDomain, []*x509.Certificate{serverCA}),
				},
			}
			serverSource := &testSPIFFESource{
				svid: server,
				bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
					client.ID.TrustDomain(): x509bundle.FromX509Authorities(
						client.ID.TrustDomain(), []*x509.Certificate{clientCA}),
				},
			}
			clientTLS, err := newSPIFFEClientTLSConfig(
				clientSource, clientPattern, expectedServerPattern)
			require.NoError(t, err)
			serverTLS := tlsconfig.MTLSServerConfig(
				serverSource, serverSource, authorizeSPIFFEIDPattern(clientPattern))

			require.Error(t, handshake(t, clientTLS, serverTLS))
		})
	}
}

func TestSPIFFEClientTLSRejectsUntrustedServerCertificate(t *testing.T) {
	t.Parallel()

	clientCA, clientCAKey := newTestCA(t, "client-ca")
	trustedServerCA, _ := newTestCA(t, "trusted-server-ca")
	untrustedServerCA, untrustedServerCAKey := newTestCA(t, "untrusted-server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 33)
	server := newTestSVID(t, untrustedServerCA, untrustedServerCAKey,
		"spiffe://example.org/v1/prod/destination/ns/tidb/server", 34)
	td := spiffeid.RequireTrustDomainFromString("example.org")
	clientSource := &testSPIFFESource{
		svid: client,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{trustedServerCA}),
		},
	}
	serverSource := &testSPIFFESource{
		svid: server,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{clientCA}),
		},
	}
	clientPattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")
	serverPattern := requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/destination/ns/tidb/*")
	clientTLS, err := newSPIFFEClientTLSConfig(clientSource, clientPattern, serverPattern)
	require.NoError(t, err)
	serverTLS := tlsconfig.MTLSServerConfig(
		serverSource, serverSource, authorizeSPIFFEIDPattern(clientPattern))

	require.Error(t, handshake(t, clientTLS, serverTLS))
}

func TestSPIFFETLSRequiresWorkloadAPIEndpoint(t *testing.T) {
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "")
	cfg := New()
	err := cfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-no-endpoint"))
	require.ErrorContains(t, err, "SPIFFE_ENDPOINT_SOCKET")
	require.Empty(t, cfg.TLS)
}

func TestSPIFFETLSWorkloadAPIConstructorFailure(t *testing.T) {
	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	newSPIFFEX509Source = func(
		context.Context,
		string,
		func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		return nil, fmt.Errorf("workload API unavailable")
	}

	cfg := New()
	err := cfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-unavailable"))
	require.ErrorContains(t, err, "workload API unavailable")
	require.Empty(t, cfg.TLS)
	require.Nil(t, cfg.tlsResource)
}

func TestSPIFFETLSInitializationHasBoundedDeadline(t *testing.T) {
	originalFactory := newSPIFFEX509Source
	originalTimeout := spiffeTLSInitTimeout
	t.Cleanup(func() {
		newSPIFFEX509Source = originalFactory
		spiffeTLSInitTimeout = originalTimeout
	})
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	spiffeTLSInitTimeout = 10 * time.Millisecond
	newSPIFFEX509Source = func(
		ctx context.Context,
		_ string,
		_ func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), spiffeTLSInitTimeout)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	started := time.Now()
	cfg := New()
	err := cfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-init-deadline"))
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	require.Less(t, time.Since(started), time.Second)
	require.Empty(t, cfg.TLS)
}

func TestSPIFFETLSMissingSVIDClosesSource(t *testing.T) {
	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	td := spiffeid.RequireTrustDomainFromString("example.org")
	ca, _ := newTestCA(t, "server-ca")
	source := &testSPIFFESource{
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{ca}),
		},
	}
	newSPIFFEX509Source = func(
		context.Context,
		string,
		func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		return source, nil
	}

	cfg := New()
	err := cfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-missing-svid"))
	require.ErrorContains(t, err, "no X509-SVID")
	require.Equal(t, 1, source.closeCount())
	require.Empty(t, cfg.TLS)
}

func TestSPIFFETLSResourceLifecycle(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, _ := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 41)
	td := spiffeid.RequireTrustDomainFromString("example.org")

	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")

	var sources []*testSPIFFESource
	newSPIFFEX509Source = func(
		_ context.Context,
		endpoint string,
		picker func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		require.Equal(t, "unix:///spire-agent-socket/spire-agent.sock", endpoint)
		source := &testSPIFFESource{
			svid: picker([]*x509svid.SVID{client}),
			bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
				td: x509bundle.FromX509Authorities(td, []*x509.Certificate{serverCA}),
			},
		}
		sources = append(sources, source)
		return source, nil
	}

	cfg := New()
	err := cfg.configureTLS(context.Background(), url.Values{
		spiffeClientIDPatternKey: {"spiffe://example.org/v1/prod/source/ns/ticdc/*"},
		spiffeServerIDPatternKey: {"spiffe://example.org/v1/prod/destination/ns/tidb/*"},
	}, common.NewChangefeedID4Test("default", "spiffe-lifecycle"))
	require.NoError(t, err)
	require.Len(t, sources, 1)
	registryName := strings.TrimPrefix(cfg.TLS, "?tls=")
	cfg.sinkURI = &url.URL{
		Scheme: "mysql",
		Host:   "tidb.example:4000",
		User:   url.User("replicator"),
	}
	dsn, err := GenBasicDSN(cfg)
	require.NoError(t, err)
	require.Equal(t, registryName, dsn.TLSConfig)
	require.NotNil(t, dsn.TLS)
	require.False(t, dsn.AllowFallbackToPlaintext)
	require.Equal(t, "replicator", dsn.User)
	require.Empty(t, dsn.Passwd)
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.NoError(t, err)
	overlappingCfg := New()
	err = overlappingCfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-lifecycle"))
	require.NoError(t, err)
	require.Len(t, sources, 2)
	overlappingRegistryName := strings.TrimPrefix(overlappingCfg.TLS, "?tls=")
	require.NotEqual(t, registryName, overlappingRegistryName)

	require.NoError(t, cfg.CloseTLS())
	require.NoError(t, cfg.CloseTLS())
	require.Equal(t, 1, sources[0].closeCount())
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.ErrorContains(t, err, "unknown config name")
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: overlappingRegistryName})
	require.NoError(t, err, "closing one config must not deregister an overlapping config")
	require.NoError(t, overlappingCfg.CloseTLS())
	require.Equal(t, 1, sources[1].closeCount())

	cleanupCfg, err := cfg.NewCleanupConfig(context.Background())
	require.NoError(t, err)
	require.Len(t, sources, 3)
	require.NoError(t, cleanupCfg.CloseTLS())
	require.Equal(t, 1, sources[2].closeCount())
}

func TestSPIFFETLSInitializationFailureClosesSource(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 51)

	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	source := &testSPIFFESource{svid: client, bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{}}
	newSPIFFEX509Source = func(
		_ context.Context,
		_ string,
		picker func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		source.svid = picker([]*x509svid.SVID{client})
		return source, nil
	}

	cfg := New()
	err := cfg.configureTLS(context.Background(), url.Values{
		spiffeClientIDPatternKey: {"spiffe://example.org/v1/prod/source/ns/ticdc/*"},
		spiffeServerIDPatternKey: {"spiffe://example.org/v1/prod/destination/ns/tidb/*"},
	}, common.NewChangefeedID4Test("default", "spiffe-failure"))
	require.Error(t, err)
	require.Equal(t, 1, source.closeCount())
	require.Empty(t, cfg.TLS, "SPIFFE failures must not fall back to plaintext")
}

func TestSPIFFETLSCleanupHonorsContextDeadline(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, _ := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 62)
	td := spiffeid.RequireTrustDomainFromString("example.org")

	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	source := &testSPIFFESource{
		svid: client,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{serverCA}),
		},
	}
	newSPIFFEX509Source = func(
		context.Context,
		string,
		func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		return source, nil
	}

	cfg := New()
	err := cfg.configureTLS(context.Background(), validSPIFFETLSValues(),
		common.NewChangefeedID4Test("default", "spiffe-cleanup-deadline"))
	require.NoError(t, err)
	require.NoError(t, cfg.CloseTLS())

	newSPIFFEX509Source = func(
		ctx context.Context,
		_ string,
		_ func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	cleanupCfg, err := cfg.NewCleanupConfig(ctx)
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, cleanupCfg)
	require.Less(t, time.Since(started), time.Second)
}

func TestApplyFailureClosesSPIFFETLS(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, _ := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 61)
	td := spiffeid.RequireTrustDomainFromString("example.org")

	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	source := &testSPIFFESource{
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{serverCA}),
		},
	}
	newSPIFFEX509Source = func(
		_ context.Context,
		_ string,
		picker func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		source.svid = picker([]*x509svid.SVID{client})
		return source, nil
	}

	values := validSPIFFETLSValues()
	values.Set("safe-mode", "not-a-boolean")
	sinkURI := &url.URL{
		Scheme:   "mysql",
		Host:     "127.0.0.1:4000",
		User:     url.User("replicator"),
		RawQuery: values.Encode(),
	}
	cfg := New()
	err := cfg.apply(context.Background(), sinkURI,
		common.NewChangefeedID4Test("default", "spiffe-apply-failure"),
		&config.ChangefeedConfig{
			TimeZone: "UTC",
			SinkConfig: &config.SinkConfig{
				TiDBSourceID: 1,
			},
		})
	require.Error(t, err)
	require.Equal(t, 1, source.closeCount())
}

func TestMySQLInitializationFailureClosesSPIFFETLS(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, _ := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/ticdc/client", 62)
	td := spiffeid.RequireTrustDomainFromString("example.org")
	source := &testSPIFFESource{
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{serverCA}),
		},
	}
	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///tmp/spiffe-unit-test.sock")
	newSPIFFEX509Source = func(
		_ context.Context,
		_ string,
		picker func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		source.svid = picker([]*x509svid.SVID{client})
		require.NotNil(t, source.svid, "SPIFFE initialization must succeed before the downstream failure")
		return source, nil
	}

	// Accept and close the downstream probe so it fails deterministically before
	// a MySQL greeting, without depending on an external database or an unused port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-probeDone
	})

	changefeedID := common.NewChangefeedID4Test("default", "spiffe-db-init-failure")
	registryName := fmt.Sprintf("%s_spiffe_%d", mysqlTLSRegistryName(changefeedID),
		spiffeTLSRegistrySequence.Load()+1)
	t.Cleanup(func() { dmysql.DeregisterTLSConfig(registryName) })
	sinkURI := &url.URL{
		Scheme: "mysql", Host: listener.Addr().String(), User: url.User("replicator"),
		RawQuery: url.Values{
			spiffeClientIDPatternKey: {"spiffe://example.org/ticdc/client"},
			spiffeServerIDPatternKey: {"spiffe://example.org/tidb/server"},
		}.Encode(),
	}
	cfg, db, _, err := newMysqlConfigAndDB(context.Background(), changefeedID, sinkURI,
		&config.ChangefeedConfig{
			TimeZone: "UTC", SinkConfig: &config.SinkConfig{TiDBSourceID: 1},
		})
	require.Error(t, err)
	require.Nil(t, cfg)
	require.Nil(t, db)
	require.Equal(t, 1, source.closeCount(), "failed downstream initialization must close its SPIFFE source")
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.ErrorContains(t, err, "unknown config name", "failed downstream initialization must deregister TLS")
}

func TestApplyRejectsSPIFFETLSReapply(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, _ := newTestCA(t, "server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 63)
	td := spiffeid.RequireTrustDomainFromString("example.org")

	originalFactory := newSPIFFEX509Source
	t.Cleanup(func() { newSPIFFEX509Source = originalFactory })
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", "unix:///spire-agent-socket/spire-agent.sock")
	source := &testSPIFFESource{
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{serverCA}),
		},
	}
	factoryCalls := 0
	newSPIFFEX509Source = func(
		_ context.Context,
		_ string,
		picker func([]*x509svid.SVID) *x509svid.SVID,
	) (spiffeX509Source, error) {
		factoryCalls++
		source.svid = picker([]*x509svid.SVID{client})
		return source, nil
	}

	values := validSPIFFETLSValues()
	sinkURI := &url.URL{
		Scheme:   "mysql",
		Host:     "127.0.0.1:4000",
		User:     url.User("replicator"),
		RawQuery: values.Encode(),
	}
	changefeedID := common.NewChangefeedID4Test("default", "spiffe-apply-once")
	changefeedConfig := &config.ChangefeedConfig{
		TimeZone: "UTC",
		SinkConfig: &config.SinkConfig{
			TiDBSourceID: 1,
		},
	}
	cfg := New()
	require.NoError(t, cfg.Apply(sinkURI, changefeedID, changefeedConfig))
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 0, source.closeCount())
	firstResource := cfg.tlsResource
	firstTLS := cfg.TLS
	registryName := strings.TrimPrefix(cfg.TLS, "?tls=")
	_, err := dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.NoError(t, err)

	secondURI := *sinkURI
	secondURI.Host = "other.example:4000"
	err = cfg.Apply(&secondURI, changefeedID, changefeedConfig)
	require.ErrorContains(t, err, "cannot be applied more than once")
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 0, source.closeCount())
	require.Same(t, sinkURI, cfg.sinkURI)
	require.Same(t, firstResource, cfg.tlsResource)
	require.Equal(t, firstTLS, cfg.TLS)
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.NoError(t, err, "rejected re-apply must leave the original TLS registration usable")

	require.NoError(t, cfg.CloseTLS())
	require.Equal(t, 1, source.closeCount())
	_, err = dmysql.NewConnector(&dmysql.Config{TLSConfig: registryName})
	require.ErrorContains(t, err, "unknown config name")
	err = cfg.Apply(&secondURI, changefeedID, changefeedConfig)
	require.ErrorContains(t, err, "cannot be applied more than once")
	require.Equal(t, 1, factoryCalls)
	require.Equal(t, 1, source.closeCount())
}

func validSPIFFETLSValues() url.Values {
	return url.Values{
		spiffeClientIDPatternKey: {"spiffe://example.org/v1/prod/source/ns/ticdc/*"},
		spiffeServerIDPatternKey: {"spiffe://example.org/v1/prod/destination/ns/tidb/*"},
	}
}

type testSPIFFESource struct {
	mu       sync.RWMutex
	svid     *x509svid.SVID
	bundles  map[spiffeid.TrustDomain]*x509bundle.Bundle
	closeCnt int
}

func (s *testSPIFFESource) GetX509SVID() (*x509svid.SVID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.svid == nil {
		return nil, fmt.Errorf("no X509-SVID")
	}
	return s.svid, nil
}

func (s *testSPIFFESource) GetX509BundleForTrustDomain(
	trustDomain spiffeid.TrustDomain,
) (*x509bundle.Bundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bundle, ok := s.bundles[trustDomain]
	if !ok {
		return nil, fmt.Errorf("no bundle for %s", trustDomain)
	}
	return bundle, nil
}

func (s *testSPIFFESource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCnt++
	return nil
}

func (s *testSPIFFESource) setSVID(svid *x509svid.SVID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.svid = svid
}

func (s *testSPIFFESource) setBundle(
	trustDomain spiffeid.TrustDomain,
	bundle *x509bundle.Bundle,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundles[trustDomain] = bundle
}

func (s *testSPIFFESource) closeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closeCnt
}

func requireSPIFFEPattern(t *testing.T, raw string) spiffeIDPattern {
	t.Helper()
	pattern, err := parseSPIFFEIDPattern(raw)
	require.NoError(t, err)
	return pattern
}

func newTestCA(t *testing.T, commonName string) (*x509.Certificate, crypto.Signer) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(raw)
	require.NoError(t, err)
	return certificate, privateKey
}

func newTestSVID(
	t *testing.T,
	ca *x509.Certificate,
	caKey crypto.Signer,
	rawID string,
	serial int64,
) *x509svid.SVID {
	t.Helper()
	id := spiffeid.RequireFromString(rawID)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: rawID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
			x509.ExtKeyUsageServerAuth,
		},
		URIs: []*url.URL{id.URL()},
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(raw)
	require.NoError(t, err)
	return &x509svid.SVID{
		ID:           id,
		Certificates: []*x509.Certificate{certificate},
		PrivateKey:   privateKey,
	}
}

func handshakePeerSerial(t *testing.T, clientConfig, serverConfig *tls.Config) *big.Int {
	t.Helper()
	clientState, serverState, err := handshakeStates(clientConfig, serverConfig)
	require.NoError(t, err)
	require.NotEmpty(t, clientState.PeerCertificates)
	require.NotEmpty(t, serverState.PeerCertificates)
	return serverState.PeerCertificates[0].SerialNumber
}

func handshake(t *testing.T, clientConfig, serverConfig *tls.Config) error {
	t.Helper()
	_, _, err := handshakeStates(clientConfig, serverConfig)
	return err
}

func handshakeStates(
	clientConfig, serverConfig *tls.Config,
) (tls.ConnectionState, tls.ConnectionState, error) {
	clientSide, serverSide := net.Pipe()
	client := tls.Client(clientSide, clientConfig)
	server := tls.Server(serverSide, serverConfig)
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		client bool
		err    error
	}
	results := make(chan result, 2)
	go func() { results <- result{client: true, err: client.HandshakeContext(ctx)} }()
	go func() { results <- result{err: server.HandshakeContext(ctx)} }()

	var clientErr, serverErr error
	for range 2 {
		result := <-results
		if result.client {
			clientErr = result.err
		} else {
			serverErr = result.err
		}
	}
	if clientErr != nil {
		return tls.ConnectionState{}, tls.ConnectionState{}, clientErr
	}
	if serverErr != nil {
		return tls.ConnectionState{}, tls.ConnectionState{}, serverErr
	}
	return client.ConnectionState(), server.ConnectionState(), nil
}
