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
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	gomysqlserver "github.com/go-mysql-org/go-mysql/server"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSPIFFEWorkloadAPIWatchRotationAndClose(t *testing.T) {
	clientCA, clientCAKey := newTestCA(t, "client-ca")
	serverCA, serverCAKey := newTestCA(t, "server-ca")
	rotatedServerCA, rotatedServerCAKey := newTestCA(t, "rotated-server-ca")
	client := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 81)
	rotatedClient := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/ticdc/client", 82)
	server := newTestSVID(t, serverCA, serverCAKey,
		"spiffe://example.org/v1/prod/destination/ns/tidb/server", 83)
	rotatedServer := newTestSVID(t, rotatedServerCA, rotatedServerCAKey,
		"spiffe://example.org/v1/prod/destination/ns/tidb/server", 84)
	td := spiffeid.RequireTrustDomainFromString("example.org")
	serverSource := &testSPIFFESource{
		svid: server,
		bundles: map[spiffeid.TrustDomain]*x509bundle.Bundle{
			td: x509bundle.FromX509Authorities(td, []*x509.Certificate{clientCA}),
		},
	}
	serverTLS := tlsconfig.MTLSServerConfig(serverSource, serverSource,
		authorizeSPIFFEIDPattern(requireSPIFFEPattern(t, "spiffe://example.org/v1/prod/source/ns/ticdc/*")))
	api, endpoint := startTestSPIFFEWorkloadAPI(t)
	api.updates <- testWorkloadAPIResponse(t, client, clientCA, serverCA)
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", endpoint)
	sinkURI := &url.URL{
		Scheme: "mysql", Host: "127.0.0.1:4000", User: url.User("replicator"),
		RawQuery: validSPIFFETLSValues().Encode(),
	}
	cfg := New()
	t.Cleanup(func() { require.NoError(t, cfg.CloseTLS()) })
	initCtx, cancelInit := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, cfg.apply(initCtx, sinkURI,
		common.NewChangefeedID4Test("default", "spiffe-workload-api"),
		&config.ChangefeedConfig{TimeZone: "UTC", SinkConfig: &config.SinkConfig{TiDBSourceID: 1}}))
	cancelInit()
	require.ErrorIs(t, initCtx.Err(), context.Canceled)
	require.EqualValues(t, 1, api.started.Load())
	require.Zero(t, api.stopped.Load(), "the initialization context must not stop the rotating watch")

	first := connectTestSPIFFEMySQLDriver(t, cfg, sinkURI, serverTLS)
	require.Equal(t, client.Certificates[0].SerialNumber, first.PeerCertificates[0].SerialNumber)

	// The actual go-spiffe Workload API watcher, rather than a test source,
	// must replace both the client SVID and the trusted server issuer bundle.
	api.updates <- testWorkloadAPIResponse(t, rotatedClient, clientCA, rotatedServerCA)
	require.Eventually(t, func() bool {
		svid, err := cfg.tlsResource.source.GetX509SVID()
		return err == nil && svid.Certificates[0].SerialNumber.Cmp(rotatedClient.Certificates[0].SerialNumber) == 0
	}, 5*time.Second, time.Millisecond)
	serverSource.setSVID(rotatedServer)
	second := connectTestSPIFFEMySQLDriver(t, cfg, sinkURI, serverTLS)
	require.Equal(t, rotatedClient.Certificates[0].SerialNumber, second.PeerCertificates[0].SerialNumber)
	require.NotEqual(t, first.PeerCertificates[0].SerialNumber, second.PeerCertificates[0].SerialNumber)
	require.EqualValues(t, 1, api.started.Load(), "rotation must reuse the existing watch")
	require.Zero(t, api.stopped.Load())

	// A validly issued certificate with the wrong client URI must replace the
	// previous SVID with no match, not preserve an authorized identity forever.
	wrongClient := newTestSVID(t, clientCA, clientCAKey,
		"spiffe://example.org/v1/prod/source/ns/other/client", 85)
	api.updates <- testWorkloadAPIResponse(t, wrongClient, clientCA, rotatedServerCA)
	require.Eventually(t, func() bool {
		_, err := cfg.tlsResource.source.GetX509SVID()
		return err != nil
	}, 5*time.Second, time.Millisecond)
	observed, err := attemptTestSPIFFEMySQLDriver(t, cfg, sinkURI, serverTLS)
	require.Error(t, err, "an invalid rotated client SVID must fail closed")
	require.Error(t, observed.err, "the server must not accept a plaintext fallback")

	api.updates <- testWorkloadAPIResponse(t, rotatedClient, clientCA, rotatedServerCA)
	require.Eventually(t, func() bool {
		svid, err := cfg.tlsResource.source.GetX509SVID()
		return err == nil && svid.Certificates[0].SerialNumber.Cmp(rotatedClient.Certificates[0].SerialNumber) == 0
	}, 5*time.Second, time.Millisecond)
	recovered := connectTestSPIFFEMySQLDriver(t, cfg, sinkURI, serverTLS)
	require.Equal(t, rotatedClient.Certificates[0].SerialNumber, recovered.PeerCertificates[0].SerialNumber)
	require.EqualValues(t, 1, api.started.Load(), "recovery must reuse the existing watch")
	require.Zero(t, api.stopped.Load())

	// Reconnect verification can read a bundle while a stream update replaces
	// it. Exercise the real source's concurrent access under the race detector.
	readerCtx, cancelReader := context.WithCancel(context.Background())
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for readerCtx.Err() == nil {
			_, _ = cfg.tlsResource.source.GetX509BundleForTrustDomain(td)
		}
	}()
	t.Cleanup(func() { cancelReader(); <-readerDone })
	for i := range 30 {
		current := client
		if i%2 == 1 {
			current = rotatedClient
		}
		api.updates <- testWorkloadAPIResponse(t, current, clientCA, rotatedServerCA)
		require.Eventually(t, func() bool {
			svid, err := cfg.tlsResource.source.GetX509SVID()
			return err == nil && svid.Certificates[0].SerialNumber.Cmp(current.Certificates[0].SerialNumber) == 0
		}, 5*time.Second, time.Millisecond)
	}
	cancelReader()
	<-readerDone

	require.NoError(t, cfg.CloseTLS())
	require.NoError(t, cfg.CloseTLS())
	require.Eventually(t, func() bool { return api.stopped.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 1, api.started.Load())
	_, err = cfg.tlsResource.source.GetX509SVID()
	require.Error(t, err, "closing the sink must close the Workload API source")
}

func TestSPIFFEWorkloadAPINoInitialDataHonorsDeadline(t *testing.T) {
	api, endpoint := startTestSPIFFEWorkloadAPI(t)
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", endpoint)
	sinkURI := &url.URL{
		Scheme: "mysql", Host: "127.0.0.1:4000", User: url.User("replicator"),
		RawQuery: validSPIFFETLSValues().Encode(),
	}
	cfg := New()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	err := cfg.apply(ctx, sinkURI, common.NewChangefeedID4Test("default", "spiffe-no-initial-data"),
		&config.ChangefeedConfig{TimeZone: "UTC", SinkConfig: &config.SinkConfig{TiDBSourceID: 1}})
	require.ErrorContains(t, err, context.DeadlineExceeded.Error())
	// Leave scheduling headroom for race-enabled CI while the context retains
	// its one second initialization deadline.
	require.Less(t, time.Since(started), 5*time.Second)
	require.Empty(t, cfg.TLS, "missing Workload API data must not fall back to plaintext")
	require.Nil(t, cfg.tlsResource)
	require.Eventually(t, func() bool { return api.stopped.Load() == 1 }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 1, api.started.Load())
}

type testSPIFFEWorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer
	updates chan *workload.X509SVIDResponse
	started atomic.Int64
	stopped atomic.Int64
}

func (s *testSPIFFEWorkloadAPI) FetchX509SVID(
	_ *workload.X509SVIDRequest,
	stream workload.SpiffeWorkloadAPI_FetchX509SVIDServer,
) error {
	s.started.Add(1)
	defer s.stopped.Add(1)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case response := <-s.updates:
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func startTestSPIFFEWorkloadAPI(t *testing.T) (*testSPIFFEWorkloadAPI, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	api := &testSPIFFEWorkloadAPI{updates: make(chan *workload.X509SVIDResponse, 2)}
	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, api)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})
	return api, "tcp://" + listener.Addr().String()
}

func testWorkloadAPIResponse(t *testing.T, svid *x509svid.SVID, authorities ...*x509.Certificate) *workload.X509SVIDResponse {
	t.Helper()
	key, err := x509.MarshalPKCS8PrivateKey(svid.PrivateKey)
	require.NoError(t, err)
	var chain, bundle []byte
	for _, cert := range svid.Certificates {
		chain = append(chain, cert.Raw...)
	}
	for _, ca := range authorities {
		bundle = append(bundle, ca.Raw...)
	}
	return &workload.X509SVIDResponse{Svids: []*workload.X509SVID{{
		SpiffeId: svid.ID.String(), X509Svid: chain, X509SvidKey: key, Bundle: bundle,
	}}}
}

func connectTestSPIFFEMySQLDriver(t *testing.T, cfg *Config, sinkURI *url.URL, serverTLS *tls.Config) tls.ConnectionState {
	t.Helper()
	observed, err := attemptTestSPIFFEMySQLDriver(t, cfg, sinkURI, serverTLS)
	require.NoError(t, err)
	require.NoError(t, observed.err)
	require.Equal(t, "replicator", observed.user)
	require.NotEmpty(t, observed.state.PeerCertificates)
	return observed.state
}

func attemptTestSPIFFEMySQLDriver(t *testing.T, cfg *Config, sinkURI *url.URL, serverTLS *tls.Config) (testMySQLHandshakeResult, error) {
	t.Helper()
	address, result := startTestMySQLServer(t, serverTLS)
	connectionURI := *sinkURI
	connectionURI.Host = address
	originalURI := cfg.sinkURI
	cfg.sinkURI = &connectionURI
	defer func() { cfg.sinkURI = originalURI }()
	dsn, err := GenBasicDSN(cfg)
	require.NoError(t, err)
	require.NotNil(t, dsn.TLS)
	require.False(t, dsn.AllowFallbackToPlaintext)
	connector, err := dmysql.NewConnector(dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := connector.Connect(ctx)
	if err == nil {
		require.NoError(t, conn.Close())
	}
	observed := <-result
	return observed, err
}

type testMySQLHandshakeResult struct {
	state tls.ConnectionState
	user  string
	err   error
}

type testMySQLHandler struct {
	gomysqlserver.EmptyHandler
}

func (testMySQLHandler) HandleQuery(string) (*gomysql.Result, error) {
	// The real driver initializes its connection using SET session variables.
	return &gomysql.Result{}, nil
}

func startTestMySQLServer(t *testing.T, tlsConfig *tls.Config) (string, <-chan testMySQLHandshakeResult) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	result := make(chan testMySQLHandshakeResult, 1)
	done := make(chan struct{})
	var mu sync.Mutex
	var accepted net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		if accepted != nil {
			_ = accepted.Close()
		}
		mu.Unlock()
		<-done
	})
	go func() {
		defer close(done)
		netConn, err := listener.Accept()
		if err != nil {
			result <- testMySQLHandshakeResult{err: err}
			return
		}
		defer netConn.Close()
		mu.Lock()
		accepted = netConn
		mu.Unlock()
		_ = netConn.SetDeadline(time.Now().Add(5 * time.Second))
		server := gomysqlserver.NewServer("8.0.11", gomysql.DEFAULT_COLLATION_ID,
			gomysql.AUTH_NATIVE_PASSWORD, nil, tlsConfig)
		provider := gomysqlserver.NewInMemoryProvider()
		provider.AddUser("replicator", "")
		conn, err := gomysqlserver.NewCustomizedConn(netConn, server, provider, testMySQLHandler{})
		if err != nil {
			result <- testMySQLHandshakeResult{err: err}
			return
		}
		defer func() {
			if !conn.Closed() {
				conn.Close()
			}
		}()
		observed := testMySQLHandshakeResult{user: conn.GetUser()}
		if tlsConn, ok := conn.Conn.Conn.(*tls.Conn); ok {
			observed.state = tlsConn.ConnectionState()
		}
		result <- observed
		for conn.HandleCommand() == nil && !conn.Closed() {
		}
	}()
	return listener.Addr().String(), result
}
