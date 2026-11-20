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
// See the License for the specific language governing permissions and
// limitations under the License.

package tcpserver

import (
	"context"
	"testing"
	"time"

	grpcTesting "github.com/grpc-ecosystem/go-grpc-middleware/testing"
	grpcTestingProto "github.com/grpc-ecosystem/go-grpc-middleware/testing/testproto"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestGRPCServerCredentialsPreserveAuthentication(t *testing.T) {
	for _, mode := range []string{"plaintext", "TLS", "mutual TLS"} {
		t.Run(mode, func(t *testing.T) {
			credential := &security.Credential{}
			if mode != "plaintext" {
				credential = makeCredential4Testing(t)
				credential.MTLS = mode == "mutual TLS"
			}
			server, err := NewTCPServer("127.0.0.1:0", credential)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			tcpDone := make(chan struct{})
			go func() {
				defer close(tcpDone)
				_ = server.Run(ctx)
			}()
			authInfo := make(chan credentials.AuthInfo, 1)
			grpcServer := grpc.NewServer(
				grpc.Creds(NewGRPCServerCredentials().Clone()),
				grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					remotePeer, ok := peer.FromContext(ctx)
					if ok {
						authInfo <- remotePeer.AuthInfo
					} else {
						authInfo <- nil
					}
					return handler(ctx, request)
				}),
			)
			grpcTestingProto.RegisterTestServiceServer(grpcServer, &grpcTesting.TestPingService{T: t})
			grpcDone := make(chan struct{})
			go func() {
				defer close(grpcDone)
				_ = grpcServer.Serve(server.GrpcListener())
			}()
			t.Cleanup(func() {
				grpcServer.Stop()
				cancel()
				_ = server.Close(context.Background())
				for _, done := range []chan struct{}{tcpDone, grpcDone} {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("server did not stop")
					}
				}
			})
			dialOption, err := credential.ToGRPCDialOption()
			require.NoError(t, err)
			conn, err := grpc.NewClient(server.GrpcListener().Addr().String(), dialOption)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			client := grpcTestingProto.NewTestServiceClient(conn)
			_, err = client.Ping(ctx, &grpcTestingProto.PingRequest{Value: "authenticated"})
			require.NoError(t, err)
			observed := <-authInfo
			require.NotNil(t, observed)
			if mode == "plaintext" {
				require.Equal(t, "insecure", observed.AuthType())
				return
			}
			tlsInfo, ok := observed.(credentials.TLSInfo)
			require.True(t, ok)
			require.True(t, tlsInfo.State.HandshakeComplete)
			require.Equal(t, credentials.PrivacyAndIntegrity, tlsInfo.SecurityLevel)
			if mode == "mutual TLS" {
				require.NotEmpty(t, tlsInfo.State.PeerCertificates)
				require.NotEmpty(t, tlsInfo.State.VerifiedChains)
				require.NoError(t, tlsInfo.State.PeerCertificates[0].VerifyHostname("127.0.0.1"))
			} else {
				require.Empty(t, tlsInfo.State.VerifiedChains)
			}
		})
	}
}
