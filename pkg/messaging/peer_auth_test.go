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

package messaging

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/test/bufconn"
)

func TestMessageCenterAuthenticatePeer(t *testing.T) {
	certificate := &x509.Certificate{
		DNSNames:    []string{"capture.example"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	verified := tls.ConnectionState{
		HandshakeComplete: true,
		PeerCertificates:  []*x509.Certificate{certificate},
		VerifiedChains:    [][]*x509.Certificate{{certificate}},
	}
	unverified := verified
	unverified.VerifiedChains = nil
	missingCertificate := verified
	missingCertificate.PeerCertificates = nil
	incomplete := verified
	incomplete.HandshakeComplete = false
	_, insecureAuthInfo, err := insecure.NewCredentials().ServerHandshake(nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name       string
		security   *security.Credential
		authInfo   credentials.AuthInfo
		address    string
		expectPass bool
	}{
		{name: "plaintext compatibility", address: "capture.example:8300", expectPass: true},
		{name: "server TLS compatibility", security: &security.Credential{}, address: "capture.example:8300", expectPass: true},
		{name: "missing peer", security: &security.Credential{MTLS: true}, address: "capture.example:8300"},
		{name: "missing TLS identity", security: &security.Credential{MTLS: true}, authInfo: insecureAuthInfo, address: "capture.example:8300"},
		{name: "unverified certificate", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: unverified}, address: "capture.example:8300"},
		{name: "missing certificate", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: missingCertificate}, address: "capture.example:8300"},
		{name: "incomplete handshake", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: incomplete}, address: "capture.example:8300"},
		{name: "wrong registered host", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: verified}, address: "other.example:8300"},
		{name: "invalid registered address", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: verified}, address: "capture.example"},
		{name: "verified DNS identity", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: verified}, address: "capture.example:8300", expectPass: true},
		{name: "verified IPv4 identity", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: verified}, address: "127.0.0.1:8300", expectPass: true},
		{name: "verified IPv6 identity", security: &security.Credential{MTLS: true}, authInfo: credentials.TLSInfo{State: verified}, address: "[::1]:8300", expectPass: true},
		{name: "CN policy also requires identity", security: &security.Credential{CertAllowedCN: []string{"capture"}}, address: "capture.example:8300"},
		{name: "CN policy with verified identity", security: &security.Credential{CertAllowedCN: []string{"capture"}}, authInfo: credentials.TLSInfo{State: verified}, address: "capture.example:8300", expectPass: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			if tc.authInfo != nil {
				ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: tc.authInfo})
			}
			server := &grpcServer{messageCenter: &messageCenter{security: tc.security}}
			err := server.authenticatePeer(ctx, &remoteMessageTarget{targetAddr: tc.address})
			if tc.expectPass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				code, ok := errors.RFCCode(err)
				require.True(t, ok)
				require.Equal(t, errors.ErrUnauthorized.RFCCode(), code)
			}
		})
	}
}

func newPeerTestClient(t *testing.T, mc *messageCenter) proto.MessageServiceClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	proto.RegisterMessageServiceServer(server, &grpcServer{messageCenter: mc})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = lis.Close() })
	conn, err := grpc.NewClient("passthrough:///peer-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewMessageServiceClient(conn)
}

func TestMessageCenterRejectsInvalidHandshake(t *testing.T) {
	valid, err := (&HandshakeMessage{Version: 1, StreamType: streamTypeCommand}).Marshal()
	require.NoError(t, err)
	unknown, err := (&HandshakeMessage{Version: 1, StreamType: "unknown"}).Marshal()
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		from    string
		to      string
		kind    IOType
		payload [][]byte
	}{
		{name: "wrong message type", from: "remote", to: "local", kind: TypeLogCoordinatorBroadcastRequest, payload: [][]byte{valid}},
		{name: "missing payload", from: "remote", to: "local", kind: TypeMessageHandShake},
		{name: "multiple payloads", from: "remote", to: "local", kind: TypeMessageHandShake, payload: [][]byte{valid, valid}},
		{name: "invalid JSON", from: "remote", to: "local", kind: TypeMessageHandShake, payload: [][]byte{[]byte("{")}},
		{name: "unknown stream type", from: "remote", to: "local", kind: TypeMessageHandShake, payload: [][]byte{unknown}},
		{name: "missing sender", to: "local", kind: TypeMessageHandShake, payload: [][]byte{valid}},
		{name: "wrong receiver", from: "remote", to: "other", kind: TypeMessageHandShake, payload: [][]byte{valid}},
		{name: "unauthenticated sender", from: "remote", to: "local", kind: TypeMessageHandShake, payload: [][]byte{valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cancelled := false
			target := &remoteMessageTarget{targetAddr: "remote.example:8300"}
			oldSession := &streamSession{cancel: func() { cancelled = true }}
			target.streams.Store(streamTypeCommand, oldSession)
			mc := &messageCenter{id: "local", security: &security.Credential{MTLS: true}}
			mc.remoteTargets.m = map[node.ID]*remoteMessageTarget{"remote": target}
			client := newPeerTestClient(t, mc)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := client.StreamMessages(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&proto.Message{From: tc.from, To: tc.to, Type: int32(tc.kind), Payload: tc.payload}))
			_, err = stream.Recv()
			require.Error(t, err)
			require.NotEqual(t, context.DeadlineExceeded, ctx.Err())
			require.False(t, cancelled)
			current, ok := target.streams.Load(streamTypeCommand)
			require.True(t, ok)
			require.Same(t, oldSession, current)
			_, ok = target.streams.Load("unknown")
			require.False(t, ok)
		})
	}
}

func TestMessageCenterBindsFrameIdentity(t *testing.T) {
	for _, name := range []string{"valid", "forged sender", "wrong receiver", "missing sender", "missing receiver"} {
		t.Run(name, func(t *testing.T) {
			target := newRemoteMessageTargetForTest()
			t.Cleanup(target.close)
			mc := &messageCenter{id: target.messageCenterID}
			mc.remoteTargets.m = map[node.ID]*remoteMessageTarget{target.targetId: target}
			client := newPeerTestClient(t, mc)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			eventStream, err := client.StreamMessages(ctx)
			require.NoError(t, err)
			eventHandshake, err := (&HandshakeMessage{Version: 1, StreamType: streamTypeEvent}).Marshal()
			require.NoError(t, err)
			require.NoError(t, eventStream.Send(&proto.Message{
				From: string(target.targetId), To: string(target.messageCenterID), Type: int32(TypeMessageHandShake), Payload: [][]byte{eventHandshake},
			}))
			stream, err := client.StreamMessages(ctx)
			require.NoError(t, err)
			handshake, err := (&HandshakeMessage{Version: 1, StreamType: streamTypeCommand}).Marshal()
			require.NoError(t, err)
			require.NoError(t, stream.Send(&proto.Message{
				From: string(target.targetId), To: string(target.messageCenterID), Type: int32(TypeMessageHandShake), Payload: [][]byte{handshake},
			}))
			frame := &proto.Message{
				From: string(target.targetId), To: string(target.messageCenterID), Type: int32(TypeLogCoordinatorBroadcastRequest), Payload: [][]byte{nil},
			}
			switch name {
			case "forged sender":
				frame.From = "forged-coordinator"
			case "wrong receiver":
				frame.To = "other-capture"
			case "missing sender":
				frame.From = ""
			case "missing receiver":
				frame.To = ""
			}
			require.NoError(t, stream.Send(frame))
			require.NoError(t, stream.CloseSend())
			_, err = stream.Recv()
			require.Error(t, err)
			require.NotEqual(t, context.DeadlineExceeded, ctx.Err())
			if name == "valid" {
				require.Len(t, target.recvCmdCh, 1)
				message := <-target.recvCmdCh
				require.Equal(t, target.targetId, message.From)
				require.Equal(t, target.messageCenterID, message.To)
			} else {
				require.Empty(t, target.recvCmdCh)
			}
		})
	}
}
