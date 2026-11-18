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
	"crypto/tls"
	"net"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type muxCredentials struct {
	credentials.TransportCredentials
}

// NewGRPCServerCredentials preserves the TLS authentication performed before
// protocol multiplexing. It is only for servers using an already secured listener.
func NewGRPCServerCredentials() credentials.TransportCredentials {
	return &muxCredentials{TransportCredentials: insecure.NewCredentials()}
}

func (c *muxCredentials) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn := rawConn
	if muxConn, ok := conn.(*cmux.MuxConn); ok {
		conn = muxConn.Conn
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		if !state.HandshakeComplete {
			return nil, nil, errors.ErrUnauthorized.FastGenByArgs("client", "TLS handshake is incomplete")
		}
		return rawConn, credentials.TLSInfo{
			State:         state,
			SecurityLevel: credentials.PrivacyAndIntegrity,
		}, nil
	}
	return c.TransportCredentials.ServerHandshake(rawConn)
}

func (c *muxCredentials) Clone() credentials.TransportCredentials {
	return NewGRPCServerCredentials()
}
