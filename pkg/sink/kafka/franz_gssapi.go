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

package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	krb5client "github.com/jcmturner/gokrb5/v8/client"
	krb5config "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/twmb/franz-go/pkg/sasl"
)

const (
	tokIDKrbAPReq  = 256
	gssAPIGeneric  = 0x60
	gssAPIInitial  = 1
	gssAPIVerify   = 2
	gssAPIFinished = 3
)

type franzKerberosClient interface {
	Login() error
	GetServiceTicket(spn string) (messages.Ticket, types.EncryptionKey, error)
	Domain() string
	CName() types.PrincipalName
	Destroy()
}

type franzGSSAPIMechanism struct {
	config security.GSSAPI
}

func (m *franzGSSAPIMechanism) Name() string {
	return "GSSAPI"
}

func (m *franzGSSAPIMechanism) Authenticate(
	_ context.Context,
	host string,
) (sasl.Session, []byte, error) {
	client, err := newFranzKerberosClient(m.config)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if err = client.Login(); err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}

	serverHost := strings.SplitN(host, ":", 2)[0]
	spn := fmt.Sprintf("%s/%s", m.config.ServiceName, serverHost)
	ticket, encKey, err := client.GetServiceTicket(spn)
	if err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}

	session := &franzGSSAPISession{
		client: client,
		ticket: ticket,
		encKey: encKey,
		step:   gssAPIInitial,
	}
	firstMessage, err := session.nextMessage(nil)
	if err != nil {
		session.close()
		return nil, nil, errors.Trace(err)
	}
	return session, firstMessage, nil
}

type franzGSSAPISession struct {
	client franzKerberosClient
	ticket messages.Ticket
	encKey types.EncryptionKey
	step   int

	closeOnce sync.Once
}

func (s *franzGSSAPISession) Challenge(challenge []byte) (bool, []byte, error) {
	switch s.step {
	case gssAPIVerify:
		msg, err := s.nextMessage(challenge)
		if err != nil {
			s.close()
			return false, nil, errors.Trace(err)
		}
		// Return a final payload while marking done=true.
		// franz-go will write this message and finish the auth flow.
		s.close()
		return true, msg, nil
	case gssAPIFinished:
		s.close()
		return true, nil, nil
	default:
		s.close()
		return false, nil, errors.New("invalid gssapi session state")
	}
}

func (s *franzGSSAPISession) close() {
	s.closeOnce.Do(func() {
		if s.client != nil {
			s.client.Destroy()
		}
	})
}

func (s *franzGSSAPISession) nextMessage(challenge []byte) ([]byte, error) {
	switch s.step {
	case gssAPIInitial:
		token, err := createKrb5Token(s.client.Domain(), s.client.CName(), s.ticket, s.encKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.step = gssAPIVerify
		return appendGSSAPIHeader(token)
	case gssAPIVerify:
		wrapTokenReq := gssapi.WrapToken{}
		if err := wrapTokenReq.Unmarshal(challenge, true); err != nil {
			return nil, errors.Trace(err)
		}
		isValid, err := wrapTokenReq.Verify(s.encKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
		if !isValid {
			if err != nil {
				return nil, errors.Trace(err)
			}
			return nil, errors.New("invalid gssapi wrap token")
		}

		wrapTokenResp, err := gssapi.NewInitiatorWrapToken(wrapTokenReq.Payload, s.encKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.step = gssAPIFinished
		return wrapTokenResp.Marshal()
	default:
		return nil, errors.New("invalid gssapi session state")
	}
}

func buildFranzGSSAPIMechanism(g security.GSSAPI) (sasl.Mechanism, error) {
	if g.ServiceName == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-service-name must not be empty when sasl mechanism is GSSAPI")
	}
	if g.KerberosConfigPath == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-kerberos-config-path must not be empty when sasl mechanism is GSSAPI")
	}
	if g.Username == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-user must not be empty when sasl mechanism is GSSAPI")
	}
	if g.Realm == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-realm must not be empty when sasl mechanism is GSSAPI")
	}

	switch g.AuthType {
	case security.UserAuth:
		if g.Password == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-password must not be empty when sasl-gssapi-auth-type is USER")
		}
	case security.KeyTabAuth:
		if g.KeyTabPath == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-keytab-path must not be empty when sasl-gssapi-auth-type is KEYTAB")
		}
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}

	return &franzGSSAPIMechanism{config: g}, nil
}

type franzGoKrb5Client struct {
	krb5client.Client
}

func (c *franzGoKrb5Client) Domain() string {
	return c.Credentials.Domain()
}

func (c *franzGoKrb5Client) CName() types.PrincipalName {
	return c.Credentials.CName()
}

func newFranzKerberosClient(g security.GSSAPI) (franzKerberosClient, error) {
	cfg, err := krb5config.Load(g.KerberosConfigPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var client *krb5client.Client
	switch g.AuthType {
	case security.KeyTabAuth:
		kt, err := keytab.Load(g.KeyTabPath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		client = krb5client.NewWithKeytab(
			g.Username, g.Realm, kt, cfg, krb5client.DisablePAFXFAST(g.DisablePAFXFAST))
	case security.UserAuth:
		client = krb5client.NewWithPassword(
			g.Username, g.Realm, g.Password, cfg, krb5client.DisablePAFXFAST(g.DisablePAFXFAST))
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}
	return &franzGoKrb5Client{*client}, nil
}

func createKrb5Token(
	domain string,
	cname types.PrincipalName,
	ticket messages.Ticket,
	sessionKey types.EncryptionKey,
) ([]byte, error) {
	authenticator, err := types.NewAuthenticator(domain, cname)
	if err != nil {
		return nil, errors.Trace(err)
	}

	authenticator.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  newAuthenticatorChecksum(),
	}
	apReq, err := messages.NewAPReq(ticket, sessionKey, authenticator)
	if err != nil {
		return nil, errors.Trace(err)
	}

	prefix := make([]byte, 2)
	binary.BigEndian.PutUint16(prefix, tokIDKrbAPReq)
	body, err := apReq.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return append(prefix, body...), nil
}

func newAuthenticatorChecksum() []byte {
	sum := make([]byte, 24)
	flags := []int{gssapi.ContextFlagInteg, gssapi.ContextFlagConf}
	binary.LittleEndian.PutUint32(sum[:4], 16)
	for _, flag := range flags {
		current := binary.LittleEndian.Uint32(sum[20:24])
		current |= uint32(flag)
		binary.LittleEndian.PutUint32(sum[20:24], current)
	}
	return sum
}

func appendGSSAPIHeader(payload []byte) ([]byte, error) {
	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		return nil, errors.Trace(err)
	}
	tkoLengthBytes := asn1tools.MarshalLengthBytes(len(oidBytes) + len(payload))
	header := append([]byte{gssAPIGeneric}, tkoLengthBytes...)
	header = append(header, oidBytes...)
	return append(header, payload...), nil
}
