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

package franz

import (
	"context"
	"encoding/binary"
	"net"

	"github.com/jcmturner/gofork/encoding/asn1"
	"github.com/jcmturner/gokrb5/v8/asn1tools"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/chksumtype"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/sasl"
)

const (
	// tokIDKrbAPReq and gssAPIGeneric identify a Kerberos AP-REQ inside a GSS-API initial context token.
	tokIDKrbAPReq = 0x0100
	gssAPIGeneric = 0x60
	// Authentication type values are part of sink URI compatibility and must remain stable.
	userAuth   = 1
	keyTabAuth = 2
)

type gssapiMechanism struct {
	config    GSSAPIConfig
	newClient func(GSSAPIConfig) (kerberosClient, error)
	newToken  func(string, types.PrincipalName, messages.Ticket, types.EncryptionKey) ([]byte, error)
}

type kerberosClient interface {
	Login() error
	Destroy()
	GetServiceTicket(string) (messages.Ticket, types.EncryptionKey, error)
	Domain() string
	CName() types.PrincipalName
}

type gokrb5Client struct {
	*client.Client
}

func (m *gssapiMechanism) Name() string {
	return "GSSAPI"
}

func (m *gssapiMechanism) Authenticate(
	_ context.Context,
	host string,
) (sasl.Session, []byte, error) {
	client, err := m.newClient(m.config)
	if err != nil {
		return nil, nil, err
	}

	if err = client.Login(); err != nil {
		client.Destroy()
		return nil, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	serverHost, err := brokerHost(host)
	if err != nil {
		client.Destroy()
		return nil, nil, err
	}

	spn := m.config.ServiceName + "/" + serverHost
	ticket, encKey, err := client.GetServiceTicket(spn)
	if err != nil {
		client.Destroy()
		return nil, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	token, err := m.newToken(client.Domain(), client.CName(), ticket, encKey)
	if err != nil {
		client.Destroy()
		return nil, nil, err
	}

	firstMessage, err := appendGSSAPIHeader(token)
	if err != nil {
		client.Destroy()
		return nil, nil, err
	}

	return &gssapiSession{client: client, encKey: encKey}, firstMessage, nil
}

func brokerHost(address string) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	return host, nil
}

type gssapiSession struct {
	client kerberosClient
	encKey types.EncryptionKey
}

func (s *gssapiSession) Challenge(challenge []byte) (bool, []byte, error) {
	defer s.client.Destroy()

	wrapTokenReq := gssapi.WrapToken{}
	if err := wrapTokenReq.Unmarshal(challenge, true); err != nil {
		return false, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	isValid, err := wrapTokenReq.Verify(s.encKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if !isValid {
		if err != nil {
			return false, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
		}

		return false, nil, errors.ErrNewKafkaSink.GenWithStackByArgs()
	}

	wrapTokenResp, err := gssapi.NewInitiatorWrapToken(wrapTokenReq.Payload, s.encKey)
	if err != nil {
		return false, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	msg, err := wrapTokenResp.Marshal()
	if err != nil {
		return false, nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	return true, msg, nil
}

func buildGSSAPIMechanism(g GSSAPIConfig) (sasl.Mechanism, error) {
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
	case userAuth:
		if g.Password == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-password must not be empty when sasl-gssapi-auth-type is USER")
		}
	case keyTabAuth:
		if g.KeyTabPath == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-keytab-path must not be empty when sasl-gssapi-auth-type is KEYTAB")
		}
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}

	return &gssapiMechanism{
		config:    g,
		newClient: newKerberosClient,
		newToken:  newKrb5Token,
	}, nil
}

func newKerberosClient(g GSSAPIConfig) (kerberosClient, error) {
	cfg, err := config.Load(g.KerberosConfigPath)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	var krbClient *client.Client
	switch g.AuthType {
	case keyTabAuth:
		kt, err := keytab.Load(g.KeyTabPath)
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
		krbClient = client.NewWithKeytab(
			g.Username, g.Realm, kt, cfg, client.DisablePAFXFAST(g.DisablePAFXFAST))
	case userAuth:
		krbClient = client.NewWithPassword(
			g.Username, g.Realm, g.Password, cfg, client.DisablePAFXFAST(g.DisablePAFXFAST))
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}

	return &gokrb5Client{Client: krbClient}, nil
}

func (c *gokrb5Client) Domain() string { return c.Credentials.Domain() }

func (c *gokrb5Client) CName() types.PrincipalName { return c.Credentials.CName() }

func newKrb5Token(
	domain string,
	cname types.PrincipalName,
	ticket messages.Ticket,
	sessionKey types.EncryptionKey,
) ([]byte, error) {
	authenticator, err := types.NewAuthenticator(domain, cname)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	authenticator.Cksum = types.Checksum{
		CksumType: chksumtype.GSSAPI,
		Checksum:  newAuthenticatorChecksum(),
	}

	apReq, err := messages.NewAPReq(ticket, sessionKey, authenticator)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	body, err := apReq.Marshal()
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	prefix := make([]byte, 2, 2+len(body))
	binary.BigEndian.PutUint16(prefix, tokIDKrbAPReq)

	return append(prefix, body...), nil
}

func newAuthenticatorChecksum() []byte {
	sum := make([]byte, 24)
	binary.LittleEndian.PutUint32(sum[:4], 16)

	flags := uint32(gssapi.ContextFlagInteg | gssapi.ContextFlagConf)
	binary.LittleEndian.PutUint32(sum[20:24], flags)

	return sum
}

func appendGSSAPIHeader(payload []byte) ([]byte, error) {
	oidBytes, err := asn1.Marshal(gssapi.OIDKRB5.OID())
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	tkoLengthBytes := asn1tools.MarshalLengthBytes(len(oidBytes) + len(payload))
	header := append([]byte{gssAPIGeneric}, tkoLengthBytes...)
	header = append(header, oidBytes...)

	return append(header, payload...), nil
}
