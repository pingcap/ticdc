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
	tokIDKrbAPReq = 256
	gssAPIGeneric = 0x60
)

type gssapiMechanism struct {
	config gssapiConfig
}

func (m *gssapiMechanism) Name() string {
	return string(gssapiMechanismName)
}

func (m *gssapiMechanism) Authenticate(
	_ context.Context,
	host string,
) (sasl.Session, []byte, error) {
	client, err := newKerberosClient(m.config)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if err = client.Login(); err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}

	serverHost := strings.SplitN(host, ":", 2)[0]
	spn := fmt.Sprintf("%s/%s", m.config.serviceName, serverHost)
	ticket, encKey, err := client.GetServiceTicket(spn)
	if err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}

	token, err := newKrb5Token(
		client.Credentials.Domain(), client.Credentials.CName(), ticket, encKey)
	if err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}
	firstMessage, err := appendGSSAPIHeader(token)
	if err != nil {
		client.Destroy()
		return nil, nil, errors.Trace(err)
	}
	return &gssapiSession{client: client, encKey: encKey}, firstMessage, nil
}

type gssapiSession struct {
	client *client.Client
	encKey types.EncryptionKey
}

func (s *gssapiSession) Challenge(challenge []byte) (bool, []byte, error) {
	defer s.client.Destroy()

	wrapTokenReq := gssapi.WrapToken{}
	if err := wrapTokenReq.Unmarshal(challenge, true); err != nil {
		return false, nil, errors.Trace(err)
	}
	isValid, err := wrapTokenReq.Verify(s.encKey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if !isValid {
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		return false, nil, errors.New("invalid gssapi wrap token")
	}

	wrapTokenResp, err := gssapi.NewInitiatorWrapToken(wrapTokenReq.Payload, s.encKey)
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	msg, err := wrapTokenResp.Marshal()
	if err != nil {
		return false, nil, errors.Trace(err)
	}
	return true, msg, nil
}

func buildGSSAPIMechanism(g gssapiConfig) (sasl.Mechanism, error) {
	if g.serviceName == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-service-name must not be empty when sasl mechanism is GSSAPI")
	}
	if g.kerberosConfigPath == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-kerberos-config-path must not be empty when sasl mechanism is GSSAPI")
	}
	if g.username == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-user must not be empty when sasl mechanism is GSSAPI")
	}
	if g.realm == "" {
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-realm must not be empty when sasl mechanism is GSSAPI")
	}

	switch g.authType {
	case userAuth:
		if g.password == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-password must not be empty when sasl-gssapi-auth-type is USER")
		}
	case keyTabAuth:
		if g.keyTabPath == "" {
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-keytab-path must not be empty when sasl-gssapi-auth-type is KEYTAB")
		}
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.authType)
	}

	return &gssapiMechanism{config: g}, nil
}

func newKerberosClient(g gssapiConfig) (*client.Client, error) {
	cfg, err := config.Load(g.kerberosConfigPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var krbClient *client.Client
	switch g.authType {
	case keyTabAuth:
		kt, err := keytab.Load(g.keyTabPath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		krbClient = client.NewWithKeytab(
			g.username, g.realm, kt, cfg, client.DisablePAFXFAST(g.disablePAFXFAST))
	case userAuth:
		krbClient = client.NewWithPassword(
			g.username, g.realm, g.password, cfg, client.DisablePAFXFAST(g.disablePAFXFAST))
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.authType)
	}
	return krbClient, nil
}

func newKrb5Token(
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

	body, err := apReq.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
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
		return nil, errors.Trace(err)
	}
	tkoLengthBytes := asn1tools.MarshalLengthBytes(len(oidBytes) + len(payload))
	header := append([]byte{gssAPIGeneric}, tkoLengthBytes...)
	header = append(header, oidBytes...)
	return append(header, payload...), nil
}
