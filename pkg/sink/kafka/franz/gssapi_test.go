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

package franz

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/etypeID"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/iana/nametype"
	"github.com/jcmturner/gokrb5/v8/messages"
	"github.com/jcmturner/gokrb5/v8/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

type fakeKerberosClient struct {
	loginError  error
	ticketError error
	spn         string
	destroyed   bool
}

func (c *fakeKerberosClient) Login() error { return c.loginError }

func (c *fakeKerberosClient) Destroy() { c.destroyed = true }

func (c *fakeKerberosClient) GetServiceTicket(spn string) (
	messages.Ticket,
	types.EncryptionKey,
	error,
) {
	c.spn = spn

	return messages.Ticket{}, testEncryptionKey(), c.ticketError
}

func (c *fakeKerberosClient) Domain() string { return "EXAMPLE.COM" }

func (c *fakeKerberosClient) CName() types.PrincipalName {
	return types.NewPrincipalName(nametype.KRB_NT_PRINCIPAL, "alice")
}

func TestBrokerHost(t *testing.T) {
	for _, test := range []struct {
		address  string
		expected string
	}{
		{address: "broker.example.com:9092", expected: "broker.example.com"},
		{address: "[2001:db8::1]:9092", expected: "2001:db8::1"},
	} {
		host, err := brokerHost(test.address)
		require.NoError(t, err)
		require.Equal(t, test.expected, host)
	}

	_, err := brokerHost("2001:db8::1:9092")
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestGSSAPIConfigValidation(t *testing.T) {
	valid := GSSAPIConfig{
		AuthType:           userAuth,
		KerberosConfigPath: "/etc/krb5.conf",
		ServiceName:        "kafka",
		Username:           "alice",
		Password:           "secret",
		Realm:              "EXAMPLE.COM",
	}

	for _, mutate := range []func(*GSSAPIConfig){
		func(cfg *GSSAPIConfig) { cfg.ServiceName = "" },
		func(cfg *GSSAPIConfig) { cfg.KerberosConfigPath = "" },
		func(cfg *GSSAPIConfig) { cfg.Username = "" },
		func(cfg *GSSAPIConfig) { cfg.Realm = "" },
		func(cfg *GSSAPIConfig) { cfg.Password = "" },
		func(cfg *GSSAPIConfig) { cfg.AuthType = 0 },
		func(cfg *GSSAPIConfig) { cfg.AuthType, cfg.KeyTabPath = keyTabAuth, "" },
	} {
		cfg := valid
		mutate(&cfg)

		_, err := buildGSSAPIMechanism(cfg)
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	}
}

func TestGSSAPIEncodingHelpers(t *testing.T) {
	checksum := newAuthenticatorChecksum()
	require.Len(t, checksum, 24)
	require.Equal(t, uint32(16), binary.LittleEndian.Uint32(checksum[:4]))
	require.Equal(t, uint32(gssapi.ContextFlagInteg|gssapi.ContextFlagConf), binary.LittleEndian.Uint32(checksum[20:24]))

	payload := []byte{1, 2, 3}
	message, err := appendGSSAPIHeader(payload)
	require.NoError(t, err)
	require.Equal(t, byte(gssAPIGeneric), message[0])
	require.Equal(t, payload, message[len(message)-len(payload):])
}

func TestNewKerberosClientRejectsMissingConfig(t *testing.T) {
	_, err := newKerberosClient(GSSAPIConfig{
		AuthType:           userAuth,
		KerberosConfigPath: "/path/that/does/not/exist",
		Username:           "alice",
		Password:           "secret",
		Realm:              "EXAMPLE.COM",
	})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}

func TestGSSAPIAuthenticate(t *testing.T) {
	client := &fakeKerberosClient{}
	mechanism := &gssapiMechanism{
		config: GSSAPIConfig{ServiceName: "kafka"},
		newClient: func(GSSAPIConfig) (kerberosClient, error) {
			return client, nil
		},
		newToken: func(
			domain string,
			cname types.PrincipalName,
			_ messages.Ticket,
			_ types.EncryptionKey,
		) ([]byte, error) {
			require.Equal(t, "EXAMPLE.COM", domain)
			require.Equal(t, "alice", cname.PrincipalNameString())

			return []byte{1, 2, 3}, nil
		},
	}

	session, message, err := mechanism.Authenticate(context.Background(), "[2001:db8::1]:9092")
	require.NoError(t, err)
	require.NotNil(t, session)
	require.Equal(t, "kafka/2001:db8::1", client.spn)
	require.Equal(t, []byte{1, 2, 3}, message[len(message)-3:])
	require.False(t, client.destroyed)

	done, _, err := session.Challenge([]byte("invalid"))
	require.False(t, done)
	require.ErrorIs(t, err, errors.ErrNewKafkaSink)
	require.True(t, client.destroyed)
}

func TestGSSAPIAuthenticateDestroysClientAfterFailure(t *testing.T) {
	for _, test := range []struct {
		name        string
		host        string
		loginError  error
		ticketError error
		tokenError  error
	}{
		{name: "login", host: "broker:9092", loginError: context.Canceled},
		{name: "broker address", host: "invalid"},
		{name: "service ticket", host: "broker:9092", ticketError: context.DeadlineExceeded},
		{name: "AP request", host: "broker:9092", tokenError: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &fakeKerberosClient{
				loginError:  test.loginError,
				ticketError: test.ticketError,
			}
			mechanism := &gssapiMechanism{
				config: GSSAPIConfig{ServiceName: "kafka"},
				newClient: func(GSSAPIConfig) (kerberosClient, error) {
					return client, nil
				},
				newToken: func(
					string,
					types.PrincipalName,
					messages.Ticket,
					types.EncryptionKey,
				) ([]byte, error) {
					return nil, test.tokenError
				},
			}

			_, _, err := mechanism.Authenticate(context.Background(), test.host)
			require.Error(t, err)
			require.True(t, client.destroyed)
		})
	}
}

func TestGSSAPIChallenge(t *testing.T) {
	key := testEncryptionKey()
	request := gssapi.WrapToken{
		Flags:   1,
		EC:      12,
		Payload: []byte{1, 2, 3, 4},
	}
	require.NoError(t, request.SetCheckSum(key, keyusage.GSSAPI_ACCEPTOR_SEAL))

	challenge, err := request.Marshal()
	require.NoError(t, err)

	client := &fakeKerberosClient{}
	session := &gssapiSession{client: client, encKey: key}
	done, response, err := session.Challenge(challenge)
	require.NoError(t, err)
	require.True(t, done)
	require.True(t, client.destroyed)

	initiatorToken := gssapi.WrapToken{}
	require.NoError(t, initiatorToken.Unmarshal(response, false))
	require.Equal(t, request.Payload, initiatorToken.Payload)

	valid, err := initiatorToken.Verify(key, keyusage.GSSAPI_INITIATOR_SEAL)
	require.NoError(t, err)
	require.True(t, valid)
}

func testEncryptionKey() types.EncryptionKey {
	return types.EncryptionKey{
		KeyType:  etypeID.AES128_CTS_HMAC_SHA1_96,
		KeyValue: []byte("0123456789abcdef"),
	}
}
