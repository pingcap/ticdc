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
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/xdg/scram"
)

func TestSCRAMClientGeneratorHandshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mechanism     saslMechanism
		hashGenerator scram.HashGeneratorFcn
	}{
		{name: "SHA-256", mechanism: scram256Mechanism, hashGenerator: sha256HashGenerator},
		{name: "SHA-512", mechanism: scram512Mechanism, hashGenerator: sha512HashGenerator},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			const (
				username = "user"
				password = "password"
			)
			options := NewOptions()
			options.sasl = &saslConfig{
				user:      username,
				password:  password,
				mechanism: test.mechanism,
			}
			config := sarama.NewConfig()
			require.NoError(t, completeSaramaSASLConfig(t.Context(), config, options))
			require.NotNil(t, config.Net.SASL.SCRAMClientGeneratorFunc)

			client := config.Net.SASL.SCRAMClientGeneratorFunc()
			require.NoError(t, client.Begin(username, password, ""))

			credentialClient, err := test.hashGenerator.NewClient(username, password, "")
			require.NoError(t, err)
			credentials := credentialClient.GetStoredCredentials(scram.KeyFactors{
				Salt:  "salt",
				Iters: 4096,
			})
			server, err := test.hashGenerator.NewServer(
				func(string) (scram.StoredCredentials, error) { return credentials, nil })
			require.NoError(t, err)
			serverConversation := server.NewConversation()

			clientMessage, err := client.Step("")
			require.NoError(t, err)
			serverMessage, err := serverConversation.Step(clientMessage)
			require.NoError(t, err)
			clientMessage, err = client.Step(serverMessage)
			require.NoError(t, err)
			serverMessage, err = serverConversation.Step(clientMessage)
			require.NoError(t, err)
			clientMessage, err = client.Step(serverMessage)
			require.NoError(t, err)

			require.Empty(t, clientMessage)
			require.True(t, client.Done())
			require.True(t, serverConversation.Done())
			require.True(t, serverConversation.Valid())
		})
	}
}
