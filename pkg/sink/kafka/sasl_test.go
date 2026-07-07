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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildSaslMechanismGSSAPIUserAuth(t *testing.T) {
	t.Parallel()

	o := &clientOptions{
		SASL: &SASL{
			SASLMechanism: GSSAPIMechanism,
			GSSAPI: GSSAPI{
				AuthType:           UserAuth,
				KerberosConfigPath: "/etc/krb5.conf",
				ServiceName:        "kafka",
				Username:           "alice",
				Password:           "pwd",
				Realm:              "EXAMPLE.COM",
			},
		},
	}

	mechanism, err := buildSaslMechanism(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, "GSSAPI", mechanism.Name())
}

func TestBuildSaslMechanismGSSAPIKeytabAuth(t *testing.T) {
	t.Parallel()

	o := &clientOptions{
		SASL: &SASL{
			SASLMechanism: GSSAPIMechanism,
			GSSAPI: GSSAPI{
				AuthType:           KeyTabAuth,
				KerberosConfigPath: "/etc/krb5.conf",
				ServiceName:        "kafka",
				Username:           "alice",
				KeyTabPath:         "/tmp/a.keytab",
				Realm:              "EXAMPLE.COM",
			},
		},
	}

	mechanism, err := buildSaslMechanism(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, "GSSAPI", mechanism.Name())
}
