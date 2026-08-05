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

func TestBuildSaslMechanismGSSAPI(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		authType   gssapiAuthType
		password   string
		keyTabPath string
	}{
		{name: "user", authType: userAuth, password: "pwd"},
		{name: "keytab", authType: keyTabAuth, keyTabPath: "/tmp/a.keytab"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := &options{sasl: &saslConfig{
				mechanism: gssapiMechanismName,
				gssapi: gssapiConfig{
					authType:           tc.authType,
					kerberosConfigPath: "/etc/krb5.conf",
					serviceName:        "kafka",
					username:           "alice",
					password:           tc.password,
					keyTabPath:         tc.keyTabPath,
					realm:              "EXAMPLE.COM",
				},
			}}

			mechanism, err := buildSaslMechanism(context.Background(), o)
			require.NoError(t, err)
			require.Equal(t, "GSSAPI", mechanism.Name())
		})
	}
}
