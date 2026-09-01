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
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

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

func TestGSSAPIRejectsMissingKerberosConfig(t *testing.T) {
	mechanism, err := buildGSSAPIMechanism(GSSAPIConfig{
		AuthType:           userAuth,
		KerberosConfigPath: "/path/that/does/not/exist",
		ServiceName:        "kafka",
		Username:           "alice",
		Password:           "secret",
		Realm:              "EXAMPLE.COM",
	})
	require.NoError(t, err)

	_, _, err = mechanism.Authenticate(context.Background(), "broker:9092")
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
}
