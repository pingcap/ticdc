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

package kafka

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestGSSAPIConfigValidation(t *testing.T) {
	valid := gssapiConfig{
		authType:           userAuth,
		kerberosConfigPath: "/etc/krb5.conf",
		serviceName:        "kafka",
		username:           "alice",
		password:           "secret",
		realm:              "EXAMPLE.COM",
	}

	for _, mutate := range []func(*gssapiConfig){
		func(cfg *gssapiConfig) { cfg.serviceName = "" },
		func(cfg *gssapiConfig) { cfg.kerberosConfigPath = "" },
		func(cfg *gssapiConfig) { cfg.username = "" },
		func(cfg *gssapiConfig) { cfg.realm = "" },
		func(cfg *gssapiConfig) { cfg.password = "" },
		func(cfg *gssapiConfig) { cfg.authType = 0 },
		func(cfg *gssapiConfig) { cfg.authType, cfg.keyTabPath = keyTabAuth, "" },
	} {
		cfg := valid
		mutate(&cfg)

		_, err := buildGSSAPIMechanism(cfg)
		require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	}
}

func TestGSSAPIMissingConfig(t *testing.T) {
	mechanism, err := buildGSSAPIMechanism(gssapiConfig{
		authType:           userAuth,
		kerberosConfigPath: "/path/that/does/not/exist",
		serviceName:        "kafka",
		username:           "alice",
		password:           "secret",
		realm:              "EXAMPLE.COM",
	})
	require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
	require.Nil(t, mechanism)
}
