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

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

const (
	// Authentication type values are part of sink URI compatibility and must remain stable.
	userAuth   = 1
	keyTabAuth = 2
)

func buildGSSAPIMechanism(g GSSAPIConfig) (sasl.Mechanism, error) {
	if err := validateGSSAPIConfig(g); err != nil {
		return nil, err
	}

	return kerberos.Kerberos(func(context.Context) (kerberos.Auth, error) {
		krbClient, err := newKerberosClient(g)
		if err != nil {
			return kerberos.Auth{}, err
		}
		return kerberos.Auth{Client: krbClient, Service: g.ServiceName}, nil
	}), nil
}

func validateGSSAPIConfig(g GSSAPIConfig) error {
	if g.ServiceName == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-service-name must not be empty when sasl mechanism is GSSAPI")
	}
	if g.KerberosConfigPath == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-kerberos-config-path must not be empty when sasl mechanism is GSSAPI")
	}
	if g.Username == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-user must not be empty when sasl mechanism is GSSAPI")
	}
	if g.Realm == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"sasl-gssapi-realm must not be empty when sasl mechanism is GSSAPI")
	}

	switch g.AuthType {
	case userAuth:
		if g.Password == "" {
			return errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-password must not be empty when sasl-gssapi-auth-type is USER")
		}
	case keyTabAuth:
		if g.KeyTabPath == "" {
			return errors.ErrKafkaInvalidConfig.GenWithStack(
				"sasl-gssapi-keytab-path must not be empty when sasl-gssapi-auth-type is KEYTAB")
		}
	default:
		return errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}
	return nil
}

func newKerberosClient(g GSSAPIConfig) (*client.Client, error) {
	cfg, err := config.Load(g.KerberosConfigPath)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	switch g.AuthType {
	case userAuth:
		return client.NewWithPassword(
			g.Username, g.Realm, g.Password, cfg, client.DisablePAFXFAST(g.DisablePAFXFAST)), nil
	case keyTabAuth:
		kt, err := keytab.Load(g.KeyTabPath)
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
		return client.NewWithKeytab(
			g.Username, g.Realm, kt, cfg, client.DisablePAFXFAST(g.DisablePAFXFAST)), nil
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
			"unsupported sasl-gssapi-auth-type %d", g.AuthType)
	}
}
