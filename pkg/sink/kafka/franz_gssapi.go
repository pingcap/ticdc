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
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

func buildGSSAPIMechanism(g gssapiConfig) (sasl.Mechanism, error) {
	if err := validateGSSAPIConfig(g); err != nil {
		return nil, err
	}

	krbClient, err := newKerberosClient(g)
	if err != nil {
		return nil, err
	}
	return kerberos.Auth{Client: krbClient, Service: g.serviceName}.AsMechanismWithClose(), nil
}

func validateGSSAPIConfig(g gssapiConfig) error {
	if g.serviceName == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-service-name must not be empty when sasl mechanism is GSSAPI")
	}
	if g.kerberosConfigPath == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-kerberos-config-path must not be empty when sasl mechanism is GSSAPI")
	}
	if g.username == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-user must not be empty when sasl mechanism is GSSAPI")
	}
	if g.realm == "" {
		return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-realm must not be empty when sasl mechanism is GSSAPI")
	}

	switch g.authType {
	case userAuth:
		if g.password == "" {
			return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-password must not be empty when sasl-gssapi-auth-type is USER")
		}
	case keyTabAuth:
		if g.keyTabPath == "" {
			return errors.ErrKafkaInvalidConfig.GenWithStack("sasl-gssapi-keytab-path must not be empty when sasl-gssapi-auth-type is KEYTAB")
		}
	default:
		return errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl-gssapi-auth-type %d", g.authType)
	}
	return nil
}

func newKerberosClient(g gssapiConfig) (*client.Client, error) {
	cfg, err := config.Load(g.kerberosConfigPath)
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	switch g.authType {
	case userAuth:
		return client.NewWithPassword(g.username, g.realm, g.password, cfg, client.DisablePAFXFAST(g.disablePAFXFAST)), nil
	case keyTabAuth:
		kt, err := keytab.Load(g.keyTabPath)
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
		return client.NewWithKeytab(g.username, g.realm, kt, cfg, client.DisablePAFXFAST(g.disablePAFXFAST)), nil
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported sasl-gssapi-auth-type %d", g.authType)
	}
}
