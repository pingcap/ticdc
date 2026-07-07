// Copyright 2020 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
)

// saslMechanism defines SASL mechanism.
type saslMechanism string

// The mechanisms we currently support.
const (
	// unknownMechanism means the SASL mechanism is unknown.
	unknownMechanism saslMechanism = ""
	// plainMechanism means the SASL mechanism is plain.
	plainMechanism saslMechanism = "PLAIN"
	// scram256Mechanism means the SASL mechanism is SCRAM-SHA-256.
	scram256Mechanism saslMechanism = "SCRAM-SHA-256"
	// scram512Mechanism means the SASL mechanism is SCRAM-SHA-512.
	scram512Mechanism saslMechanism = "SCRAM-SHA-512"
	// gssapiMechanismName means the SASL mechanism is GSSAPI.
	gssapiMechanismName saslMechanism = "GSSAPI"
	// oauthMechanism means the SASL mechanism is OAUTHBEARER.
	oauthMechanism saslMechanism = "OAUTHBEARER"
)

// saslMechanismFromString converts the string to a SASL mechanism.
func saslMechanismFromString(s string) (saslMechanism, error) {
	switch strings.ToLower(s) {
	case "plain":
		return plainMechanism, nil
	case "scram-sha-256":
		return scram256Mechanism, nil
	case "scram-sha-512":
		return scram512Mechanism, nil
	case "gssapi":
		return gssapiMechanismName, nil
	case "oauthbearer":
		return oauthMechanism, nil
	default:
		return unknownMechanism, errors.Errorf("unknown %s SASL mechanism", s)
	}
}

// saslConfig holds necessary path parameter to support sasl-scram
type saslConfig struct {
	user      string
	password  string
	mechanism saslMechanism
	gssapi    gssapiConfig
	oauth2    oauth2Config
}

// oauth2Config holds necessary parameters to support sasl-oauth2.
type oauth2Config struct {
	clientID     string
	clientSecret string
	tokenURL     string
	scopes       []string
	grantType    string
	audience     string
}

// validate validates the parameters of oauth2Config.
// Some parameters are required, some are optional.
func (o *oauth2Config) validate() error {
	if len(o.clientID) == 0 {
		return errors.New("OAuth2 client id is empty")
	}
	if len(o.clientSecret) == 0 {
		return errors.New("OAuth2 client secret is empty")
	}
	if len(o.tokenURL) == 0 {
		return errors.New("OAuth2 token url is empty")
	}
	return nil
}

// gssapiAuthType defines the type of GSSAPI authentication.
type gssapiAuthType int

const (
	// unknownAuth means the auth type is unknown.
	unknownAuth gssapiAuthType = 0
	// userAuth means the auth type is user.
	userAuth gssapiAuthType = 1
	// keyTabAuth means the auth type is keytab.
	keyTabAuth gssapiAuthType = 2
)

// gssapiAuthTypeFromString convent the string to gssapiAuthType.
func gssapiAuthTypeFromString(s string) (gssapiAuthType, error) {
	switch strings.ToLower(s) {
	case "user":
		return userAuth, nil
	case "keytab":
		return keyTabAuth, nil
	default:
		return unknownAuth, errors.Errorf("unknown %s auth type", s)
	}
}

// gssapiConfig holds necessary path parameter to support sasl-gssapi.
type gssapiConfig struct {
	authType           gssapiAuthType
	keyTabPath         string
	kerberosConfigPath string
	serviceName        string
	username           string
	password           string
	realm              string
	disablePAFXFAST    bool
}
