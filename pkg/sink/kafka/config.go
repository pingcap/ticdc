// Copyright 2023 PingCAP, Inc.
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
	"crypto/x509"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pkg/errors"
)

var (
	ProtocolDefault  = "plaintext"
	ProtocolSSL      = "ssl"
	ProtocolSASL     = "sasl_plaintext"
	ProtocolSASL_SSL = "sasl_ssl"
)

func NewConfig(options *Options) *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(options.BrokerEndpoints, ","),
		// "log_level",options.
	}
	if options.EnableTLS {
		_ = config.SetKey("security.protocol", ProtocolSSL)
		if options.Credential != nil && options.Credential.IsTLSEnabled() {
			_ = config.SetKey("ssl.ca.location", options.Credential.CAPath)
			_ = config.SetKey("ssl.certificate.location", options.Credential.CertPath)
			_ = config.SetKey("ssl.key.location", options.Credential.KeyPath)
			addVerifyPeerCertificate(config, options.Credential.CertAllowedCN)

		}
		_ = config.SetKey("enable.ssl.certificate.verification", !options.InsecureSkipVerify)
	}

	completeSASLConfig(config, options)
	return config
}

func completeSASLConfig(config *kafka.ConfigMap, o *Options) error {
	if o.SASL != nil && o.SASL.SASLMechanism != "" {
		if o.EnableTLS {
			_ = config.SetKey("security.protocol", ProtocolSASL_SSL)
		} else {
			_ = config.SetKey("security.protocol", ProtocolSASL)
		}
		_ = config.SetKey("sasl.mechanisms", o.SASL.SASLMechanism)
		switch o.SASL.SASLMechanism {
		case SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512, SASLTypePlaintext:
			_ = config.SetKey("sasl.username", o.SASL.SASLUser)
			_ = config.SetKey("sasl.password", o.SASL.SASLPassword)
		case SASLTypeGSSAPI:
			_ = config.SetKey("sasl.username", o.SASL.SASLUser)
			_ = config.SetKey("sasl.kerberos.service.name", o.SASL.GSSAPI.ServiceName)
			switch o.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				_ = config.SetKey("sasl.password", o.SASL.SASLPassword)
			case security.KeyTabAuth:
				_ = config.SetKey("sasl.kerberos.keytab", o.SASL.GSSAPI.KeyTabPath)
			}
		case SASLTypeOAuth:
			_ = config.SetKey("sasl.oauthbearer.token.endpoint.url", o.SASL.OAuth2.TokenURL)
		}
	}

	return nil
}

func addVerifyPeerCertificate(config *kafka.ConfigMap, verifyCN []string) {
	if len(verifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range verifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		verifyPeerCertificate := func(
			rawCerts [][]byte, verifiedChains [][]*x509.Certificate,
		) error {
			cns := make([]string, 0, len(verifiedChains))
			for _, chains := range verifiedChains {
				for _, chain := range chains {
					cns = append(cns, chain.Subject.CommonName)
					if _, match := checkCN[chain.Subject.CommonName]; match {
						return nil
					}
				}
			}
			return errors.Errorf("client certificate authentication failed. "+
				"The Common Name from the client certificate %v was not found "+
				"in the configuration cluster-verify-cn with value: %s", cns, verifyCN)
		}
		_ = config.SetKey("ssl.certificate.verify_cb", verifyPeerCertificate)
	}
}
