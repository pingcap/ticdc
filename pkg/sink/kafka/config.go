// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	ProtocolDefault  = "plaintext"
	ProtocolSSL      = "ssl"
	ProtocolSASL     = "sasl_plaintext"
	ProtocolSASL_SSL = "sasl_ssl"
)

func NewConfig(options *Options) *kafka.ConfigMap {
	config := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(options.BrokerEndpoints, ","),
		"allow.auto.create.topics": options.AutoCreate,
		// retries may cause reordering unless enable.idempotence is set to true.
		"message.send.max.retries":     0,
		"max.in.flight":                1,
		"request.required.acks":        int(options.RequiredAcks),
		"queue.buffering.max.ms":       time.Duration(0),
		"queue.buffering.max.messages": options.MaxMessages,
		"message.max.bytes":            options.MaxMessageBytes,
		"socket.timeout.ms":            int(options.DialTimeout.Milliseconds()),
		"log_level":                    getLogLevel(),
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
	compression := strings.ToLower(strings.TrimSpace(options.Compression))
	config.SetKey("compression.codec", compression)
	log.Info("kafka producer config", zap.Any("config", config))
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
			switch o.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				_ = config.SetKey("sasl.username", o.SASL.SASLUser)
				_ = config.SetKey("sasl.password", o.SASL.SASLPassword)
			case security.KeyTabAuth:
				_ = config.SetKey("sasl.kerberos.service.name", o.SASL.GSSAPI.ServiceName)
				_ = config.SetKey("sasl.kerberos.keytab", o.SASL.GSSAPI.KeyTabPath)
				_ = config.SetKey("sasl.kerberos.principal", fmt.Sprintf("%s@%s", o.SASL.GSSAPI.ServiceName, o.SASL.GSSAPI.Realm))
			}
		case SASLTypeOAuth:
			_ = config.SetKey("sasl.oauthbearer.token.endpoint.url", o.SASL.OAuth2.TokenURL)
			_ = config.SetKey("sasl.oauthbearer.client.id", o.SASL.OAuth2.ClientID)
			_ = config.SetKey("sasl.oauthbearer.client.secret", o.SASL.OAuth2.ClientSecret)
			_ = config.SetKey("sasl.oauthbearer.scope", o.SASL.OAuth2.Scopes)
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

func getLogLevel() int {
	switch log.GetLevel() {
	case zapcore.DebugLevel:
		return 7
	case zapcore.InfoLevel:
		return 6
	case zapcore.WarnLevel:
		return 4
	case zapcore.ErrorLevel:
		return 3
	case zapcore.DPanicLevel:
		return 2
	case zapcore.FatalLevel:
		return 0
	default:
		return 6
	}
}
