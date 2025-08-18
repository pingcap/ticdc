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

package main

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

// option represents the options of the pulsar consumer
type option struct {
	address []string
	topic   string

	protocol            config.Protocol
	enableTiDBExtension bool

	// the replicaConfig of the changefeed which produce data to the topic
	replicaConfig *config.ReplicaConfig

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string

	oauth2PrivateKey string
	oauth2IssuerURL  string
	oauth2ClientID   string
	oauth2Scope      string
	oauth2Audience   string

	mtlsAuthTLSCertificatePath string
	mtlsAuthTLSPrivateKeyPath  string

	downstreamURI string
	partitionNum  int
}

func newConsumerOption() *option {
	return &option{
		protocol: config.ProtocolCanalJSON,
		// the default value of partitionNum is 1
		partitionNum: 1,
	}
}

// Adjust the consumer option by the upstream uri passed in parameters.
func (o *option) Adjust(upstreamURI *url.URL, configFile string) {
	o.topic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	o.address = strings.Split(upstreamURI.Host, ",")

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		err := util.StrictDecodeFile(configFile, "pulsar consumer", replicaConfig)
		if err != nil {
			log.Panic("decode config file failed", zap.Error(err))
		}
	}
	o.replicaConfig = replicaConfig

	s := upstreamURI.Query().Get("protocol")
	if s != "" {
		protocol, err := config.ParseSinkProtocolFromString(s)
		if err != nil {
			log.Panic("invalid protocol", zap.Error(err), zap.String("protocol", s))
		}
		o.protocol = protocol
	}
	if !config.IsPulsarSupportedProtocols(o.protocol) {
		log.Panic("unsupported protocol, pulsar sink currently only support these protocols: [canal-json]",
			zap.String("protocol", s))
	}

	s = upstreamURI.Query().Get("enable-tidb-extension")
	if s != "" {
		enableTiDBExtension, err := strconv.ParseBool(s)
		if err != nil {
			log.Panic("invalid enable-tidb-extension of upstream-uri")
		}
		o.enableTiDBExtension = enableTiDBExtension
	}

	log.Info("consumer option adjusted",
		zap.String("configFile", configFile),
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("topic", o.topic),
		zap.Any("protocol", o.protocol),
		zap.Bool("enableTiDBExtension", o.enableTiDBExtension))
}
