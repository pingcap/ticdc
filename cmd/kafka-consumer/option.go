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

package main

import (
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type option struct {
	address      []string
	version      string
	topic        string
	partitionNum int32
	groupID      string

	maxMessageBytes int
	maxBatchSize    int

	protocol config.Protocol

	codecConfig *common.Config
	sinkConfig  *config.SinkConfig

	timezone      string
	ca, cert, key string

	downstreamURI string

	// avro schema registry uri should be set if the encoding protocol is avro
	schemaRegistryURI string

	// upstreamTiDBDSN is the dsn of the upstream TiDB cluster
	upstreamTiDBDSN string
}

func newOption() *option {
	return &option{
		version:         "2.4.0",
		maxMessageBytes: math.MaxInt64,
		maxBatchSize:    math.MaxInt64,
	}
}

// Adjust the consumer option by the upstream uri passed in parameters.
func (o *option) Adjust(upstreamURIStr string, configFile string) error {
	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`",
			zap.String("upstreamURI", upstreamURIStr))
	}

	s := upstreamURI.Query().Get("version")
	if s != "" {
		o.version = s
	}
	o.topic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	o.address = strings.Split(upstreamURI.Host, ",")

	s = upstreamURI.Query().Get("partition-num")
	if s != "" {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.Panic("invalid partition-num of upstream-uri")
		}
		o.partitionNum = int32(c)
	}

	s = upstreamURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-message-bytes of upstream-uri")
		}
		o.maxMessageBytes = c
	}

	s = upstreamURI.Query().Get("max-batch-size")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-batch-size of upstream-uri")
		}
		o.maxBatchSize = c
	}

	s = upstreamURI.Query().Get("protocol")
	if s == "" {
		log.Panic("cannot found the protocol from the sink url")
	}
	protocol, err := config.ParseSinkProtocolFromString(s)
	if err != nil {
		log.Panic("invalid protocol", zap.String("protocol", s), zap.Error(err))
	}
	o.protocol = protocol

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		err = util.StrictDecodeFile(configFile, "kafka consumer", replicaConfig)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err = filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			return errors.Trace(err)
		}
	}
	// the TiDB source ID should never be set to 0
	replicaConfig.Sink.TiDBSourceID = 1
	replicaConfig.Sink.Protocol = putil.AddressOf(protocol.String())

	o.codecConfig = common.NewConfig(protocol)
	if err = o.codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return errors.Trace(err)
	}
	tz, err := putil.GetTimezone(o.timezone)
	if err != nil {
		return errors.Trace(err)
	}
	o.codecConfig.TimeZone = tz

	if protocol == config.ProtocolAvro {
		o.codecConfig.AvroEnableWatermark = true
	}

	log.Info("consumer option adjusted",
		zap.String("configFile", configFile),
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("version", o.version),
		zap.String("topic", o.topic),
		zap.Int32("partitionNum", o.partitionNum),
		zap.String("groupID", o.groupID),
		zap.Int("maxMessageBytes", o.maxMessageBytes),
		zap.Int("maxBatchSize", o.maxBatchSize),
		zap.String("upstreamURI", upstreamURI.String()),
		zap.String("downstreamURI", o.downstreamURI))
	return nil
}
