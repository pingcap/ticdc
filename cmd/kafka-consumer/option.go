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
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type option struct {
	// reader options
	address       []string
	topic         string
	groupID       string
	ca, cert, key string

	// writer options
	protocol        config.Protocol
	partitionNum    int32
	maxMessageBytes int
	maxBatchSize    int

	codecConfig *common.Config
	sinkConfig  *config.SinkConfig

	timezone string

	// downstreamURI specifies the URI for the downstream MySQL
	downstreamURI string

	// avro schema registry uri should be set if the encoding protocol is avro
	schemaRegistryURI string

	// upstreamTiDBDSN is the dsn of the upstream TiDB cluster
	upstreamTiDBDSN string

	enableTableAcrossNodes bool
}

func newOption() *option {
	return &option{
		maxMessageBytes: math.MaxInt64,
		maxBatchSize:    math.MaxInt64,
	}
}

// Adjust the consumer option by the upstream uri passed in parameters.
func (o *option) Adjust(upstreamURIStr string, configFile string) {
	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`",
			zap.String("upstreamURI", upstreamURIStr))
	}

	o.topic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	if len(o.topic) == 0 {
		log.Panic("no topic provided for the consumer")
	}

	o.address = strings.Split(upstreamURI.Host, ",")

	s := upstreamURI.Query().Get("max-message-bytes")
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

	s = upstreamURI.Query().Get("partition-num")
	if s != "" {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.Panic("invalid partition-num of upstream-uri")
		}
		o.partitionNum = int32(c)
	}
	partitionNum, err := getPartitionNum(o)
	if err != nil {
		log.Panic("cannot get the partition number", zap.String("topic", o.topic), zap.Error(err))
	}
	if o.partitionNum == 0 {
		o.partitionNum = partitionNum
	}

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		err = util.StrictDecodeFile(configFile, "kafka consumer", replicaConfig)
		if err != nil {
			log.Panic("decode config file failed", zap.String("configFile", configFile), zap.Error(err))
		}
		if _, err = filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			log.Panic("verify table rules failed", zap.Error(err))
		}
	}
	// the TiDB source ID should never be set to 0
	replicaConfig.Sink.TiDBSourceID = 1
	replicaConfig.Sink.Protocol = putil.AddressOf(protocol.String())
	o.sinkConfig = replicaConfig.Sink

	o.codecConfig = common.NewConfig(protocol)
	if err = o.codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		log.Panic("codec config apply failed", zap.Error(err))
	}
	o.codecConfig.AvroConfluentSchemaRegistry = o.schemaRegistryURI

	tz, err := putil.GetTimezone(o.timezone)
	if err != nil {
		log.Panic("parse timezone failed", zap.Error(err))
	}
	o.codecConfig.TimeZone = tz

	if protocol == config.ProtocolAvro {
		o.codecConfig.AvroEnableWatermark = true
	}
	o.enableTableAcrossNodes = putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes)

	log.Info("consumer option adjusted",
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("topic", o.topic),
		zap.Int32("partitionNum", o.partitionNum),
		zap.String("protocol", protocol.String()),
		zap.String("schemaRegistryURL", o.schemaRegistryURI),
		zap.String("groupID", o.groupID),
		zap.Int("maxMessageBytes", o.maxMessageBytes),
		zap.Int("maxBatchSize", o.maxBatchSize),
		zap.String("configFile", configFile),
		zap.String("upstreamURI", upstreamURI.String()),
		zap.String("downstreamURI", o.downstreamURI))
}
