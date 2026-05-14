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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type option struct {
	address       []string
	topic         string
	groupID       string
	ca, cert, key string

	protocol        config.Protocol
	partitionNum    int32
	maxMessageBytes int
	maxBatchSize    int

	codecConfig *codeccommon.Config
	sinkConfig  *config.SinkConfig

	timezone string

	downstreamURI     string
	schemaRegistryURI string
	upstreamTiDBDSN   string
	generatedGroupID  string

	enableSyncpoint       bool
	syncpointInterval     time.Duration
	syncpointRetention    time.Duration
	enableSyncpointSet    bool
	syncpointIntervalSet  bool
	syncpointRetentionSet bool
	downstreamURIParsed   *url.URL

	enableTableAcrossNodes bool
}

func newOption() *option {
	return &option{
		maxMessageBytes: math.MaxInt64,
		maxBatchSize:    math.MaxInt64,
	}
}

func (o *option) adjust(upstreamURIStr string, configFile string) error {
	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		return errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	if strings.ToLower(upstreamURI.Scheme) != "kafka" {
		return errors.Errorf("invalid upstream-uri scheme %q, expected kafka", upstreamURI.Scheme)
	}

	o.topic = strings.Trim(upstreamURI.Path, "/")
	if o.topic == "" {
		return errors.New("no topic provided for the consumer")
	}
	o.address = strings.Split(upstreamURI.Host, ",")

	if s := upstreamURI.Query().Get("max-message-bytes"); s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return errors.Annotate(err, "invalid max-message-bytes of upstream-uri")
		}
		o.maxMessageBytes = c
	}

	if s := upstreamURI.Query().Get("max-batch-size"); s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return errors.Annotate(err, "invalid max-batch-size of upstream-uri")
		}
		o.maxBatchSize = c
	}

	s := upstreamURI.Query().Get("protocol")
	if s == "" {
		return errors.New("cannot find protocol from upstream-uri")
	}
	protocol, err := config.ParseSinkProtocolFromString(s)
	if err != nil {
		return errors.Trace(err)
	}
	o.protocol = protocol

	if s = upstreamURI.Query().Get("partition-num"); s != "" {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return errors.Annotate(err, "invalid partition-num of upstream-uri")
		}
		o.partitionNum = int32(c)
	}
	partitionNum, err := getPartitionNum(o)
	if err != nil {
		return errors.Trace(err)
	}
	if o.partitionNum == 0 {
		o.partitionNum = partitionNum
	}

	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		if err = util.StrictDecodeFile(configFile, "kafka consumer v2", replicaConfig); err != nil {
			return errors.Trace(err)
		}
		if _, err = filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			return errors.Trace(err)
		}
	}
	replicaConfig.Sink.TiDBSourceID = 1
	replicaConfig.Sink.Protocol = putil.AddressOf(protocol.String())
	o.sinkConfig = replicaConfig.Sink

	o.codecConfig = codeccommon.NewConfig(protocol)
	if err = o.codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return errors.Trace(err)
	}
	o.codecConfig.AvroConfluentSchemaRegistry = o.schemaRegistryURI

	tz, err := putil.GetTimezone(o.timezone)
	if err != nil {
		return errors.Trace(err)
	}
	o.codecConfig.TimeZone = tz
	if protocol == config.ProtocolAvro {
		o.codecConfig.AvroEnableWatermark = true
	}

	downstreamURI, err := url.Parse(o.downstreamURI)
	if err != nil {
		return errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	o.downstreamURIParsed = downstreamURI
	o.applySyncpointConfig(replicaConfig)
	if o.enableSyncpoint {
		if o.groupID == "" || o.groupID == o.generatedGroupID {
			return errors.New("consumer-group-id must be explicitly set when syncpoint is enabled")
		}
		if o.syncpointInterval <= 0 {
			return errors.New("sync-point-interval must be larger than 0 when syncpoint is enabled")
		}
		if o.syncpointRetention <= 0 {
			return errors.New("sync-point-retention must be larger than 0 when syncpoint is enabled")
		}
		if !config.IsMySQLCompatibleScheme(config.GetScheme(downstreamURI)) {
			return errors.New("syncpoint requires a MySQL/TiDB compatible downstream URI")
		}
		if protocol == config.ProtocolCanalJSON && !o.codecConfig.EnableTiDBExtension {
			return errors.New("canal-json syncpoint requires enable-tidb-extension=true")
		}
	}

	o.enableTableAcrossNodes = putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes)

	log.Info("kafka consumer v2 option adjusted",
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("topic", o.topic),
		zap.Int32("partitionNum", o.partitionNum),
		zap.String("protocol", protocol.String()),
		zap.String("schemaRegistryURL", o.schemaRegistryURI),
		zap.String("groupID", o.groupID),
		zap.Int("maxMessageBytes", o.maxMessageBytes),
		zap.Int("maxBatchSize", o.maxBatchSize),
		zap.Bool("enableSyncpoint", o.enableSyncpoint),
		zap.Duration("syncpointInterval", o.syncpointInterval),
		zap.Duration("syncpointRetention", o.syncpointRetention),
		zap.String("configFile", configFile),
		zap.String("upstreamURI", upstreamURI.String()),
		zap.String("downstreamURI", o.downstreamURI))
	return nil
}

func (o *option) applySyncpointConfig(replicaConfig *config.ReplicaConfig) {
	enableSyncpoint := putil.GetOrZero(replicaConfig.EnableSyncPoint)
	syncpointInterval := putil.GetOrZero(replicaConfig.SyncPointInterval)
	syncpointRetention := putil.GetOrZero(replicaConfig.SyncPointRetention)

	if o.enableSyncpointSet {
		enableSyncpoint = o.enableSyncpoint
	}
	if o.syncpointIntervalSet {
		syncpointInterval = o.syncpointInterval
	}
	if o.syncpointRetentionSet {
		syncpointRetention = o.syncpointRetention
	}

	o.enableSyncpoint = enableSyncpoint
	o.syncpointInterval = syncpointInterval
	o.syncpointRetention = syncpointRetention
}
