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

package main

import (
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	cmdutil "github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

func parseRequiredURI(rawURI string) *url.URL {
	if rawURI == "" {
		log.Panic("consumer URI is required", zap.String("rawURI", rawURI))
	}
	uri, err := url.Parse(rawURI)
	if err != nil {
		log.Panic("consumer URI parsing failed", zap.String("rawURI", rawURI), zap.Error(err))
	}
	uri.Scheme = strings.ToLower(uri.Scheme)
	if uri.Scheme == "" {
		log.Panic("consumer URI must include a scheme", zap.String("rawURI", rawURI))
	}
	return uri
}

func logConsumerConfig(
	sourceType source.Type,
	consumerID string,
	upstreamURI *url.URL,
	downstreamURI string,
	replicaConfig *config.ReplicaConfig,
	codecConfig *common.Config,
) {
	fields := []zap.Field{
		zap.String("sourceType", string(sourceType)),
		zap.String("consumerID", consumerID),
		zap.String("upstreamURI", util.MaskSensitiveDataInURI(upstreamURI.String())),
		zap.String("downstreamURI", util.MaskSensitiveDataInURI(downstreamURI)),
		zap.String("protocol", codecConfig.Protocol.String()),
		zap.String("timeZone", codecConfig.TimeZone.String()),
		zap.Bool("caseSensitive", util.GetOrZero(replicaConfig.CaseSensitive)),
		zap.Bool("enableTiDBExtension", codecConfig.EnableTiDBExtension),
		zap.Bool("avroEnableWatermark", codecConfig.AvroEnableWatermark),
	}
	if codecConfig.AvroConfluentSchemaRegistry != "" {
		fields = append(fields,
			zap.String("schemaRegistry", util.MaskSensitiveDataInURI(codecConfig.AvroConfluentSchemaRegistry)))
	}
	switch sourceType {
	case source.Kafka:
		fields = append(fields,
			zap.String("topic", strings.Trim(upstreamURI.Path, "/")),
			zap.Int("maxMessageBytes", codecConfig.MaxMessageBytes),
			zap.Int("maxBatchSize", codecConfig.MaxBatchSize))
	case source.Pulsar:
		fields = append(fields, zap.String("topic", strings.Trim(upstreamURI.Path, "/")))
	case source.Storage:
		fields = append(fields,
			zap.Int("fileIndexWidth", util.GetOrZero(replicaConfig.Sink.FileIndexWidth)),
			zap.Any("dateSeparator", util.GetOrZero(replicaConfig.Sink.DateSeparator)))
		if codecConfig.Protocol == config.ProtocolCsv {
			fields = append(fields,
				zap.String("csvDelimiter", codecConfig.Delimiter),
				zap.String("csvTerminator", codecConfig.Terminator),
				zap.String("csvNull", codecConfig.NullString),
				zap.Bool("csvIncludeCommitTs", codecConfig.IncludeCommitTs),
				zap.Bool("csvOutputOldValue", codecConfig.OutputOldValue))
		}
	}
	log.Info("consumer configuration", fields...)
}

func loadReplicaConfig(configFile string, upstreamURI *url.URL) *config.ReplicaConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		if err := cmdutil.StrictDecodeFile(configFile, "consumer", replicaConfig); err != nil {
			log.Panic("consumer configuration loading failed", zap.String("configFile", configFile), zap.Error(err))
		}
		if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			log.Panic("consumer filter rules are invalid", zap.Error(err))
		}
	}
	if err := replicaConfig.ValidateAndAdjust(upstreamURI); err != nil {
		log.Panic("consumer configuration is invalid", zap.Error(err))
	}
	return replicaConfig
}

// newCodecConfig builds the decoder config. loadReplicaConfig has already
// applied the sink URI parameters, including protocol, to replicaConfig.Sink.
func newCodecConfig(upstreamURI *url.URL, replicaConfig *config.ReplicaConfig) *common.Config {
	protocol, err := config.ParseSinkProtocolFromString(util.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		log.Panic("consumer protocol is invalid", zap.Error(err))
	}
	codecConfig := common.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		log.Panic("consumer codec configuration is invalid", zap.Error(err))
	}
	if replicaConfig.Integrity != nil {
		codecConfig.EnableRowChecksum = replicaConfig.Integrity.Enabled()
	}

	// The upstream encoder only emits commit-ts, DDL and resolved events when
	// these flags are enabled; the decoder cannot reconstruct them.
	switch protocol {
	case config.ProtocolCanalJSON, config.ProtocolDebezium:
		if !codecConfig.EnableTiDBExtension {
			log.Panic("consumer requires TiDB protocol extensions", zap.String("protocol", protocol.String()))
		}
	case config.ProtocolAvro, config.ProtocolDebeziumAvro:
		if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
			log.Panic("consumer requires TiDB protocol extensions and Avro watermarks", zap.String("protocol", protocol.String()))
		}
		if codecConfig.AvroConfluentSchemaRegistry == "" {
			log.Panic("consumer requires an Avro schema registry")
		}
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		log.Panic("consumer requires Debezium schemas")
	}
	return codecConfig
}
