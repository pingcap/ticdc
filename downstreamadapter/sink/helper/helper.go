// Copyright 2022 PingCAP, Inc.
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

package helper

import (
	"net/url"
	"strings"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
)

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero DDLDispatchRule = iota
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll
)

func GetDDLDispatchRule(protocol config.Protocol) DDLDispatchRule {
	switch protocol {
	case config.ProtocolCanal, config.ProtocolCanalJSON:
		return PartitionZero
	default:
	}
	return PartitionAll
}

// GetEncoderConfig returns the encoder config and validates the config.
func GetEncoderConfig(
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	protocol config.Protocol,
	sinkConfig *config.SinkConfig,
	maxMsgBytes int,
) (*common.Config, error) {
	encoderConfig := common.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, sinkConfig); err != nil {
		return nil, errors.WrapError(errors.ErrSinkInvalidConfig, err)
	}
	// Always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large
	// then cause producer meet `message too large`.
	encoderConfig = encoderConfig.
		WithMaxMessageBytes(maxMsgBytes).
		WithChangefeedID(changefeedID)

	tz, err := util.GetTimezone(config.GetGlobalServerConfig().TZ)
	if err != nil {
		return nil, errors.WrapError(errors.ErrSinkInvalidConfig, err)
	}
	encoderConfig.TimeZone = tz

	if err = encoderConfig.Validate(); err != nil {
		return nil, errors.WrapError(errors.ErrSinkInvalidConfig, err)
	}

	return encoderConfig, nil
}

// GetTopic returns the topic name from the sink URI.
func GetTopic(sinkURI *url.URL) (string, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return "", errors.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}
	return topic, nil
}

// GetProtocol returns the protocol from the sink URI.
func GetProtocol(protocolStr string) (config.Protocol, error) {
	protocol, err := config.ParseSinkProtocolFromString(protocolStr)
	if err != nil {
		return protocol, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	return protocol, nil
}

// GetFileExtension returns the extension for specific protocol
func GetFileExtension(protocol config.Protocol) string {
	switch protocol {
	case config.ProtocolAvro, config.ProtocolCanalJSON, config.ProtocolMaxwell,
		config.ProtocolOpen, config.ProtocolSimple:
		return ".json"
	case config.ProtocolCraft:
		return ".craft"
	case config.ProtocolCanal:
		return ".canal"
	case config.ProtocolCsv:
		return ".csv"
	default:
		return ".unknown"
	}
}
