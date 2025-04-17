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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
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

// GetScheme returns the scheme of the url.
func GetScheme(url *url.URL) string {
	return strings.ToLower(url.Scheme)
}
