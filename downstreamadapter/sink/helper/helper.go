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

const (
	// KafkaScheme indicates the scheme is kafka.
	KafkaScheme = "kafka"
	// KafkaSSLScheme indicates the scheme is kafka+ssl.
	KafkaSSLScheme = "kafka+ssl"
	// BlackHoleScheme indicates the scheme is blackhole.
	BlackHoleScheme = "blackhole"
	// MySQLScheme indicates the scheme is MySQL.
	MySQLScheme = "mysql"
	// MySQLSSLScheme indicates the scheme is MySQL+ssl.
	MySQLSSLScheme = "mysql+ssl"
	// TiDBScheme indicates the scheme is TiDB.
	TiDBScheme = "tidb"
	// TiDBSSLScheme indicates the scheme is TiDB+ssl.
	TiDBSSLScheme = "tidb+ssl"
	// S3Scheme indicates the scheme is s3.
	S3Scheme = "s3"
	// FileScheme indicates the scheme is local fs or NFS.
	FileScheme = "file"
	// GCSScheme indicates the scheme is gcs.
	GCSScheme = "gcs"
	// GSScheme is an alias for "gcs"
	GSScheme = "gs"
	// AzblobScheme indicates the scheme is azure blob storage.\
	AzblobScheme = "azblob"
	// AzureScheme is an alias for "azblob"
	AzureScheme = "azure"
	// CloudStorageNoopScheme indicates the scheme is noop.
	CloudStorageNoopScheme = "noop"
	// PulsarScheme  indicates the scheme is pulsar
	PulsarScheme = "pulsar"
	// PulsarSSLScheme indicates the scheme is pulsar+ssl
	PulsarSSLScheme = "pulsar+ssl"
	// PulsarHTTPScheme indicates the schema is pulsar with http protocol
	PulsarHTTPScheme = "pulsar+http"
	// PulsarHTTPSScheme indicates the schema is pulsar with https protocol
	PulsarHTTPSScheme = "pulsar+https"
)

// IsMQScheme returns true if the scheme belong to mq scheme.
func IsMQScheme(scheme string) bool {
	return scheme == KafkaScheme || scheme == KafkaSSLScheme ||
		scheme == PulsarScheme || scheme == PulsarSSLScheme || scheme == PulsarHTTPScheme || scheme == PulsarHTTPSScheme
}

// IsMySQLCompatibleScheme returns true if the scheme is compatible with MySQL.
func IsMySQLCompatibleScheme(scheme string) bool {
	return scheme == MySQLScheme || scheme == MySQLSSLScheme ||
		scheme == TiDBScheme || scheme == TiDBSSLScheme
}

// IsStorageScheme returns true if the scheme belong to storage scheme.
func IsStorageScheme(scheme string) bool {
	return scheme == FileScheme || scheme == S3Scheme || scheme == GCSScheme ||
		scheme == GSScheme || scheme == AzblobScheme || scheme == AzureScheme || scheme == CloudStorageNoopScheme
}

// IsPulsarScheme returns true if the scheme belong to pulsar scheme.
func IsPulsarScheme(scheme string) bool {
	return scheme == PulsarScheme || scheme == PulsarSSLScheme || scheme == PulsarHTTPScheme || scheme == PulsarHTTPSScheme
}

// IsBlackHoleScheme returns true if the scheme belong to blackhole scheme.
func IsBlackHoleScheme(scheme string) bool {
	return scheme == BlackHoleScheme
}

// GetScheme returns the scheme of the url.
func GetScheme(url *url.URL) string {
	return strings.ToLower(url.Scheme)
}
