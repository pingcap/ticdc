// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"net/url"
	"strings"

	"github.com/pingcap/ticdc/pkg/errors"
)

const (
	// ProtocolKey specifies the key of the protocol in the SinkURI.
	ProtocolKey = "protocol"
)

// Protocol is the protocol of the message.
type Protocol int

// Enum types of the Protocol.
const (
	ProtocolUnknown Protocol = iota
	ProtocolDefault
	ProtocolCanal
	ProtocolAvro
	ProtocolMaxwell
	ProtocolCanalJSON
	ProtocolCraft
	ProtocolOpen
	ProtocolCsv
	ProtocolDebezium
	ProtocolSimple
)

// IsBatchEncode returns whether the protocol is a batch encoder.
func (p Protocol) IsBatchEncode() bool {
	return p == ProtocolOpen || p == ProtocolCanal || p == ProtocolMaxwell || p == ProtocolCraft
}

// ParseSinkProtocolFromString converts the protocol from string to Protocol enum type.
func ParseSinkProtocolFromString(protocol string) (Protocol, error) {
	switch strings.ToLower(protocol) {
	case "default":
		return ProtocolOpen, nil
	case "canal":
		return ProtocolCanal, nil
	case "avro":
		return ProtocolAvro, nil
	case "flat-avro":
		return ProtocolAvro, nil
	case "maxwell":
		return ProtocolMaxwell, nil
	case "canal-json":
		return ProtocolCanalJSON, nil
	case "craft":
		return ProtocolCraft, nil
	case "open-protocol":
		return ProtocolOpen, nil
	case "csv":
		return ProtocolCsv, nil
	case "debezium":
		return ProtocolDebezium, nil
	case "simple":
		return ProtocolSimple, nil
	default:
		return ProtocolUnknown, errors.ErrSinkUnknownProtocol.GenWithStackByArgs(protocol)
	}
}

// String converts the Protocol enum type string to string.
func (p Protocol) String() string {
	switch p {
	case ProtocolDefault:
		return "default"
	case ProtocolCanal:
		return "canal"
	case ProtocolAvro:
		return "avro"
	case ProtocolMaxwell:
		return "maxwell"
	case ProtocolCanalJSON:
		return "canal-json"
	case ProtocolCraft:
		return "craft"
	case ProtocolOpen:
		return "open-protocol"
	case ProtocolCsv:
		return "csv"
	case ProtocolDebezium:
		return "debezium"
	case ProtocolSimple:
		return "simple"
	default:
		panic("unreachable")
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

// IsPulsarSupportedProtocols returns whether the protocol is supported by pulsar.
func IsPulsarSupportedProtocols(p Protocol) bool {
	return p == ProtocolCanalJSON
}
