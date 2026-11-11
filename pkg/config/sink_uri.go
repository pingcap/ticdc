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

package config

import (
	"net/url"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// ParseSinkURI returns the sink implementation type and downstream system type
// derived from the URI scheme.
// Unrecognized schemes use BlackHoleSinkType and retain their scheme as the
// downstream type. Empty schemes use "unknown" as the downstream type.
// It panics with sanitized URI details if the URI cannot be parsed.
func ParseSinkURI(sinkURI string) (sinkType common.SinkType, downstreamType string) {
	parsed, err := url.Parse(sinkURI)
	if err != nil {
		log.Panic("unable to parse sink uri",
			zap.String("url", util.MaskSensitiveDataInURIForError(sinkURI)),
			zap.Error(util.MaskSensitiveDataInURLError(err)))
	}

	scheme := GetScheme(parsed)
	switch scheme {
	case MySQLScheme, MySQLSSLScheme:
		return common.MysqlSinkType, MySQLScheme
	case TiDBScheme, TiDBSSLScheme:
		return common.MysqlSinkType, TiDBScheme
	case KafkaScheme, KafkaSSLScheme:
		return common.KafkaSinkType, KafkaScheme
	case PulsarScheme, PulsarSSLScheme, PulsarHTTPScheme, PulsarHTTPSScheme:
		return common.PulsarSinkType, PulsarScheme
	case FileScheme, S3Scheme, CloudStorageNoopScheme:
		return common.CloudStorageSinkType, scheme
	case GCSScheme, GSScheme:
		return common.CloudStorageSinkType, GCSScheme
	case AzblobScheme, AzureScheme:
		return common.CloudStorageSinkType, AzblobScheme
	case BlackHoleScheme:
		return common.BlackHoleSinkType, BlackHoleScheme
	case "":
		return common.BlackHoleSinkType, "unknown"
	default:
		return common.BlackHoleSinkType, scheme
	}
}
