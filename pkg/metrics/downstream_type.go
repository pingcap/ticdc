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

package metrics

import (
	"net/url"

	"github.com/pingcap/ticdc/pkg/config"
)

// DownstreamTypeFromSinkURI returns a stable downstream type label value for a
// changefeed sink-uri.
//
// The returned value is derived from the URI scheme and normalized to:
// - keep the number of label values small (e.g., "mysql+ssl" -> "mysql")
// - keep compatibility with future sink schemes (unknown schemes are returned as-is)
// - avoid leaking sensitive information (only the scheme is used)
//
// It returns "unknown" when the sink-uri cannot be parsed or has an empty scheme.
func DownstreamTypeFromSinkURI(sinkURI string) string {
	parsed, err := url.Parse(sinkURI)
	if err != nil {
		return "unknown"
	}
	return downstreamTypeFromScheme(config.GetScheme(parsed))
}

func downstreamTypeFromScheme(scheme string) string {
	switch scheme {
	case config.MySQLScheme, config.MySQLSSLScheme:
		// MySQL-compatible downstreams may be either MySQL or TiDB. We only
		// expose a confirmed "tidb" label when the sink reports it is TiDB.
		// Otherwise, use a combined label to avoid misleading users.
		return "mysql/tidb"
	case config.TiDBScheme, config.TiDBSSLScheme:
		return config.TiDBScheme
	case config.KafkaScheme, config.KafkaSSLScheme:
		return config.KafkaScheme
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return config.PulsarScheme
	case config.FileScheme:
		return config.FileScheme
	case config.S3Scheme:
		return config.S3Scheme
	case config.GCSScheme, config.GSScheme:
		return config.GCSScheme
	case config.AzblobScheme, config.AzureScheme:
		return config.AzblobScheme
	case config.CloudStorageNoopScheme:
		return config.CloudStorageNoopScheme
	case config.BlackHoleScheme:
		return config.BlackHoleScheme
	case "":
		return "unknown"
	default:
		return scheme
	}
}
