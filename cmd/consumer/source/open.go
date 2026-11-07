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

package source

import (
	"context"
	"net/url"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// Type identifies the upstream transport.
type Type string

const (
	Kafka   Type = "kafka"
	Pulsar  Type = "pulsar"
	Storage Type = "storage"
)

// TypeOf classifies an upstream URI. It panics on an unsupported scheme because
// the consumer cannot start without a known source.
func TypeOf(uri *url.URL) Type {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return Kafka
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return Pulsar
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return Storage
	}
	log.Panic("consumer upstream scheme is unsupported", zap.String("scheme", uri.Scheme))
	return ""
}

// Options configures a source reader.
type Options struct {
	UpstreamURI   *url.URL
	ConsumerID    string
	ReplicaConfig *config.ReplicaConfig
	CodecConfig   *common.Config
	Budget        *budget.Budget
}

// Open builds the reader for the upstream URI.
func Open(ctx context.Context, opts Options) Reader {
	switch TypeOf(opts.UpstreamURI) {
	case Kafka:
		return newKafkaReader(ctx, opts)
	case Pulsar:
		return newPulsarReader(opts)
	default:
		return newStorageReader(ctx, opts)
	}
}
