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

// Package source reads encoded changes from an upstream system.
//
// A source owns transport concerns: connections, offsets, acknowledgements and
// partition routing. The conductor only sees decoded events.
package source

import (
	"context"
	"net/url"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const (
	// readBatchRows bounds one source read burst.
	readBatchRows = 1024
)

// Payload is one unit of encoded source data and its transport acknowledgement.
// Exactly one of Value, File, or Watermark is set. A File without a Value is a
// storage schema record.
type Payload struct {
	Partition int32
	Key       []byte
	Value     []byte
	Bytes     int64
	Offset    int64
	// File is set for storage inputs that decode one file with its schema.
	File *File
	// Watermark marks a completed storage scan.
	Watermark uint64
	// Ack confirms this input and all preceding inputs on its partition. It runs
	// only after the downstream checkpoint covers them.
	Ack func(context.Context) error
}

// file is the storage file context needed to decode its rows.
type File struct {
	Schema  *cloudstorage.SchemaFile
	TableID int64
}

// Reader reads source data. Read and Close are called by one goroutine.
type Reader interface {
	// Read returns the next available inputs. It may return no inputs when the
	// source has nothing available yet.
	Read(context.Context) ([]*Payload, error)
	// Partitions returns the number of independently read partitions. A source
	// without partitions publishes a global watermark instead.
	Partitions() int32
	Close()
}

// validator is implemented by sources that can verify encoded inputs against
// their own routing and size limits.
type Validator interface {
	// CheckDML validates that a dispatched row batch was read from the
	// partition its routing selects.
	CheckDML(dml *event.DMLEvent, partition int32, offset int64)
}

// schemaNotifier is implemented by sources that retire schema files once the
// scheduler has delivered their DDL.
type SchemaNotifier interface {
	MarkProcessed(tables []event.SchemaTableName, timestamp uint64)
}

// Kind identifies the upstream transport.
type Kind string

const (
	KindKafka   Kind = "kafka"
	KindPulsar  Kind = "pulsar"
	KindStorage Kind = "storage"
)

// TypeOf classifies an upstream URI. It panics on an unsupported scheme
// because the consumer cannot start without a known source.
func TypeOf(uri *url.URL) Kind {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return KindKafka
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return KindPulsar
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return KindStorage
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
}

// NewOptions builds source options for the upstream URI.
func NewOptions(upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, codecConfig *common.Config) Options {
	return Options{
		UpstreamURI:   upstreamURI,
		ConsumerID:    consumerID,
		ReplicaConfig: replicaConfig,
		CodecConfig:   codecConfig,
	}
}

// Open builds the reader for the upstream URI.
func Open(ctx context.Context, opts Options) Reader {
	switch TypeOf(opts.UpstreamURI) {
	case KindKafka:
		return newKafkaReader(ctx, opts)
	case KindPulsar:
		return newPulsarReader(opts)
	default:
		return newStorageReader(ctx, opts)
	}
}
