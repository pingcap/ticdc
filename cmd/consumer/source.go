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

// The source stage reads encoded changes from an upstream system.
//
// A source owns transport concerns: connections, offsets, acknowledgements,
// partition routing and which streams must be read to cover a dependency. The
// scheduler only asks for a dependency and receives coverage.
package main

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

// sourcePayload is one unit of encoded source data and its transport acknowledgement.
// Exactly one of Value, File, or Watermark is set. A File without a Value is a
// storage schema record.
type sourcePayload struct {
	Stream int32
	Key    []byte
	Value  []byte
	Bytes  int64
	Offset int64
	// File is set for storage inputs that decode one file with its schema.
	File *file
	// Watermark marks a completed storage scan.
	Watermark uint64
	// Ack confirms this input and all preceding inputs on its stream. It runs
	// only after the downstream checkpoint covers them.
	Ack func(context.Context) error
}

// file is the storage file context needed to decode its rows.
type file struct {
	Schema  *cloudstorage.SchemaFile
	TableID int64
}

// stream describes one independently read source stream.
type stream struct {
	ID    int32
	Topic string
	// DDLBroadcast reports that every stream carries the same DDLs.
	DDLBroadcast bool
	// Control reports that this stream carries the authoritative DDL and
	// resolved events for the source.
	Control bool
}

// semantics describes the ordering guarantees a source provides.
type semantics struct {
	// CommitOrdered reports that a stream delivers rows and DDLs in commit
	// order, so the scheduler need not sort them.
	CommitOrdered bool
	// DDLsCanReorder reports that DDLs from different streams may be decoded
	// out of commit order.
	DDLsCanReorder bool
}

// streamWatermark is decoded progress on one stream.
type streamWatermark struct {
	Stream    int32
	Timestamp uint64
}

// sourceProgress is the scheduler's view of decoded input progress and read intent.
type sourceProgress struct {
	// Watermarks contains the latest decoded resolved timestamp per stream.
	Watermarks []streamWatermark
	// Scope narrows the streams that must be read. Nil means every stream; an
	// empty non-nil slice means control streams only.
	Scope []event.SchemaTableName
	// Target is the timestamp that must be covered for Scope. Zero clears the
	// current target.
	Target uint64
	// Retarget applies Target to every stream in Scope. It is false for
	// incremental progress updates.
	Retarget bool
	// Proven lists streams that observed an event proving Target is covered.
	Proven []int32
}

// sourceReader reads source data. Read is called by one goroutine; Advance and
// ContinueReading are called serially by the scheduler; Close runs after all
// callers stop.
type sourceReader interface {
	// Read returns the next available inputs. It may return no inputs when the
	// source has nothing available yet.
	Read(context.Context) ([]*sourcePayload, error)
	Streams() []stream
	Semantics() semantics
	// Advance publishes decoded progress and the outstanding dependency. It
	// returns true when the dependency is covered.
	Advance(sourceProgress) bool
	ContinueReading(bool)
	Close()
}

// validator is implemented by sources that can verify encoded inputs against
// their own routing and size limits.
type validator interface {
	// CheckInput validates the decoded row count of one input.
	CheckInput(input *sourcePayload, rows int)
	// CheckDML validates that a dispatched row batch was read from the stream
	// its routing selects.
	CheckDML(dml *event.DMLEvent, stream int32, offset int64)
}

// schemaNotifier is implemented by sources that retire schema files once the
// scheduler has delivered their DDL.
type schemaNotifier interface {
	MarkProcessed(tables []event.SchemaTableName, timestamp uint64)
}

// sourceType identifies the upstream transport.
type sourceType string

const (
	sourceKafka   sourceType = "kafka"
	sourcePulsar  sourceType = "pulsar"
	sourceStorage sourceType = "storage"
)

// parseSourceType classifies an upstream URI. It panics on an unsupported scheme
// because the consumer cannot start without a known source.
func parseSourceType(uri *url.URL) sourceType {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return sourceKafka
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return sourcePulsar
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return sourceStorage
	}
	log.Panic("consumer upstream scheme is unsupported", zap.String("scheme", uri.Scheme))
	return ""
}

// sourceOptions configures a source reader.
type sourceOptions struct {
	UpstreamURI   *url.URL
	ConsumerID    string
	ReplicaConfig *config.ReplicaConfig
	CodecConfig   *common.Config
}

// newSourceReader builds the reader for the upstream URI.
func newSourceReader(ctx context.Context, opts sourceOptions) sourceReader {
	switch parseSourceType(opts.UpstreamURI) {
	case sourceKafka:
		return newKafkaReader(ctx, opts)
	case sourcePulsar:
		return newPulsarReader(opts)
	default:
		return newStorageReader(ctx, opts)
	}
}
