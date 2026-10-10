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
	"cmp"
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
)

// Read and Advance have one caller. Confirm may run concurrently.
// Close runs after all callers have stopped.
type reader interface {
	Read(ctx context.Context) (*readData, error)
	Advance(ctx context.Context, feedback readFeedback) (readProgress, error)
	Confirm(ctx context.Context) error
	Close() error
}

type readOrder uint8

const (
	inputOrder readOrder = iota
	commitOrder
)

type readFormat uint8

const (
	messageFormat readFormat = iota
	rowFormat                // Rows decoded with externally supplied table metadata.
)

// readData carries encoded contents, logical decoding context and input proofs.
// Physical offsets, message IDs and file paths remain private to each reader.
type readData struct {
	format        readFormat
	key           []byte
	value         []byte
	stream        int32
	record        *ack
	retainedBytes int64
	table         *common.TableInfo
	ddl           *event.DDLEvent
	control       *readControl
	group         *readGroup
	groupEnd      bool
	dmlBoundary   *readBoundary
	ddlBoundary   *readBoundary
	ddlOrder      readOrder
}

// Group inputs are contiguous. An unordered group must be completely decoded
// before its rows are sorted.
type readGroup struct {
	tableID  int64
	order    readOrder
	boundary *readBoundary
}

// Decoded facts drive reading progress. They never confirm downstream writes.
// A boundary request covers only the candidate events already decoded.
type readFeedback struct {
	data           *readData
	decoded        bool
	dml            *event.DMLEvent
	ddl            *event.DDLEvent
	watermark      uint64
	hasWatermark   bool
	needsMoreInput bool
	boundaryDDL    *event.DDLEvent
	pendingDML     int
}

type readProgress struct {
	watermark      uint64
	hasWatermark   bool
	needsMoreInput bool
	boundary       *readBoundary
	control        *readControl
	skip           bool
}

// Controls retain their input references until the downstream watermark passes.
type readControl struct {
	watermark uint64
	tableID   int64
	records   []*ack
	bytes     int64
}

// Readers own the positions behind a proof and publish when it is reached.
type readBoundary struct {
	reached bool
}

type decodeConfig struct {
	codec      *codecCommon.Config
	topic      string
	upstreamDB *sql.DB
}

func newReader(ctx context.Context, upstreamURI *url.URL, consumerID, timezone string, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (reader, *decodeConfig, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, nil, err
	}
	var r reader
	switch source {
	case sourceKafka:
		r, err = newKafkaReader(ctx, upstreamURI, consumerID, memory, replicaConfig)
	case sourcePulsar:
		r, err = newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, memory)
	default:
		r, err = newStorageReader(ctx, upstreamURI, replicaConfig, memory)
	}
	if err != nil {
		return nil, nil, err
	}
	decoding, err := newDecodeConfig(ctx, upstreamURI, timezone, replicaConfig)
	if err != nil {
		_ = r.Close()
		return nil, nil, err
	}
	return r, decoding, nil
}

// Resolve transport-specific defaults and requirements before constructing the assembler.
func newDecodeConfig(ctx context.Context, upstreamURI *url.URL, timezone string, replicaConfig *config.ReplicaConfig) (decoding *decodeConfig, err error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	protocolName := upstreamURI.Query().Get(config.ProtocolKey)
	switch source {
	case sourcePulsar:
		protocolName = cmp.Or(protocolName, putil.GetOrZero(replicaConfig.Sink.Protocol), "canal-json")
	case sourceStorage:
		protocolName = putil.GetOrZero(replicaConfig.Sink.Protocol)
	}
	protocol, err := config.ParseSinkProtocolFromString(protocolName)
	if err != nil {
		return nil, err
	}
	switch source {
	case sourceKafka:
		switch protocol {
		case config.ProtocolOpen, config.ProtocolCanalJSON, config.ProtocolAvro, config.ProtocolSimple, config.ProtocolDebezium, config.ProtocolDebeziumAvro:
		default:
			return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("unsupported Kafka protocol " + protocol.String())
		}
	case sourcePulsar:
		if protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar consumer requires canal-json")
		}
	case sourceStorage:
		if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrStorageSinkInvalidConfig.FastGenByArgs("Storage consumer requires csv or canal-json")
		}
	}
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return nil, err
	}
	if source != sourceStorage {
		switch protocol {
		case config.ProtocolCanalJSON, config.ProtocolDebezium:
			if !codecConfig.EnableTiDBExtension {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension must be true")
			}
		case config.ProtocolAvro, config.ProtocolDebeziumAvro:
			if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension and avro-enable-watermark must be true")
			}
			if codecConfig.AvroConfluentSchemaRegistry == "" {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("schema-registry is required")
			}
		}
	} else if protocol == config.ProtocolCanalJSON {
		codecConfig.EnableTiDBExtension = true
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		return nil, err
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("debezium-disable-schema must be false")
	}
	var db *sql.DB
	if source == sourceKafka && upstreamURI.Query().Get("upstream-tidb-dsn") != "" {
		db, err = sql.Open("mysql", upstreamURI.Query().Get("upstream-tidb-dsn"))
		if err != nil {
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "open consumer upstream TiDB")
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(10 * time.Minute)
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err != nil {
			_ = db.Close()
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
		}
	}
	return &decodeConfig{codec: codecConfig, topic: strings.Trim(upstreamURI.Path, "/"), upstreamDB: db}, nil
}
