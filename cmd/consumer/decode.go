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

// The decode stage turns encoded source inputs into TiCDC events.
//
// The group preserves source submission order and keeps undecoded inputs in
// their original slots, so the scheduler can build dependencies without knowing
// the codec protocol.
package main

import (
	"context"
	"database/sql"
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// decodedEvent carries a fully decoded event and its source position. A nil event
// marks the end of an input, including inputs that produce no events.
type decodedEvent struct {
	Origin origin
	Event  event.Event
}

const (
	decoderConcurrency = 8
	decoderQueueSize   = 128
	eventBatchSize     = 1024
)

// future keeps one input's output in submission order. Large inputs stream
// bounded batches; the conductor only receives completed events.
type future struct {
	payload *sourcePayload
	origin  origin
	output  chan []decodedEvent
	events  []decodedEvent
}

// decoder pairs a generic codec decoder with the simple-protocol decoder when
// the source needs schema-aware row buffering.
type decoder struct {
	common.Decoder
	schema *simple.Decoder
}

// decoderGroup decodes inputs with a bounded pool while preserving submission order.
type decoderGroup struct {
	codecConfig *common.Config
	upstreamDB  *sql.DB
	selectors   *columnselector.ColumnSelectors
	decoders    []decoder
	schemas     *simple.TableInfoCache
	input       chan *future
	ordered     chan *future
	output      chan []decodedEvent
}

// decodeOptions configures a decode group.
type decodeOptions struct {
	CodecConfig   *common.Config
	ReplicaConfig *config.ReplicaConfig
	Streams       []stream
	UpstreamURI   *url.URL
}

// newDecodeGroup builds a decode group for the source streams.
func newDecodeGroup(ctx context.Context, opts decodeOptions) *decoderGroup {
	codecConfig := opts.CodecConfig
	var selectors *columnselector.ColumnSelectors
	if codecConfig.Protocol == config.ProtocolCsv {
		var err error
		selectors, err = columnselector.New(opts.ReplicaConfig.Sink, util.GetOrZero(opts.ReplicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
	}
	upstreamDB := newUpstreamDB(ctx, opts.UpstreamURI)
	group := &decoderGroup{
		codecConfig: codecConfig,
		upstreamDB:  upstreamDB,
		selectors:   selectors,
		decoders:    make([]decoder, decoderConcurrency),
		input:       make(chan *future, decoderQueueSize),
		ordered:     make(chan *future, decoderQueueSize),
		output:      make(chan []decodedEvent, 1),
	}
	// Storage constructs a decoder for each file using that file's schema.
	if len(opts.Streams) == 0 {
		return group
	}
	if codecConfig.Protocol == config.ProtocolSimple {
		group.schemas = simple.NewTableInfoCache()
	}
	for i := range group.decoders {
		if group.schemas == nil {
			// The conductor decides which partition's DDL to execute. Every
			// decoder must produce complete DDL metadata, regardless of its index.
			item, err := codec.NewEventDecoder(ctx, 0, codecConfig, opts.Streams[0].Topic, upstreamDB)
			if err != nil {
				log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
			}
			group.decoders[i] = decoder{Decoder: item}
			continue
		}
		decoded, err := simple.NewDecoder(ctx, codecConfig, upstreamDB, group.schemas)
		if err != nil {
			log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
		}
		if decoded == nil {
			continue
		}
		schema, ok := decoded.(*simple.Decoder)
		if !ok {
			log.Panic("consumer simple decoder does not support schema waiting")
		}
		group.decoders[i] = decoder{Decoder: decoded, schema: schema}
	}
	return group
}

// ReleaseSchemas drops schema history that is no longer reachable from the
// durable checkpoint.
func (d *decoderGroup) ReleaseSchemas(checkpoint uint64) {
	if d.schemas != nil && checkpoint != 0 {
		d.schemas.ReleaseBefore(checkpoint)
	}
}

// Close releases the upstream metadata connection.
func (d *decoderGroup) Close() {
	if d.upstreamDB == nil {
		return
	}
	if err := d.upstreamDB.Close(); err != nil {
		log.Warn("consumer upstream database close failed", zap.Error(err))
	}
}

// AddPayload transfers ownership of the source to the group. The reader calls
// it serially, so the ordered queue has exactly the source's submission order.
func (d *decoderGroup) AddPayload(ctx context.Context, payloads []*sourcePayload) error {
	for idx, item := range payloads {
		future := &future{
			payload: item,
			origin:  item.Origin(),
			output:  make(chan []decodedEvent, 1),
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.input <- future:
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.ordered <- future:
		}

		payloads[idx] = nil
	}
	return nil
}

func (d *decoderGroup) Output() <-chan []decodedEvent {
	return d.output
}

func (d *decoderGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return d.emit(ctx)
	})
	for _, decoder := range d.decoders {
		g.Go(func() error {
			return d.runDecoder(ctx, decoder)
		})
	}
	return g.Wait()
}

func (d *decoderGroup) emit(ctx context.Context) error {
	defer close(d.output)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case future := <-d.ordered:
			if err := d.emitInput(ctx, future); err != nil {
				return err
			}
		}
	}
}

func (d *decoderGroup) emitInput(ctx context.Context, future *future) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case results, ok := <-future.output:
			if !ok {
				return nil
			}
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case d.output <- results:
			}
		}
	}
}

func (d *decoderGroup) runDecoder(ctx context.Context, decoder decoder) error {
	var pending map[*common.DMLMessage]*future
	if decoder.schema != nil {
		pending = make(map[*common.DMLMessage]*future)
	}
	for {
		changed, err := d.drainCached(ctx, decoder, pending)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		case future := <-d.input:
			message, err := d.decodeInput(ctx, decoder, future)
			if err != nil {
				return err
			}
			if message == nil {
				if err := future.finish(ctx); err != nil {
					return err
				}
				continue
			}
			pending[message] = future
		}
	}
}

func (f *future) addEvent(ctx context.Context, value event.Event) error {
	f.events = append(f.events, decodedEvent{Origin: f.origin, Event: value})
	if len(f.events) == eventBatchSize {
		return f.flush(ctx)
	}
	return nil
}

func (f *future) flush(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case f.output <- f.events:
		f.events = nil
		return nil
	}
}

func (f *future) finish(ctx context.Context) error {
	if err := f.addEvent(ctx, nil); err != nil {
		return err
	}
	if len(f.events) != 0 {
		if err := f.flush(ctx); err != nil {
			return err
		}
	}
	close(f.output)
	return nil
}

func (d *decoderGroup) addRow(ctx context.Context, future *future, message *common.DMLMessage) error {
	dml := message.ToDMLEvent()
	if dml == nil {
		log.Panic("consumer DML decoding failed", zap.String("schema", message.Schema), zap.String("table", message.Table), zap.Uint64("commitTs", message.GetCommitTs()))
	}
	if input := future.payload; input.File != nil {
		dml.PhysicalTableID = input.File.TableID
		if dml.TableInfo.UpdateTS != input.File.Schema.TableVersion {
			dml.TableInfo = dml.TableInfo.CloneWithRouting(dml.TableInfo.GetTargetSchemaName(), dml.TableInfo.GetTargetTableName())
			dml.TableInfo.UpdateTS = input.File.Schema.TableVersion
		}
	}
	return future.addEvent(ctx, dml)
}

// drainCached emits rows that were decoded before their schema arrived. The
// schema change channel is read before the pending rows to avoid missing a
// write between that check and the select below.
func (d *decoderGroup) drainCached(ctx context.Context, decoder decoder, pending map[*common.DMLMessage]*future) (<-chan struct{}, error) {
	if decoder.schema == nil {
		return nil, nil
	}
	changed := d.schemas.Changes()
	for _, message := range decoder.schema.GetCachedMessages() {
		future := pending[message]
		if err := d.addRow(ctx, future, message); err != nil {
			return nil, err
		}
		if err := future.finish(ctx); err != nil {
			return nil, err
		}
		delete(pending, message)
	}
	return changed, nil
}

func (d *decoderGroup) decodeInput(ctx context.Context, decoder decoder, future *future) (*common.DMLMessage, error) {
	input := future.payload
	if input.File != nil {
		return nil, d.decodeStorage(ctx, future)
	}
	if input.Watermark != 0 {
		return nil, future.addEvent(ctx, &event.ResolvedEvent{ResolvedTs: input.Watermark})
	}
	decoder.AddKeyValue(input.Key, input.Value)
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		kind, more := decoder.HasNext()
		if !more {
			return nil, nil
		}
		switch kind {
		case common.MessageTypeRow:
			message := decoder.NextDMLMessage()
			if message == nil {
				// Simple has one event per input. Keep its original output slot
				// while this decoder continues with subsequent messages.
				message = decoder.schema.PendingDMLMessage
				return message, nil
			}
			if err := d.addRow(ctx, future, message); err != nil {
				return nil, err
			}
		case common.MessageTypeDDL:
			ddl := decoder.NextDDLEvent()
			if d.schemas != nil && ddl.Query == "" {
				continue
			}
			if err := future.addEvent(ctx, ddl); err != nil {
				return nil, err
			}
		case common.MessageTypeResolved:
			if err := future.addEvent(ctx, &event.ResolvedEvent{ResolvedTs: decoder.NextResolvedEvent()}); err != nil {
				return nil, err
			}
		}
	}
}

func (d *decoderGroup) decodeStorage(ctx context.Context, future *future) error {
	input := future.payload
	schema := input.File.Schema
	table := schema.TableInfo()
	table.UpdateTS = schema.TableVersion
	if input.Value == nil {
		scope := event.InfluenceTypeNormal
		switch {
		case schema.Schema == "" || model.ActionType(schema.Type) == model.ActionCreateView:
			scope = event.InfluenceTypeAll
		case schema.Table == "":
			scope = event.InfluenceTypeDB
		}
		return future.addEvent(ctx, &event.DDLEvent{
			FinishedTs:    schema.TableVersion,
			Type:          schema.Type,
			Query:         schema.Query,
			SchemaName:    schema.Schema,
			TableName:     schema.Table,
			TableInfo:     table,
			BlockedTables: &event.InfluencedTables{InfluenceType: scope},
		})
	}
	var decoder common.Decoder
	switch d.codecConfig.Protocol {
	case config.ProtocolCsv:
		var err error
		decoder, err = csv.NewDecoderWithColumnSelector(ctx, d.codecConfig, table, input.Value, d.selectors.GetForTableInfo(table))
		if err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "create CSV decoder")
		}
	case config.ProtocolCanalJSON:
		decoder = canal.NewTxnDecoder(d.codecConfig)
		decoder.AddKeyValue(nil, input.Value)
	default:
		return errors.ErrCodecDecode.GenWithStackByArgs("storage consumer protocol is unsupported")
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		kind, more := decoder.HasNext()
		if !more {
			return nil
		}
		if kind != common.MessageTypeRow {
			continue
		}
		message := decoder.NextDMLMessage()
		if err := d.addRow(ctx, future, message); err != nil {
			return err
		}
	}
}

func newUpstreamDB(ctx context.Context, upstreamURI *url.URL) *sql.DB {
	dsn := upstreamURI.Query().Get("upstream-tidb-dsn")
	if dsn == "" {
		return nil
	}
	upstreamDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panic("consumer upstream database open failed", zap.Error(err))
	}
	upstreamDB.SetMaxOpenConns(10)
	upstreamDB.SetMaxIdleConns(10)
	upstreamDB.SetConnMaxLifetime(10 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = upstreamDB.PingContext(ctx); err != nil {
		log.Panic("consumer upstream database connection failed", zap.Error(err))
	}
	return upstreamDB
}
