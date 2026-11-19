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

// Package decode turns encoded source inputs into TiCDC events.
//
// The group preserves source submission order and keeps undecoded inputs in
// their original slots, so the conductor sees every input in read order.
package decode

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/source"
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

const decoderQueueSize = 128

// Future carries one input and the events decoded from it. The decoder owning
// the input's partition appends the events and closes done; the conductor reads
// them after Ready. A
// nil event marks the end of an input, including inputs that produce no events.
type Future struct {
	Payload *source.Payload
	Events  []event.Event
	done    chan struct{}
}

// decoder pairs a generic codec decoder with the simple-protocol decoder when
// the source needs schema-aware row buffering.
type decoder struct {
	common.Decoder
	schema *simple.Decoder
}

// Group decodes every partition with its own decoder while preserving
// submission order. Decoder state describes one partition's schema history, so
// a decoder never handles another partition's input.
type Group struct {
	codecConfig  *common.Config
	upstreamDB   *sql.DB
	selectors    *columnselector.ColumnSelectors
	decoders     []decoder
	schemas      *common.TableInfoCache
	allocator    *common.TableIDAllocator
	ddlPartition int32
	// inputs dispatches an undecoded input to the decoder owning its partition.
	inputs []chan *Future
	// output queues inputs for the conductor in submission order. A Future may
	// still be decoding when it enters.
	output chan *Future
}

// Options configures a decode group.
type Options struct {
	CodecConfig   *common.Config
	ReplicaConfig *config.ReplicaConfig
	Partitions    int32
	Topic         string
	UpstreamURI   *url.URL
	// DDLPartition is the partition whose DDL messages are delivered
	// downstream. The other partitions decode them only to update their own
	// state.
	DDLPartition int32
}

// NewOptions builds decode options for a source.
func NewOptions(
	codecConfig *common.Config, replicaConfig *config.ReplicaConfig, reader source.Reader, upstreamURI *url.URL,
) Options {
	return Options{
		CodecConfig:   codecConfig,
		ReplicaConfig: replicaConfig,
		Partitions:    reader.Partitions(),
		Topic:         strings.Trim(upstreamURI.Path, "/"),
		UpstreamURI:   upstreamURI,
		DDLPartition:  reader.DDLPartition(),
	}
}

// NewGroup builds a decode group for the source partitions.
func NewGroup(ctx context.Context, opts Options) *Group {
	codecConfig := opts.CodecConfig
	upstreamDB := newUpstreamDB(ctx, opts.UpstreamURI)
	partitions := max(1, int(opts.Partitions))
	group := &Group{
		codecConfig:  codecConfig,
		upstreamDB:   upstreamDB,
		allocator:    common.NewTableIDAllocator(),
		ddlPartition: opts.DDLPartition,
		decoders:     make([]decoder, partitions),
		inputs:       make([]chan *Future, partitions),
		output:       make(chan *Future, decoderQueueSize),
	}
	for i := range group.inputs {
		group.inputs[i] = make(chan *Future, decoderQueueSize)
	}
	if codecConfig.Protocol == config.ProtocolCsv {
		selectors, err := columnselector.New(opts.ReplicaConfig.Sink, util.GetOrZero(opts.ReplicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
		group.selectors = selectors
	}
	// Storage constructs a decoder for each file using that file's schema.
	if opts.Partitions == 0 {
		return group
	}
	if codecConfig.Protocol == config.ProtocolSimple {
		group.schemas = common.NewTableInfoCache()
	}
	for i := range group.decoders {
		if group.schemas == nil {
			item, err := codec.NewEventDecoder(ctx, codecConfig, opts.Topic, upstreamDB, group.allocator)
			if err != nil {
				log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
			}
			group.decoders[i] = decoder{Decoder: item}
			continue
		}
		decoded, err := simple.NewDecoder(ctx, codecConfig, upstreamDB, group.schemas, group.allocator)
		if err != nil {
			log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
		}
		schema, ok := decoded.(*simple.Decoder)
		if !ok {
			log.Panic("consumer simple decoder does not support schema waiting")
		}
		group.decoders[i] = decoder{Decoder: decoded, schema: schema}
	}
	return group
}

// Close releases the upstream metadata connection.
func (d *Group) Close() {
	if d.upstreamDB == nil {
		return
	}
	if err := d.upstreamDB.Close(); err != nil {
		log.Warn("consumer upstream database close failed", zap.Error(err))
	}
}

// Add transfers ownership of the source to the group. The reader calls it
// serially, so output receives futures in the source's submission order even
// though the decoders finish them in any order.
func (d *Group) Add(ctx context.Context, payloads []*source.Payload) error {
	for _, item := range payloads {
		f := &Future{
			Payload: item,
			done:    make(chan struct{}),
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.inputs[item.Partition] <- f:
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.output <- f:
		}
	}
	return nil
}

func (d *Group) Output() <-chan *Future {
	return d.output
}

func (d *Group) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, decoder := range d.decoders {
		input := d.inputs[i]
		g.Go(func() error {
			return d.runDecoder(ctx, decoder, input)
		})
	}
	return g.Wait()
}

func (d *Group) runDecoder(ctx context.Context, decoder decoder, input <-chan *Future) error {
	// Inputs whose message the decoder cached until the table info arrives, in
	// the order the decoder cached them.
	var waiting []*Future
	for {
		var changed <-chan struct{}
		waiting, changed = d.drainCached(decoder, waiting)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		case f := <-input:
			cached, err := d.decodeInput(ctx, decoder, f)
			if err != nil {
				return err
			}
			if cached {
				waiting = append(waiting, f)
				continue
			}
			f.finish()
		}
	}
}

func (f *Future) appendEvent(value event.Event) {
	f.Events = append(f.Events, value)
}

// Ready blocks until the pool decoded every event of the input.
func (f *Future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.done:
	}
	return nil
}

func (f *Future) finish() {
	f.appendEvent(nil)
	close(f.done)
}

func (d *Group) addRow(f *Future, dml *event.DMLEvent) {
	if dml == nil {
		log.Panic("consumer DML decoding failed")
	}
	if input := f.Payload; input.File != nil {
		dml.PhysicalTableID = input.File.TableID
		if dml.TableInfo.UpdateTS != input.File.Schema.TableVersion {
			dml.TableInfo = dml.TableInfo.CloneWithRouting(dml.TableInfo.GetTargetSchemaName(), dml.TableInfo.GetTargetTableName())
			dml.TableInfo.UpdateTS = input.File.Schema.TableVersion
		}
	}
	f.appendEvent(dml)
}

// drainCached emits rows that were decoded before their schema arrived. The
// schema change channel is read before the pending rows to avoid missing a
// write between that check and the select below.
func (d *Group) drainCached(decoder decoder, waiting []*Future) ([]*Future, <-chan struct{}) {
	if decoder.schema == nil {
		return waiting, nil
	}
	changed := d.schemas.Changes()
	for _, dml := range decoder.schema.GetCachedEvents() {
		f := waiting[0]
		waiting = waiting[1:]
		d.addRow(f, dml)
		f.finish()
	}
	return waiting, changed
}

// decodeInput decodes the input, reporting whether the decoder cached the
// message until its table info arrives.
func (d *Group) decodeInput(ctx context.Context, decoder decoder, future *Future) (bool, error) {
	input := future.Payload
	if input.File != nil {
		return false, d.decodeStorage(ctx, future)
	}
	if input.Watermark != 0 {
		future.appendEvent(&event.ResolvedEvent{ResolvedTs: input.Watermark})
		return false, nil
	}
	decoder.AddKeyValue(input.Key, input.Value)
	for {
		if err := context.Cause(ctx); err != nil {
			return false, err
		}
		kind, more := decoder.HasNext()
		if !more {
			return false, nil
		}
		switch kind {
		case common.MessageTypeRow:
			dml := decoder.NextDMLEvent()
			if dml == nil {
				// Simple has one message per input; the decoder keeps it cached
				// until the table info arrives.
				return true, nil
			}
			d.addRow(future, dml)
		case common.MessageTypeDDL:
			ddl := decoder.NextDDLEvent()
			if input.Partition != d.ddlPartition {
				// Every partition receives the broadcast DDL and decodes it to
				// update its own state, but only one copy is applied downstream.
				continue
			}
			if d.schemas != nil && ddl.Query == "" {
				continue
			}
			future.appendEvent(ddl)
		case common.MessageTypeResolved:
			future.appendEvent(&event.ResolvedEvent{ResolvedTs: decoder.NextResolvedEvent()})
		}
	}
}

func (d *Group) decodeStorage(ctx context.Context, future *Future) error {
	input := future.Payload
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
		future.appendEvent(&event.DDLEvent{
			FinishedTs:    schema.TableVersion,
			Type:          schema.Type,
			Query:         schema.Query,
			SchemaName:    schema.Schema,
			TableName:     schema.Table,
			TableInfo:     table,
			BlockedTables: &event.InfluencedTables{InfluenceType: scope},
		})
		return nil
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
		d.addRow(future, decoder.NextDMLEvent())
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
