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

const (
	decoderConcurrency = 8
	decoderQueueSize   = 128
)

// future carries one input and the events decoded from it. The decoder pool
// appends the events and closes done; the conductor reads them after Ready. A
// nil event marks the end of an input, including inputs that produce no events.
type future struct {
	payload *sourcePayload
	events  []event.Event
	done    chan struct{}
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
	// input dispatches an undecoded input to the pool.
	input chan *future
	// output queues inputs for the conductor in submission order. A future may
	// still be decoding when it enters.
	output chan *future
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
	upstreamDB := newUpstreamDB(ctx, opts.UpstreamURI)
	group := &decoderGroup{
		codecConfig: codecConfig,
		upstreamDB:  upstreamDB,
		decoders:    make([]decoder, decoderConcurrency),
		input:       make(chan *future, decoderQueueSize),
		output:      make(chan *future, decoderQueueSize),
	}
	if codecConfig.Protocol == config.ProtocolCsv {
		selectors, err := columnselector.New(opts.ReplicaConfig.Sink, util.GetOrZero(opts.ReplicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
		group.selectors = selectors
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

// Add transfers ownership of the source to the group. The reader calls it
// serially, so output receives futures in the source's submission order even
// though the pool finishes them in any order.
func (d *decoderGroup) Add(ctx context.Context, payloads []*sourcePayload) error {
	for _, item := range payloads {
		future := &future{
			payload: item,
			done:    make(chan struct{}),
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.input <- future:
		}

		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case d.output <- future:
		}
	}
	return nil
}

func (d *decoderGroup) Output() <-chan *future {
	return d.output
}

func (d *decoderGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, decoder := range d.decoders {
		g.Go(func() error {
			return d.runDecoder(ctx, decoder)
		})
	}
	return g.Wait()
}

func (d *decoderGroup) runDecoder(ctx context.Context, decoder decoder) error {
	// Inputs whose message the decoder cached until the table info arrives, in
	// the order the decoder cached them.
	var waiting []*future
	for {
		var changed <-chan struct{}
		waiting, changed = d.drainCached(decoder, waiting)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		case future := <-d.input:
			cached, err := d.decodeInput(ctx, decoder, future)
			if err != nil {
				return err
			}
			if cached {
				waiting = append(waiting, future)
				continue
			}
			future.finish()
		}
	}
}

func (f *future) addEvent(value event.Event) {
	f.events = append(f.events, value)
}

// Ready blocks until the pool decoded every event of the input.
func (f *future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.done:
	}
	return nil
}

func (f *future) finish() {
	f.addEvent(nil)
	close(f.done)
}

func (d *decoderGroup) addRow(future *future, dml *event.DMLEvent) {
	if dml == nil {
		log.Panic("consumer DML decoding failed")
	}
	if input := future.payload; input.File != nil {
		dml.PhysicalTableID = input.File.TableID
		if dml.TableInfo.UpdateTS != input.File.Schema.TableVersion {
			dml.TableInfo = dml.TableInfo.CloneWithRouting(dml.TableInfo.GetTargetSchemaName(), dml.TableInfo.GetTargetTableName())
			dml.TableInfo.UpdateTS = input.File.Schema.TableVersion
		}
	}
	future.addEvent(dml)
}

// drainCached emits rows that were decoded before their schema arrived. The
// schema change channel is read before the pending rows to avoid missing a
// write between that check and the select below.
func (d *decoderGroup) drainCached(decoder decoder, waiting []*future) ([]*future, <-chan struct{}) {
	if decoder.schema == nil {
		return waiting, nil
	}
	changed := d.schemas.Changes()
	for _, dml := range decoder.schema.GetCachedEvents() {
		future := waiting[0]
		waiting = waiting[1:]
		d.addRow(future, dml)
		future.finish()
	}
	return waiting, changed
}

// decodeInput decodes the input, reporting whether the decoder cached the
// message until its table info arrives.
func (d *decoderGroup) decodeInput(ctx context.Context, decoder decoder, future *future) (bool, error) {
	input := future.payload
	if input.File != nil {
		return false, d.decodeStorage(ctx, future)
	}
	if input.Watermark != 0 {
		future.addEvent(&event.ResolvedEvent{ResolvedTs: input.Watermark})
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
			if d.schemas != nil && ddl.Query == "" {
				continue
			}
			future.addEvent(ddl)
		case common.MessageTypeResolved:
			future.addEvent(&event.ResolvedEvent{ResolvedTs: decoder.NextResolvedEvent()})
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
		future.addEvent(&event.DDLEvent{
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
