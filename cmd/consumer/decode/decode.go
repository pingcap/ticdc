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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	decoderQueueSize  = 128
	decodeConcurrency = 16
)

// PendingDecode marks a Simple DML parked until its table schema arrives.
type PendingDecode struct {
	ID       uint64
	CommitTs uint64
}

// ResolvedDML carries a parked DML decoded once its table schema arrived.
type ResolvedDML struct {
	ID        uint64
	DML       *event.DMLEvent
	Partition int32
	Offset    int64
}

// Future carries one input and the events decoded from it. The decoder owning
// the input populates its events, pending placeholders, and woken DMLs and
// closes done; the conductor reads them after Ready. A nil event marks the end
// of an input, including inputs that produce no events.
type Future struct {
	Payload  *source.Payload
	Events   []event.Event
	Pending  []PendingDecode
	Resolved []ResolvedDML
	Marker   uint64
	done     chan struct{}
}

type waitingSchemaKey struct {
	schema  string
	table   string
	version uint64
}

type waitingDML struct {
	id        uint64
	pending   *simple.PendingMessage
	partition int32
	offset    int64
}

type wokenDML struct {
	waitingDML
	tableInfo *commonType.TableInfo
}

// decoder pairs a generic codec decoder with the simple-protocol decoder when
// the source needs schema-aware row buffering.
type decoder struct {
	common.EventDecoder
	schema *simple.Decoder
}

// Group decodes inputs across concurrent decoders while preserving submission
// order. Same-partition MQ inputs map deterministically to the same decoder,
// and storage inputs round-robin across decoders.
type Group struct {
	kind              source.Kind
	codecConfig       *common.Config
	upstreamDB        *sql.DB
	claimCheckStorage storeapi.Storage
	schemaManager     schemamanager.SchemaManager
	selectors         *columnselector.ColumnSelectors
	decoders          []decoder
	schemas           *common.TableInfoProvider
	waitMu            sync.Mutex
	nextPendingID     uint64
	waiting           map[waitingSchemaKey][]waitingDML
	ddlMu             sync.Mutex
	seenDDLs          map[ddl.Key]struct{}
	index             int
	// queues dispatches undecoded inputs to the decoder pipelines.
	queues []chan *Future
	// output queues inputs for the conductor in submission order. A Future may
	// still be decoding when it enters.
	output chan *Future
}

// Options configures a decode group.
type Options struct {
	CodecConfig   *common.Config
	ReplicaConfig *config.ReplicaConfig
	UpstreamURI   *url.URL
}

// NewGroup builds a decode group with concurrency decoupled from source partitions.
func NewGroup(ctx context.Context, opts Options) *Group {
	kind := source.TypeOf(opts.UpstreamURI)
	topic := strings.Trim(opts.UpstreamURI.Path, "/")
	codecConfig := opts.CodecConfig
	upstreamDB := newUpstreamDB(ctx, opts.UpstreamURI)

	group := &Group{
		kind:        kind,
		codecConfig: codecConfig,
		upstreamDB:  upstreamDB,
		schemas:     common.NewTableInfoProvider(),
		waiting:     make(map[waitingSchemaKey][]waitingDML),
		seenDDLs:    make(map[ddl.Key]struct{}),
		decoders:    make([]decoder, decodeConcurrency),
		queues:      make([]chan *Future, decodeConcurrency),
		output:      make(chan *Future, decoderQueueSize),
	}
	for i := range group.queues {
		group.queues[i] = make(chan *Future, decoderQueueSize)
	}

	if codecConfig.Protocol == config.ProtocolCsv {
		selectors, err := columnselector.New(opts.ReplicaConfig.Sink, util.GetOrZero(opts.ReplicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
		group.selectors = selectors
	}

	// Storage decodes each file with a decoder built from that file's schema,
	// so pipelines need no long-lived protocol decoder instance.
	if kind == source.KindStorage {
		return group
	}

	if codecConfig.LargeMessageHandle.EnableClaimCheck() {
		switch codecConfig.Protocol {
		case config.ProtocolOpen, config.ProtocolDefault, config.ProtocolCanalJSON, config.ProtocolSimple:
			storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, codecConfig.LargeMessageHandle.ClaimCheckStorageURI)
			if err != nil {
				log.Panic("consumer claim-check storage initialization failed", zap.Error(err))
			}
			group.claimCheckStorage = storage
		}
	}

	if codecConfig.Protocol == config.ProtocolAvro || codecConfig.Protocol == config.ProtocolDebeziumAvro {
		schemaM, err := schemamanager.NewConfluentSchemaManager(ctx, codecConfig.AvroConfluentSchemaRegistry, nil)
		if err != nil {
			log.Panic("consumer schema registry initialization failed", zap.Error(err))
		}
		group.schemaManager = schemaM
	}

	for i := range group.decoders {
		item, err := codec.NewEventDecoder(ctx, codecConfig, topic, upstreamDB, group.claimCheckStorage, group.schemaManager)
		if err != nil {
			log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
		}

		item.ShareTableInfo(group.schemas)

		schema, ok := item.(*simple.Decoder)
		if !ok {
			group.decoders[i] = decoder{EventDecoder: item}
			continue
		}

		group.decoders[i] = decoder{EventDecoder: item, schema: schema}
	}

	return group
}

// Schemas returns the table info cache shared by the decoders.
func (d *Group) Schemas() *common.TableInfoProvider {
	return d.schemas
}

// Close releases the upstream metadata connection and shared decoder clients.
func (d *Group) Close() {
	if d.upstreamDB != nil {
		if err := d.upstreamDB.Close(); err != nil {
			log.Warn("consumer upstream database close failed", zap.Error(err))
		}
	}

	if d.claimCheckStorage != nil {
		d.claimCheckStorage.Close()
	}

	if d.schemaManager != nil {
		d.schemaManager.Close()
	}
}

func (d *Group) decoderIndex(item *source.Payload) int {
	if d.kind == source.KindStorage {
		idx := d.index % len(d.queues)
		d.index++
		return idx
	}

	return int(uint32(item.Partition) % uint32(len(d.queues)))
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
		case d.queues[d.decoderIndex(item)] <- f:
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
		futures := d.queues[i]
		g.Go(func() error {
			return d.runDecoder(ctx, decoder, futures)
		})
	}

	return g.Wait()
}

func (d *Group) runDecoder(ctx context.Context, decoder decoder, futures <-chan *Future) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case f := <-futures:
			if err := d.decodeInput(ctx, decoder, f); err != nil {
				return err
			}

			f.finish()
		}
	}
}

// Ready blocks until the decoder finished every event of the input.
func (f *Future) Ready(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-f.done:
	}

	return nil
}

func (f *Future) finish() {
	f.Events = append(f.Events, nil)
	close(f.done)
}

func (d *Group) addRow(f *Future, dml *event.DMLEvent) {
	if dml == nil {
		log.Panic("consumer DML decoding failed")
	}

	f.Events = append(f.Events, dml)
}

func (d *Group) popWaitingLocked(info *commonType.TableInfo, dst []wokenDML) []wokenDML {
	if info == nil || info.TableName.Schema == "" || info.TableName.Table == "" {
		return dst
	}

	key := waitingSchemaKey{
		schema:  info.TableName.Schema,
		table:   info.TableName.Table,
		version: info.GetUpdateTS(),
	}

	items := d.waiting[key]
	if len(items) == 0 {
		return dst
	}

	delete(d.waiting, key)
	for _, item := range items {
		dst = append(dst, wokenDML{waitingDML: item, tableInfo: info})
	}

	return dst
}

func (d *Group) decodeSimpleRow(schemaDecoder *simple.Decoder, future *Future) {
	pending := schemaDecoder.TakePendingDML()

	if released := d.schemas.ReleasedWatermark(); released != 0 && pending.CommitTs <= released {
		return
	}

	d.waitMu.Lock()

	if released := d.schemas.ReleasedWatermark(); released != 0 && pending.CommitTs <= released {
		d.waitMu.Unlock()
		return
	}

	tableInfo := d.schemas.Read(pending.Schema, pending.Table, pending.SchemaVersion)
	if tableInfo == nil {
		if d.waiting == nil {
			d.waiting = make(map[waitingSchemaKey][]waitingDML)
		}

		d.nextPendingID++
		id := d.nextPendingID
		key := waitingSchemaKey{
			schema:  pending.Schema,
			table:   pending.Table,
			version: pending.SchemaVersion,
		}

		d.waiting[key] = append(d.waiting[key], waitingDML{
			id:        id,
			pending:   pending,
			partition: future.Payload.Partition,
			offset:    future.Payload.Offset,
		})
		future.Pending = append(future.Pending, PendingDecode{
			ID:       id,
			CommitTs: pending.CommitTs,
		})
		future.Marker = max(future.Marker, pending.CommitTs)
		d.waitMu.Unlock()
		return
	}

	d.waitMu.Unlock()

	d.addRow(future, schemaDecoder.DecodeDMLWithInfo(pending, tableInfo))
}

func (d *Group) decodeSimpleDDL(schemaDecoder *simple.Decoder, future *Future) {
	ddlEvent := schemaDecoder.DecodeDDLEvent()

	var effectiveTs uint64
	if !ddlEvent.IsBootstrap {
		effectiveTs = ddlEvent.FinishedTs
	}
	var woken []wokenDML
	d.waitMu.Lock()
	d.schemas.WriteTableInfo(ddlEvent.TableInfo, effectiveTs)

	woken = d.popWaitingLocked(ddlEvent.TableInfo, woken)
	if len(ddlEvent.MultipleTableInfos) > 1 && ddlEvent.MultipleTableInfos[1] != nil {
		d.schemas.WriteTableInfo(ddlEvent.MultipleTableInfos[1], 0)
		woken = d.popWaitingLocked(ddlEvent.MultipleTableInfos[1], woken)
	}
	d.waitMu.Unlock()

	for _, item := range woken {
		dml := schemaDecoder.DecodeDMLWithInfo(item.pending, item.tableInfo)
		if dml == nil {
			log.Panic("consumer DML decoding failed")
		}

		future.Resolved = append(future.Resolved, ResolvedDML{
			ID:        item.id,
			DML:       dml,
			Partition: item.partition,
			Offset:    item.offset,
		})
	}

	if ddlEvent.IsBootstrap || ddlEvent.Query == "" {
		return
	}

	if !d.claimDDL(ddlEvent) {
		future.Marker = max(future.Marker, ddlEvent.GetCommitTs())
		return
	}

	future.Events = append(future.Events, ddlEvent)
}

// decodeInput decodes one input payload into future.
func (d *Group) decodeInput(ctx context.Context, decoder decoder, future *Future) error {
	payload := future.Payload
	if payload.File != nil {
		return d.decodeStorage(ctx, future)
	}

	if payload.Watermark != 0 {
		future.Events = append(future.Events, &event.ResolvedEvent{ResolvedTs: payload.Watermark})
		return nil
	}

	decoder.AddKeyValue(payload.Key, payload.Value)
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}

		kind, more := decoder.HasNext()
		if !more {
			return nil
		}

		switch kind {
		case common.MessageTypeRow:
			if decoder.schema != nil {
				d.decodeSimpleRow(decoder.schema, future)
				continue
			}

			dml := decoder.NextDMLEvent()
			if dml != nil {
				d.addRow(future, dml)
			}
		case common.MessageTypeDDL:
			if decoder.schema != nil {
				d.decodeSimpleDDL(decoder.schema, future)
				continue
			}

			ddlEvent := decoder.NextDDLEvent()
			if ddlEvent.IsBootstrap || ddlEvent.Query == "" {
				continue
			}

			if !d.claimDDL(ddlEvent) {
				future.Marker = max(future.Marker, ddlEvent.GetCommitTs())
				continue
			}

			future.Events = append(future.Events, ddlEvent)
		case common.MessageTypeResolved:
			future.Events = append(future.Events, &event.ResolvedEvent{ResolvedTs: decoder.NextResolvedEvent()})
		}
	}
}

func (d *Group) claimDDL(ddlEvent *event.DDLEvent) bool {
	if ddlEvent == nil {
		return false
	}

	key := ddl.KeyOf(ddlEvent)

	d.ddlMu.Lock()
	defer d.ddlMu.Unlock()

	if d.seenDDLs == nil {
		d.seenDDLs = make(map[ddl.Key]struct{})
	}
	if _, ok := d.seenDDLs[key]; ok {
		return false
	}

	d.seenDDLs[key] = struct{}{}
	return true
}

func (d *Group) decodeStorage(ctx context.Context, future *Future) error {
	payload := future.Payload
	schema := payload.File.Schema
	table := schema.TableInfo()
	table.UpdateTS = schema.TableVersion

	if payload.Value == nil {
		scope := event.InfluenceTypeNormal
		switch {
		case schema.Schema == "" || model.ActionType(schema.Type) == model.ActionCreateView:
			scope = event.InfluenceTypeAll
		case schema.Table == "":
			scope = event.InfluenceTypeDB
		}

		future.Events = append(future.Events, &event.DDLEvent{
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
		decoder, err = csv.NewDecoderWithColumnSelector(ctx, d.codecConfig, table, payload.Value, d.selectors.GetForTableInfo(table))
		if err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err)
		}
	case config.ProtocolCanalJSON:
		decoder = canal.NewTxnDecoder(d.codecConfig)
		decoder.AddKeyValue(nil, payload.Value)
	default:
		return errors.ErrCodecDecode.GenWithStack("storage consumer protocol is unsupported")
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

		dml := decoder.NextDMLEvent()
		if dml == nil {
			log.Panic("consumer DML decoding failed")
		}

		dml.PhysicalTableID = payload.File.TableID
		dml.TableInfo.UpdateTS = schema.TableVersion
		d.addRow(future, dml)
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
