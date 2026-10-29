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

package lake

import (
	"context"
	"hash/fnv"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/pingcap/ticdc/downstreamadapter/sink/statistics"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/spool"
	"github.com/pingcap/ticdc/pkg/writelease"
	"golang.org/x/sync/errgroup"
)

type workItem struct {
	event  *commonEvent.DMLEvent
	charge int64
	ddl    *commonEvent.DDLEvent
	done   chan error
}

// Sink owns a bounded source admission pool, shared spool and a fixed number
// of serial table lanes. No transport epoch owns data after admission.
type Sink struct {
	opts                  options
	client                *httpClient
	spool                 *spool.Spool
	lanes                 []*tableLane
	statistics            *statistics.Statistics
	ctx                   context.Context
	cancel                context.CancelCauseFunc
	normal                atomic.Bool
	stopOnce, cleanupOnce sync.Once
	mu                    sync.Mutex
	sourceBytes           int64
	sourceEvents          int
	admissionChanged      chan struct{}
	started               bool
	closed                bool
	done                  chan struct{}
	ddlMu                 sync.Mutex
}

func Verify(ctx context.Context, cfg *config.ChangefeedConfig, uri *url.URL) error {
	o, err := parseOptions(uri, cfg)
	if err != nil {
		return err
	}
	c := newHTTPClient(o)
	defer c.transport.CloseIdleConnections()
	rows, err := c.selectRows(ctx, "SELECT ?::BIGINT", []any{int64(1)})
	if err != nil {
		return err
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil || *rows[0][0] != "1" {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("server must support HTTP bound parameters and terminal query states")
	}
	return nil
}

func New(ctx context.Context, id common.ChangeFeedID, cfg *config.ChangefeedConfig, uri *url.URL, keyspaceID uint32) (*Sink, error) {
	o, err := parseOptions(uri, cfg)
	if err != nil {
		return nil, err
	}
	buffer, err := spool.New(id, spool.WithRootDir(o.spoolDir), spool.WithDirectoryNamespace("lake-sink-spool", config.GetGlobalServerConfig().AdvertiseAddr), spool.WithMetrics(newSpoolMetrics(id)), spool.WithDiskQuotaBytes(o.spoolBytes), spool.WithSegmentBytes(min(64<<20, o.spoolBytes/4)), spool.WithMemoryRatio(o.memoryRatio))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	s := &Sink{opts: o, client: newHTTPClient(o), spool: buffer, ctx: ctx, cancel: cancel, done: make(chan struct{}), admissionChanged: make(chan struct{}), statistics: statistics.New(id, keyspaceID)}
	s.normal.Store(true)
	for i := 0; i < o.workers; i++ {
		s.lanes = append(s.lanes, &tableLane{sink: s, input: make(chan workItem, o.sourceEvents), schemas: newSchemaManager(s.client, o), batches: make(map[string]*pendingTable)})
	}
	return s, nil
}
func (s *Sink) SinkType() common.SinkType { return common.LakeSinkType }
func (s *Sink) IsNormal() bool            { return s.normal.Load() && s.ctx.Err() == nil }

func (s *Sink) BatchCount() int { return min(s.opts.sourceEvents, s.opts.batchRows) }

func (s *Sink) BatchBytes() int                                   { return int(min(s.opts.sourceBytes/2, s.opts.batchBytes)) }
func (s *Sink) SetWriteGate(gate *writelease.Gate)                { s.client.gate.Store(gate) }
func (s *Sink) SetTableSchemaStore(*commonEvent.TableSchemaStore) {}
func (s *Sink) AddCheckpointTs(uint64)                            {}

// stop marks an internal failure as unretryable, even when its cause is a
// timeout. Only stopLifecycle is used for externally requested cancellation.
func (s *Sink) stop(err error) {
	s.stopLifecycle(errors.WrapChangefeedUnretryableErr(err))
}

func (s *Sink) stopLifecycle(err error) {
	s.stopOnce.Do(func() { s.normal.Store(false); s.cancel(err) })
}

func (s *Sink) lane(database, table string) *tableLane {
	h := fnv.New64a()
	_, _ = h.Write([]byte(database + "\x00" + table))
	return s.lanes[h.Sum64()%uint64(len(s.lanes))]
}

func (s *Sink) releaseSource(charge int64) {
	s.mu.Lock()
	s.sourceBytes -= charge
	s.sourceEvents--
	s.notifyAdmissionLocked()
	s.mu.Unlock()
}

// notifyAdmissionLocked broadcasts a quota/queue change without allocating
// goroutines or retaining a second waiting-event queue. The caller holds mu.
func (s *Sink) notifyAdmissionLocked() {
	close(s.admissionChanged)
	s.admissionChanged = make(chan struct{})
}

func sourceEventBytes(e *commonEvent.DMLEvent) int64 {
	charge := e.Rows.MemoryUsage() + max(e.GetSize(), int64(0)) + int64(len(e.RowTypes))*8 + int64(len(e.RowKeys))*24 + int64(len(e.Checksum))*64 + 1024
	for _, key := range e.RowKeys {
		charge += int64(len(key))
	}
	return charge
}

func (s *Sink) AddDMLEvent(e *commonEvent.DMLEvent) {
	if !s.IsNormal() {
		return
	}
	if e == nil || e.TableInfo == nil || e.Rows == nil {
		s.stop(errors.ErrLakeInvalidData.GenWithStackByArgs("invalid native DML event"))
		return
	}
	// Shared chunks are conservatively charged in full for each admitted event.
	// Waiting callers still own their event under the upstream memory budget.
	charge := sourceEventBytes(e)
	if charge > s.opts.sourceBytes {
		s.stop(errors.ErrLakeInvalidData.GenWithStackByArgs("single source event exceeds admission byte limit"))
		return
	}
	db := e.TableInfo.GetSchemaName()
	if s.opts.database != "" {
		db = s.opts.database
	}
	lane := s.lane(db, e.TableInfo.GetTableName())
	for {
		s.mu.Lock()
		if !s.IsNormal() || s.closed {
			s.mu.Unlock()
			return
		}
		if charge <= s.opts.sourceBytes-s.sourceBytes && s.sourceEvents < s.opts.sourceEvents {
			s.sourceBytes += charge
			s.sourceEvents++
			select {
			case lane.input <- workItem{event: e, charge: charge}:
				s.mu.Unlock()
				return
			default:
				// A control/barrier item may occupy this lane. Roll back the
				// reservation and wait for the independent lane worker.
				s.sourceBytes -= charge
				s.sourceEvents--
			}
		}
		changed := s.admissionChanged
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-changed:
		}
	}
}

func (s *Sink) waitWork(lane *tableLane, item workItem) error {
	item.done = make(chan error, 1)
	select {
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	case lane.input <- item:
	}
	select {
	case <-s.ctx.Done():
		return context.Cause(s.ctx)
	case err := <-item.done:
		return err
	}
}

func (s *Sink) FlushDMLBeforeBlock(commonEvent.BlockEvent) error {
	if !s.IsNormal() {
		return context.Cause(s.ctx)
	}
	for _, lane := range s.lanes {
		if err := s.waitWork(lane, workItem{}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	s.ddlMu.Lock()
	defer s.ddlMu.Unlock()
	if err := s.FlushDMLBeforeBlock(event); err != nil {
		return err
	}
	e, ok := event.(*commonEvent.DDLEvent)
	if !ok {
		err := errors.ErrLakeInvalidData.GenWithStackByArgs("syncpoints are not supported")
		s.stop(err)
		return err
	}
	db, table := e.SchemaName, e.TableName
	if e.TableInfo != nil {
		db, table = e.TableInfo.GetSchemaName(), e.TableInfo.GetTableName()
	}
	if s.opts.database != "" {
		db = s.opts.database
	}
	if err := s.waitWork(s.lane(db, table), workItem{ddl: e}); err != nil {
		return err
	}
	event.PostFlush()
	return nil
}

func (s *Sink) cleanup() {
	s.cleanupOnce.Do(func() {
		// Pair with AddDMLEvent's admission critical section. Once normal is
		// false, no producer may publish behind cleanup's final queue drain.
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		for _, lane := range s.lanes {
			lane.cleanup()
		}
		s.spool.Close()
		s.client.transport.CloseIdleConnections()
		s.statistics.Close()
	})
}

func (s *Sink) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return context.Cause(s.ctx)
	}
	if s.started {
		s.mu.Unlock()
		return errors.ErrLakeInvalidData.GenWithStackByArgs("sink lifecycle cannot be reused")
	}
	s.started = true
	s.mu.Unlock()
	defer close(s.done)
	defer s.cleanup()
	stop := context.AfterFunc(ctx, func() { s.stopLifecycle(context.Cause(ctx)) })
	defer stop()
	g, _ := errgroup.WithContext(s.ctx)
	for _, lane := range s.lanes {
		g.Go(func() error {
			err := lane.run(s.ctx)
			if err != nil {
				s.stop(err)
			}
			return err
		})
	}
	_ = g.Wait()
	s.normal.Store(false)
	// The first cause already distinguishes a normal external stop from an
	// unretryable internal failure. No nested timeout can downgrade the latter.
	return context.Cause(s.ctx)
}

func (s *Sink) Close() {
	s.stopLifecycle(context.Canceled)
	s.mu.Lock()
	s.closed = true
	started := s.started
	s.mu.Unlock()
	if started {
		<-s.done
	} else {
		s.cleanup()
	}
}
