// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"sync/atomic"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

type taskKind uint8

const (
	taskKindDML taskKind = iota
	taskKindFlush
)

type task struct {
	kind taskKind

	// Shared by DML and flush tasks for stable per-dispatcher routing.
	dispatcherID commonType.DispatcherID

	// DML-only fields.
	event          *commonEvent.DMLEvent           // Original DML event to encode and flush.
	callbacks      *txnCallbacks                   // Lightweight txn callbacks detached from event.
	tableInfo      *commonType.TableInfo           // Table info used after event is released.
	versionedTable cloudstorage.VersionedTableName // Versioned output identity for the DML event.
	encodedMsgs    []*common.Message               // Encoded result built from event.

	// Flush-only field.
	marker *flushMarker // Barrier marker used by FlushDMLBeforeBlock.
}

func newDMLTask(
	version cloudstorage.VersionedTableName,
	event *commonEvent.DMLEvent,
) *task {
	// The dispatcher path registers progress callbacks before calling
	// Sink.AddDMLEvent, so snapshot callbacks here and release the large event
	// object after encoding.
	return &task{
		kind:           taskKindDML,
		event:          event,
		callbacks:      newTxnCallbacks(event),
		tableInfo:      event.TableInfo,
		versionedTable: version,
		dispatcherID:   event.GetDispatcherID(),
	}
}

func newFlushTask(
	dispatcherID commonType.DispatcherID,
	commitTs uint64,
) *task {
	return &task{
		kind:         taskKindFlush,
		dispatcherID: dispatcherID,
		marker:       newFlushMarker(commitTs),
	}
}

func (t *task) isFlushTask() bool {
	return t != nil && t.kind == taskKindFlush
}

func (t *task) replacePostFlushCallbacks() {
	if len(t.encodedMsgs) == 0 {
		return
	}

	// Txn encoders put event.PostFlush into message.Callback. That method value
	// keeps the original DMLEvent reachable through the encoded messages, so
	// replace it with the lightweight callback copy before releasing task.event.
	for _, msg := range t.encodedMsgs {
		msg.Callback = nil
	}
	// One callback on the last message is enough because all messages in a task
	// are enqueued and flushed as one spool entry.
	t.encodedMsgs[len(t.encodedMsgs)-1].Callback = t.callbacks.postFlush
}

// txnCallbacks is a lightweight copy of a DMLEvent's enqueue and flush
// callbacks. It lets cloud storage release the full DMLEvent after encoding
// while preserving the event callback semantics: each stage runs at most once.
type txnCallbacks struct {
	flushed  []func()
	enqueued []func()

	flushedCalled  atomic.Bool
	enqueuedCalled atomic.Bool
}

func newTxnCallbacks(event *commonEvent.DMLEvent) *txnCallbacks {
	if event == nil {
		return &txnCallbacks{}
	}
	return &txnCallbacks{
		flushed:  append([]func(){}, event.PostTxnFlushed...),
		enqueued: append([]func(){}, event.PostTxnEnqueued...),
	}
}

func (c *txnCallbacks) postFlush() {
	if c == nil || !c.flushedCalled.CompareAndSwap(false, true) {
		return
	}
	for _, f := range c.flushed {
		if f != nil {
			f()
		}
	}
	c.postEnqueue()
}

func (c *txnCallbacks) postEnqueue() {
	if c == nil || !c.enqueuedCalled.CompareAndSwap(false, true) {
		return
	}
	for _, f := range c.enqueued {
		if f != nil {
			f()
		}
	}
}

func (t *task) wait(ctx context.Context) error {
	if !t.isFlushTask() {
		return nil
	}
	return t.marker.wait(ctx)
}

type flushMarker struct {
	commitTs uint64
	done     chan struct{}
}

func newFlushMarker(commitTs uint64) *flushMarker {
	return &flushMarker{
		commitTs: commitTs,
		done:     make(chan struct{}),
	}
}

// finish closes done to broadcast flush completion to all waiters.
// Writer is the only owner that may call finish.
func (m *flushMarker) finish() {
	close(m.done)
}

func (m *flushMarker) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(context.Cause(ctx))
	case <-m.done:
		return nil
	}
}
