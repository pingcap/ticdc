// Copyright 2025 PingCAP, Inc.
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
	versionedTable cloudstorage.VersionedTableName // Versioned output identity for the DML event.
	encodedMsgs    []*common.Message               // Encoded result built from event.

	// Flush-only field.
	marker *flushMarker // Barrier marker used by FlushDMLBeforeBlock.
}

func newDMLTask(
	version cloudstorage.VersionedTableName,
	event *commonEvent.DMLEvent,
) *task {
	return &task{
		kind:           taskKindDML,
		event:          event,
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
