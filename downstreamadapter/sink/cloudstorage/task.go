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
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	pkgcloudstorage "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

type taskKind uint8

const (
	taskKindDML taskKind = iota
	taskKindDrain
)

type task struct {
	kind           taskKind
	event          *commonEvent.DMLEvent
	versionedTable pkgcloudstorage.VersionedTableName
	dispatcherID   commonType.DispatcherID

	encodedMsgs []*common.Message
	spoolEntry  *spool.Entry
	marker      *drainMarker
}

func newDMLTask(
	version pkgcloudstorage.VersionedTableName,
	event *commonEvent.DMLEvent,
) *task {
	return &task{
		kind:           taskKindDML,
		event:          event,
		versionedTable: version,
		dispatcherID:   event.GetDispatcherID(),
	}
}

func newDrainTask(
	dispatcherID commonType.DispatcherID,
	commitTs uint64,
	doneCh chan error,
) *task {
	return &task{
		kind:         taskKindDrain,
		dispatcherID: dispatcherID,
		marker: &drainMarker{
			dispatcherID: dispatcherID,
			commitTs:     commitTs,
			doneCh:       doneCh,
		},
	}
}

func (t *task) isDrainTask() bool {
	return t != nil && t.kind == taskKindDrain
}

type drainMarker struct {
	dispatcherID commonType.DispatcherID
	commitTs     uint64
	doneCh       chan error
}

func (m *drainMarker) done(err error) {
	select {
	case m.doneCh <- err:
	default:
	}
}
