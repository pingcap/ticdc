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

	spoolpkg "github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage/spool"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/hash"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/utils/chann"
)

// eventFragment is used to attach a sequence number to TxnCallbackableEvent.
type eventFragment struct {
	event          *commonEvent.DMLEvent
	versionedTable cloudstorage.VersionedTableName
	dispatcherID   commonType.DispatcherID

	// The sequence number is mainly useful for TxnCallbackableEvent defragmentation.
	// e.g. TxnCallbackableEvent 1~5 are dispatched to a group of encoding workers, but the
	// encoding completion time varies. Let's say the final completion sequence are 1,3,2,5,4,
	// we can use the sequence numbers to do defragmentation so that the events can arrive
	// at dmlWriters sequentially.
	seqNumber uint64
	// encodedMsgs denote the encoded messages after the event is handled in encodingWorker.
	encodedMsgs []*common.Message
	spoolEntry  *spoolpkg.Entry
	marker      *drainMarker
}

func newEventFragment(seq uint64, version cloudstorage.VersionedTableName, event *commonEvent.DMLEvent) eventFragment {
	return eventFragment{
		seqNumber:      seq,
		versionedTable: version,
		dispatcherID:   event.GetDispatcherID(),
		event:          event,
	}
}

func newDrainMarkerFragment(
	seq uint64,
	dispatcherID commonType.DispatcherID,
	commitTs uint64,
	doneCh chan error,
) eventFragment {
	return eventFragment{
		seqNumber:    seq,
		dispatcherID: dispatcherID,
		marker: &drainMarker{
			dispatcherID: dispatcherID,
			commitTs:     commitTs,
			doneCh:       doneCh,
		},
	}
}

func (f eventFragment) isDrainMarker() bool {
	return f.marker != nil
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

// defragmenter is used to handle event fragments which can be registered
// out of order.
type defragmenter struct {
	lastDispatchedSeq uint64
	future            map[uint64]eventFragment
	inputCh           <-chan eventFragment
	outputChs         []*chann.DrainableChann[eventFragment]
	hasher            *hash.PositionInertia
	spool             *spoolpkg.Manager
}

func newDefragmenter(
	inputCh <-chan eventFragment,
	outputChs []*chann.DrainableChann[eventFragment],
	spool *spoolpkg.Manager,
) *defragmenter {
	return &defragmenter{
		future:    make(map[uint64]eventFragment),
		inputCh:   inputCh,
		outputChs: outputChs,
		hasher:    hash.NewPositionInertia(),
		spool:     spool,
	}
}

func (d *defragmenter) Run(ctx context.Context) error {
	defer d.close()
	for {
		select {
		case <-ctx.Done():
			d.future = nil
			return errors.Trace(ctx.Err())
		case frag, ok := <-d.inputCh:
			if !ok {
				return nil
			}
			// check whether to write messages to output channel right now
			next := d.lastDispatchedSeq + 1
			if frag.seqNumber == next {
				if err := d.writeMsgsConsecutive(ctx, frag); err != nil {
					return err
				}
			} else if frag.seqNumber > next {
				d.future[frag.seqNumber] = frag
			} else {
				return nil
			}
		}
	}
}

func (d *defragmenter) writeMsgsConsecutive(
	ctx context.Context,
	start eventFragment,
) error {
	if err := d.dispatchFragToDMLWorker(start); err != nil {
		return err
	}

	// try to dispatch more fragments to DML workers
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		next := d.lastDispatchedSeq + 1
		if frag, ok := d.future[next]; ok {
			delete(d.future, next)
			if err := d.dispatchFragToDMLWorker(frag); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (d *defragmenter) dispatchFragToDMLWorker(frag eventFragment) error {
	if d.spool != nil && !frag.isDrainMarker() {
		entry, err := d.spool.Enqueue(frag.encodedMsgs, frag.event.PostEnqueue)
		if err != nil {
			return errors.Trace(err)
		}
		frag.spoolEntry = entry
		frag.encodedMsgs = nil
	}

	d.hasher.Reset()
	d.hasher.Write(frag.dispatcherID.Marshal())
	workerID := d.hasher.Sum32() % uint32(len(d.outputChs))
	d.outputChs[workerID].In() <- frag
	d.lastDispatchedSeq = frag.seqNumber
	return nil
}

func (d *defragmenter) close() {
	for _, ch := range d.outputChs {
		ch.CloseAndDrain()
	}
}
