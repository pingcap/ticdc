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

	// The sequence number is mainly useful for TxnCallbackableEvent defragmentation.
	// e.g. TxnCallbackableEvent 1~5 are dispatched to a group of encoding workers, but the
	// encoding completion time varies. Let's say the final completion sequence are 1,3,2,5,4,
	// we can use the sequence numbers to do defragmentation so that the events can arrive
	// at dmlWriters sequentially.
	seqNumber uint64
	// encodedMsgs denote the encoded messages after the event is handled in encodingWorker.
	encodedMsgs []*common.Message
}

func newEventFragment(seq uint64, version cloudstorage.VersionedTableName, event *commonEvent.DMLEvent) eventFragment {
	return eventFragment{
		seqNumber:      seq,
		versionedTable: version,
		event:          event,
	}
}

// defragmenter is used to handle event fragments which can be registered
// out of order.
type defragmenter struct {
	lastDispatchedSeq uint64
	future            map[uint64]eventFragment
	inputCh           <-chan eventFragment
	outputChs         []*chann.UnlimitedChannel[eventFragment, any]
	hasher            *hash.PositionInertia
}

func newDefragmenter(
	inputCh <-chan eventFragment,
	outputChs []*chann.UnlimitedChannel[eventFragment, any],
) *defragmenter {
	return &defragmenter{
		future:    make(map[uint64]eventFragment),
		inputCh:   inputCh,
		outputChs: outputChs,
		hasher:    hash.NewPositionInertia(),
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
				d.writeMsgsConsecutive(ctx, frag)
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
) {
	d.dispatchFragToDMLWorker(start)

	// try to dispatch more fragments to DML workers
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		next := d.lastDispatchedSeq + 1
		if frag, ok := d.future[next]; ok {
			delete(d.future, next)
			d.dispatchFragToDMLWorker(frag)
		} else {
			return
		}
	}
}

func (d *defragmenter) dispatchFragToDMLWorker(frag eventFragment) {
	tableName := frag.versionedTable.TableNameWithPhysicTableID
	d.hasher.Reset()
	d.hasher.Write([]byte(tableName.Schema), []byte(tableName.Table))
	workerID := d.hasher.Sum32() % uint32(len(d.outputChs))
	d.outputChs[workerID].Push(frag)
	d.lastDispatchedSeq = frag.seqNumber
}

func (d *defragmenter) close() {
	for _, ch := range d.outputChs {
		ch.Close()
	}
}
