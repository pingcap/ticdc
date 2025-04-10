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

package writer

import (
	"context"
	"sync/atomic"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Worker denotes the worker responsible for encoding RowChangedEvents
// to messages formatted in the specific protocol.
type Worker struct {
	id           int
	changeFeedID commonType.ChangeFeedID
	encoder      common.TxnEventEncoder
	isClosed     uint64
	inputCh      <-chan EventFragment
	outputCh     chan<- EventFragment
}

func NewWorker(
	workerID int,
	changefeedID commonType.ChangeFeedID,
	encoder common.TxnEventEncoder,
	inputCh <-chan EventFragment,
	outputCh chan<- EventFragment,
) *Worker {
	return &Worker{
		id:           workerID,
		changeFeedID: changefeedID,
		encoder:      encoder,
		inputCh:      inputCh,
		outputCh:     outputCh,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case frag, ok := <-w.inputCh:
			if !ok || atomic.LoadUint64(&w.isClosed) == 1 {
				return nil
			}
			err := w.encodeEvents(frag)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (w *Worker) encodeEvents(frag EventFragment) error {
	w.encoder.AppendTxnEvent(frag.event)
	frag.encodedMsgs = w.encoder.Build()
	w.outputCh <- frag

	return nil
}

func (w *Worker) Close() {
	if !atomic.CompareAndSwapUint64(&w.isClosed, 0, 1) {
		return
	}
}
