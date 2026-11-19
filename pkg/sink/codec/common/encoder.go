// Copyright 2025 PingCAP, Inc.
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

package common

import (
	"context"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// EventEncoder is an abstraction for events encoder
type EventEncoder interface {
	// EncodeCheckpointEvent appends a checkpoint event into the batch.
	// This event will be broadcast to all partitions to signal a global checkpoint.
	EncodeCheckpointEvent(ts uint64) (*Message, error)
	// EncodeDDLEvent appends a DDL event into the batch
	EncodeDDLEvent(e *commonEvent.DDLEvent) (*Message, error)
	// AppendRowChangedEvent appends a row changed event into the batch or buffer.
	AppendRowChangedEvent(context.Context, string, *commonEvent.RowEvent) error
	// Build builds the batch messages from AppendRowChangedEvent and returns the messages.
	Build() []*Message
}

// TxnEventEncoder is an abstraction for events encoder
type TxnEventEncoder interface {
	// AppendTxnEvent append a txn event into the buffer.
	AppendTxnEvent([]*commonEvent.RowEvent) error
	// Build builds the batch and returns the bytes of key and value.
	// Should be called after `AppendTxnEvent`
	Build() []*Message
}
