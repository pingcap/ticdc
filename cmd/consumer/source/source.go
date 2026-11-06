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

// Package source reads encoded changes from an upstream system.
//
// A source owns transport concerns: connections, offsets, acknowledgements,
// partition routing, memory admission and which streams must be read to cover
// a dependency. The scheduler only asks for a dependency and receives coverage.
package source

import (
	"context"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common/event"
)

const (
	// readBatchRows bounds one source read burst.
	readBatchRows = 1024
	// readBatchBytes bounds the encoded bytes in one source read burst.
	readBatchBytes = 2 << 20
)

// Input is one unit of encoded source data and its transport acknowledgement.
// Exactly one of Value, File, or Watermark is set. A File without a Value is a
// storage schema record.
type Input struct {
	Stream int32
	Key    []byte
	Value  []byte
	Bytes  int64
	Offset int64
	// File is set for storage inputs that decode one file with its schema.
	File *File
	// Watermark marks a completed storage scan.
	Watermark uint64
	// Ack confirms this input and all preceding inputs on its stream. It runs
	// only after the downstream checkpoint covers them.
	Ack func(context.Context) error
}

// Origin identifies an input after its encoded payload has been released. The
// scheduler uses it to validate delivery and to acknowledge transport progress.
type Origin struct {
	Stream int32
	Offset int64
	Bytes  int64
	Ack    func(context.Context) error
}

// Origin returns the transport identity of this input.
func (i *Input) Origin() Origin {
	return Origin{
		Stream: i.Stream,
		Offset: i.Offset,
		Bytes:  i.Bytes,
		Ack:    i.Ack,
	}
}

// File is the storage file context needed to decode its rows.
type File struct {
	Schema  *cloudstorage.SchemaFile
	TableID int64
}

// Stream describes one independently read source stream.
type Stream struct {
	ID    int32
	Topic string
	// DDLBroadcast reports that every stream carries the same DDLs.
	DDLBroadcast bool
	// Control reports that this stream carries the authoritative DDL and
	// resolved events for the source.
	Control bool
}

// Semantics describes the ordering guarantees a source provides.
type Semantics struct {
	// CommitOrdered reports that a stream delivers rows and DDLs in commit
	// order, so the scheduler need not sort them.
	CommitOrdered bool
	// DDLsCanReorder reports that DDLs from different streams may be decoded
	// out of commit order.
	DDLsCanReorder bool
}

// StreamWatermark is decoded progress on one stream.
type StreamWatermark struct {
	Stream    int32
	Timestamp uint64
}

// Progress is the scheduler's view of decoded input progress and read intent.
type Progress struct {
	// Watermarks contains the latest decoded resolved timestamp per stream.
	Watermarks []StreamWatermark
	// Scope narrows the streams that must be read. Nil means every stream; an
	// empty non-nil slice means control streams only.
	Scope []event.SchemaTableName
	// Target is the timestamp that must be covered for Scope. Zero clears the
	// current target.
	Target uint64
	// Retarget applies Target to every stream in Scope. It is false for
	// incremental progress updates.
	Retarget bool
	// Proven lists streams that observed an event proving Target is covered.
	Proven []int32
}

// Reader reads source data. Read is called by one goroutine; Advance and
// ContinueReading are called serially by the scheduler; Close runs after all
// callers stop.
type Reader interface {
	Read(context.Context) ([]*Input, error)
	Streams() []Stream
	Semantics() Semantics
	// Advance publishes decoded progress and the outstanding dependency. It
	// returns true when the dependency is covered.
	Advance(Progress) bool
	ContinueReading(bool)
	Close()
}

// Validator is implemented by sources that can verify encoded inputs against
// their own routing and size limits.
type Validator interface {
	// CheckInput validates the decoded row count of one input.
	CheckInput(origin Origin, rows int)
	// CheckDML validates that a dispatched row batch was read from the stream
	// its routing selects.
	CheckDML(dml *event.DMLEvent, stream int32, offset int64)
}

// SchemaNotifier is implemented by sources that retire schema files once the
// scheduler has delivered their DDL.
type SchemaNotifier interface {
	MarkProcessed(tables []event.SchemaTableName, timestamp uint64)
}
