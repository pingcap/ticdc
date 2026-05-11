//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package writer

import (
	"context"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/uuid"
)

var (
	_ RedoEvent = (*commonEvent.RedoRowEvent)(nil)
	_ RedoEvent = (*commonEvent.DDLEvent)(nil)
)

// RedoEvent is the interface for redo event.
type RedoEvent interface {
	PostFlush()
	ToRedoLog() *commonEvent.RedoLog
}

// RedoDMLWriter writes row redo events, all operations are thread-safe.
type RedoDMLWriter interface {
	AddDMLEvents(ctx context.Context, events ...*commonEvent.RedoRowEvent) error
	Run(ctx context.Context) error
	Close() error
}

// RedoDDLWriter writes DDL redo events, all operations are thread-safe.
type RedoDDLWriter interface {
	WriteDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error
	Close() error
	SetTableSchemaStore(*commonEvent.TableSchemaStore)
}

// Option define the writerOptions
type Option func(writer *LogWriterOptions)

// LogWriterOptions is the options for writer
type LogWriterOptions struct {
	GetLogFileName   func() string
	GetUUIDGenerator func() uuid.Generator
}

// WithLogFileName provide the Option for fileName
func WithLogFileName(f func() string) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetLogFileName = f
		}
	}
}

// WithUUIDGenerator provides the Option for uuid generator
func WithUUIDGenerator(f func() uuid.Generator) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetUUIDGenerator = f
		}
	}
}

// EncodeFrameSize encodes the frame size for etcd wal which uses code
// from etcd wal/encoder.go. Ref: https://github.com/etcd-io/etcd/pull/5250
func EncodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}
