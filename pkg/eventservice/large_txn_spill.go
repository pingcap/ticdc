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

package eventservice

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
)

const largeTxnSpillRecordLenSize = 8

// largeTxnInsertSpill stores deferred insert rows for a single large transaction.
type largeTxnInsertSpill struct {
	path    string
	file    *os.File
	closed  bool
	cleaned bool
}

func newLargeTxnInsertSpill(dir string) (*largeTxnInsertSpill, error) {
	if dir == "" {
		return nil, errors.New("large txn spill dir is empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, errors.Trace(err)
	}

	file, err := os.CreateTemp(dir, "eventservice-large-txn-insert-*.spill")
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &largeTxnInsertSpill{
		path: file.Name(),
		file: file,
	}, nil
}

func (s *largeTxnInsertSpill) Append(entry *common.RawKVEntry) error {
	if s.cleaned {
		return errors.New("large txn spill has been cleaned up")
	}
	if s.closed {
		return errors.New("large txn spill is closed")
	}
	if entry == nil {
		return errors.New("cannot append nil RawKVEntry to large txn spill")
	}

	payload := entry.Encode()
	var lenBuf [largeTxnSpillRecordLenSize]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(payload)))
	if err := writeLargeTxnSpillRecord(s.file, lenBuf[:]); err != nil {
		return err
	}
	return writeLargeTxnSpillRecord(s.file, payload)
}

func (s *largeTxnInsertSpill) NewReader() (*largeTxnInsertSpillReader, error) {
	if s.cleaned {
		return nil, errors.New("large txn spill has been cleaned up")
	}
	if err := s.Close(); err != nil {
		return nil, err
	}

	file, err := os.Open(s.path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &largeTxnInsertSpillReader{file: file}, nil
}

func (s *largeTxnInsertSpill) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}

	err := s.file.Close()
	s.file = nil
	return errors.Trace(err)
}

func (s *largeTxnInsertSpill) Cleanup() error {
	if s == nil {
		return nil
	}
	if err := s.Close(); err != nil {
		return err
	}
	if s.cleaned {
		return nil
	}
	s.cleaned = true
	if s.path == "" {
		return nil
	}

	err := os.Remove(s.path)
	if err != nil && !os.IsNotExist(err) {
		return errors.Trace(err)
	}
	return nil
}

type largeTxnInsertSpillReader struct {
	file   *os.File
	closed bool
}

func (r *largeTxnInsertSpillReader) Next() (*common.RawKVEntry, error) {
	if r.closed {
		return nil, errors.New("large txn spill reader is closed")
	}

	var lenBuf [largeTxnSpillRecordLenSize]byte
	_, err := io.ReadFull(r.file, lenBuf[:])
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, errors.Trace(err)
	}

	recordLen := binary.LittleEndian.Uint64(lenBuf[:])
	if recordLen == 0 {
		return nil, errors.New("large txn spill contains empty record")
	}
	if recordLen > uint64(int(^uint(0)>>1)) {
		return nil, errors.Errorf("large txn spill record is too large: %d", recordLen)
	}

	data := make([]byte, int(recordLen))
	if _, err := io.ReadFull(r.file, data); err != nil {
		return nil, errors.Trace(err)
	}

	entry := &common.RawKVEntry{}
	if err := entry.Decode(data); err != nil {
		return nil, errors.Trace(err)
	}
	return entry, nil
}

func (r *largeTxnInsertSpillReader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	if r.file == nil {
		return nil
	}

	err := r.file.Close()
	r.file = nil
	return errors.Trace(err)
}

func writeLargeTxnSpillRecord(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return errors.Trace(err)
		}
		if n == 0 {
			return errors.New("failed to write large txn spill record")
		}
		data = data[n:]
	}
	return nil
}
