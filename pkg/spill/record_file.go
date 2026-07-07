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
// See the License for the specific language governing permissions and
// limitations under the License.

package spill

import (
	"encoding/binary"
	"io"
	"os"

	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const recordLenSize = 8

// Handle points to one framed record in a RecordFile.
type Handle struct {
	Offset int64
	Length uint64
}

// Valid returns whether the handle points to a non-empty record.
func (h Handle) Valid() bool {
	return h.Length > 0
}

// RecordFile stores temporary length-prefixed records in a local file.
type RecordFile struct {
	path    string
	file    *os.File
	closed  bool
	cleaned bool
}

// NewRecordFile creates a temporary spill file under dir.
func NewRecordFile(dir string, pattern string) (*RecordFile, error) {
	if dir == "" {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("empty spill directory")
	}
	if pattern == "" {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("empty spill file pattern")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "create spill directory")
	}

	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "create spill file")
	}
	return &RecordFile{
		path: file.Name(),
		file: file,
	}, nil
}

// Path returns the underlying temporary file path.
func (s *RecordFile) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Append writes one record and returns its handle.
func (s *RecordFile) Append(data []byte) (Handle, error) {
	return s.AppendChunks(data)
}

// AppendChunks writes one record assembled from chunks without first joining
// them into a single byte slice.
func (s *RecordFile) AppendChunks(chunks ...[]byte) (Handle, error) {
	if s == nil || s.cleaned {
		return Handle{}, cerror.ErrSpillFileOp.GenWithStackByArgs("spill file has been cleaned up")
	}
	if s.closed || s.file == nil {
		return Handle{}, cerror.ErrSpillFileOp.GenWithStackByArgs("spill file is closed")
	}

	recordLen := uint64(0)
	for _, chunk := range chunks {
		recordLen += uint64(len(chunk))
	}
	if recordLen == 0 {
		return Handle{}, cerror.ErrSpillFileOp.GenWithStackByArgs("empty spill record")
	}

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return Handle{}, cerror.WrapError(cerror.ErrSpillFileOp, err, "seek spill file")
	}

	var lenBuf [recordLenSize]byte
	binary.LittleEndian.PutUint64(lenBuf[:], recordLen)
	if err := writeFull(s.file, lenBuf[:]); err != nil {
		return Handle{}, err
	}
	for _, chunk := range chunks {
		if err := writeFull(s.file, chunk); err != nil {
			return Handle{}, err
		}
	}

	return Handle{Offset: offset + recordLenSize, Length: recordLen}, nil
}

// Read reads the record at handle.
func (s *RecordFile) Read(handle Handle) ([]byte, error) {
	if s == nil || s.cleaned {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("spill file has been cleaned up")
	}
	if !handle.Valid() {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("invalid spill record handle")
	}
	if handle.Length > uint64(int(^uint(0)>>1)) {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("spill record is too large")
	}

	file := s.file
	if file == nil {
		var err error
		file, err = os.Open(s.path)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "open spill file")
		}
		defer file.Close()
	}

	data := make([]byte, int(handle.Length))
	if _, err := file.ReadAt(data, handle.Offset); err != nil {
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "read spill record")
	}
	return data, nil
}

// NewReader returns a sequential reader over the spill records.
func (s *RecordFile) NewReader() (*Reader, error) {
	if s == nil || s.cleaned {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("spill file has been cleaned up")
	}
	file, err := os.Open(s.path)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "open spill file")
	}
	return &Reader{file: file}, nil
}

// Close closes the writer side of the spill file.
func (s *RecordFile) Close() error {
	if s == nil || s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}

	err := s.file.Close()
	s.file = nil
	return cerror.WrapError(cerror.ErrSpillFileOp, err, "close spill file")
}

// Cleanup closes and removes the spill file.
func (s *RecordFile) Cleanup() error {
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
		return cerror.WrapError(cerror.ErrSpillFileOp, err, "remove spill file")
	}
	return nil
}

// Reader reads records sequentially from a RecordFile.
type Reader struct {
	file   *os.File
	closed bool
}

// Next returns the next record payload.
func (r *Reader) Next() ([]byte, error) {
	if r == nil || r.closed {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("spill reader is closed")
	}

	var lenBuf [recordLenSize]byte
	_, err := io.ReadFull(r.file, lenBuf[:])
	if err != nil {
		if cerror.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "read spill record length")
	}

	recordLen := binary.LittleEndian.Uint64(lenBuf[:])
	if recordLen == 0 {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("empty spill record")
	}
	if recordLen > uint64(int(^uint(0)>>1)) {
		return nil, cerror.ErrSpillFileOp.GenWithStackByArgs("spill record is too large")
	}

	data := make([]byte, int(recordLen))
	if _, err := io.ReadFull(r.file, data); err != nil {
		return nil, cerror.WrapError(cerror.ErrSpillFileOp, err, "read spill record")
	}
	return data, nil
}

// Close closes the sequential reader.
func (r *Reader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	if r.file == nil {
		return nil
	}

	err := r.file.Close()
	r.file = nil
	return cerror.WrapError(cerror.ErrSpillFileOp, err, "close spill reader")
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return cerror.WrapError(cerror.ErrSpillFileOp, err, "write spill record")
		}
		if n == 0 {
			return cerror.ErrSpillFileOp.GenWithStackByArgs("zero bytes written")
		}
		data = data[n:]
	}
	return nil
}
