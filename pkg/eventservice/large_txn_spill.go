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
	"io"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	recordspill "github.com/pingcap/ticdc/pkg/spill"
)

// largeTxnInsertSpill stores deferred insert rows for a single large transaction.
type largeTxnInsertSpill struct {
	path string
	file *recordspill.RecordFile
}

func newLargeTxnInsertSpill(dir string) (*largeTxnInsertSpill, error) {
	if dir == "" {
		return nil, errors.New("large txn spill dir is empty")
	}
	file, err := recordspill.NewRecordFile(dir, "eventservice-large-txn-insert-*.spill")
	if err != nil {
		return nil, err
	}

	return &largeTxnInsertSpill{
		path: file.Path(),
		file: file,
	}, nil
}

func (s *largeTxnInsertSpill) Append(entry *common.RawKVEntry) error {
	if entry == nil {
		return errors.New("cannot append nil RawKVEntry to large txn spill")
	}

	_, err := s.file.Append(entry.Encode())
	return err
}

func (s *largeTxnInsertSpill) NewReader() (*largeTxnInsertSpillReader, error) {
	if err := s.Close(); err != nil {
		return nil, err
	}

	reader, err := s.file.NewReader()
	if err != nil {
		return nil, err
	}
	return &largeTxnInsertSpillReader{reader: reader}, nil
}

func (s *largeTxnInsertSpill) Close() error {
	if s == nil {
		return nil
	}
	return s.file.Close()
}

func (s *largeTxnInsertSpill) Cleanup() error {
	if s == nil {
		return nil
	}
	return s.file.Cleanup()
}

type largeTxnInsertSpillReader struct {
	reader *recordspill.Reader
}

func (r *largeTxnInsertSpillReader) Next() (*common.RawKVEntry, error) {
	data, err := r.reader.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}

	entry := &common.RawKVEntry{}
	if err := entry.Decode(data); err != nil {
		return nil, errors.Trace(err)
	}
	return entry, nil
}

func (r *largeTxnInsertSpillReader) Close() error {
	if r == nil {
		return nil
	}
	return r.reader.Close()
}
