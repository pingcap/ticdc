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
	"os"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	recordspill "github.com/pingcap/ticdc/pkg/spill"
)

const (
	largeTxnInsertSpillDirName = "eventservice"
	largeTxnInsertSpillPattern = "eventservice-large-txn-insert-*.spill"
)

// largeTxnInsertSpill stores deferred insert rows for a single large transaction.
type largeTxnInsertSpill struct {
	file *recordspill.RecordFile
}

func getLargeTxnInsertSpillDir() string {
	return filepath.Join(config.GetGlobalServerConfig().DataDir, largeTxnInsertSpillDirName)
}

func cleanupLargeTxnInsertSpillFiles(dir string) (int, error) {
	paths, err := filepath.Glob(filepath.Join(dir, largeTxnInsertSpillPattern))
	if err != nil {
		return 0, errors.WrapError(errors.ErrSpillFileOp, err, "list large transaction spill files")
	}

	removed := 0
	for _, path := range paths {
		info, err := os.Lstat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return removed, errors.WrapError(errors.ErrSpillFileOp, err, "stat large transaction spill file")
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(path); err != nil {
			return removed, errors.WrapError(errors.ErrSpillFileOp, err, "remove large transaction spill file")
		}
		removed++
	}
	return removed, nil
}

func newLargeTxnInsertSpill(dir string) (*largeTxnInsertSpill, error) {
	if dir == "" {
		return nil, errors.New("large txn spill dir is empty")
	}
	file, err := recordspill.NewRecordFile(dir, largeTxnInsertSpillPattern)
	if err != nil {
		return nil, err
	}

	return &largeTxnInsertSpill{file: file}, nil
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
