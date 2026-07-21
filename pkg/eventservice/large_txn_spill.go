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
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/encryption"
	"github.com/pingcap/ticdc/pkg/errors"
	recordspill "github.com/pingcap/ticdc/pkg/spill"
)

const (
	largeTxnInsertSpillDirName = "eventservice"
	largeTxnInsertSpillPattern = "eventservice-large-txn-insert-*.spill"
)

// largeTxnInsertSpill stores deferred insert rows for a single large transaction.
type largeTxnInsertSpill struct {
	file              *recordspill.RecordFile
	keyspaceID        uint32
	encryptionManager encryption.EncryptionManager
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

func newLargeTxnInsertSpill(dir string, keyspaceID uint32) (*largeTxnInsertSpill, error) {
	encryptionManager, _ := appcontext.TryGetService[encryption.EncryptionManager](appcontext.EncryptionManager)
	return newLargeTxnInsertSpillWithEncryption(dir, keyspaceID, encryptionManager)
}

func newLargeTxnInsertSpillWithEncryption(
	dir string,
	keyspaceID uint32,
	encryptionManager encryption.EncryptionManager,
) (*largeTxnInsertSpill, error) {
	if dir == "" {
		return nil, errors.ErrSpillFileOp.GenWithStackByArgs("empty large transaction spill directory")
	}
	file, err := recordspill.NewRecordFile(dir, largeTxnInsertSpillPattern)
	if err != nil {
		return nil, err
	}

	return &largeTxnInsertSpill{
		file:              file,
		keyspaceID:        keyspaceID,
		encryptionManager: encryptionManager,
	}, nil
}

func (s *largeTxnInsertSpill) Append(ctx context.Context, entry *common.RawKVEntry) error {
	if entry == nil {
		return errors.ErrSpillFileOp.GenWithStackByArgs("cannot append nil RawKVEntry")
	}

	data := entry.Encode()
	var err error
	if s.encryptionManager != nil {
		data, err = s.encryptionManager.EncryptData(ctx, s.keyspaceID, data)
		if err != nil {
			return err
		}
	}
	_, err = s.file.Append(data)
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
	return &largeTxnInsertSpillReader{
		reader:            reader,
		keyspaceID:        s.keyspaceID,
		encryptionManager: s.encryptionManager,
	}, nil
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
	reader            *recordspill.Reader
	keyspaceID        uint32
	encryptionManager encryption.EncryptionManager
}

func (r *largeTxnInsertSpillReader) Next(ctx context.Context) (*common.RawKVEntry, error) {
	data, err := r.reader.Next()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	if r.encryptionManager != nil {
		data, err = r.encryptionManager.DecryptData(ctx, r.keyspaceID, data)
		if err != nil {
			return nil, err
		}
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
