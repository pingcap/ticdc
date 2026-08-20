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

package reader

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
)

// framedFileWriter writes reader-owned temporary sorted files in redo framing.
type framedFileWriter struct {
	path     string
	tempPath string
	file     *os.File
	writer   *pioutil.PageWriter
	lenBuf   [8]byte
}

func newFramedFileWriter(path string) (*framedFileWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), redo.DefaultDirMode); err != nil {
		return nil, errors.WrapError(errors.ErrRedoFileOp, err)
	}
	return &framedFileWriter{
		path:     path,
		tempPath: path + redo.TmpEXT,
	}, nil
}

func (w *framedFileWriter) Write(data []byte) error {
	if w.file == nil {
		file, err := os.OpenFile(
			w.tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, redo.DefaultFileMode)
		if err != nil {
			return errors.WrapError(errors.ErrRedoFileOp, err)
		}
		w.file = file
		w.writer = pioutil.NewPageWriter(file, redo.PageBytes, 0)
	}

	lenField, padBytes := writer.EncodeFrameSize(len(data))
	binary.LittleEndian.PutUint64(w.lenBuf[:], lenField)
	if _, err := w.writer.Write(w.lenBuf[:]); err != nil {
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	if _, err := w.writer.Write(data); err != nil {
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	if padBytes != 0 {
		var padding [8]byte
		if _, err := w.writer.Write(padding[:padBytes]); err != nil {
			return errors.WrapError(errors.ErrRedoFileOp, err)
		}
	}
	return nil
}

func (w *framedFileWriter) Close() error {
	if w.file == nil {
		return nil
	}

	if _, err := w.writer.FlushN(); err != nil {
		w.Abort()
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	if err := w.file.Sync(); err != nil {
		w.Abort()
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	if err := w.file.Close(); err != nil {
		w.file = nil
		_ = os.Remove(w.tempPath)
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	w.file = nil
	if err := os.Rename(w.tempPath, w.path); err != nil {
		_ = os.Remove(w.tempPath)
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}

	dir, err := os.Open(filepath.Dir(w.path))
	if err != nil {
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.WrapError(errors.ErrRedoFileOp, err)
	}
	return nil
}

func (w *framedFileWriter) Abort() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	_ = os.Remove(w.tempPath)
}
