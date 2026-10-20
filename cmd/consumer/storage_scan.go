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

package main

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const storageScanPageSize = 1000

// Discover metadata on every scan so new tables, dates and dispatcher indexes
// remain visible. Data-file contents and sequence numbers come from the indexes.
func (c *storageReader) metadataPaths(ctx context.Context) ([]string, error) {
	var paths []string
	collect := func(name string, _ int64) error {
		if cloudstorage.IsSchemaFile(name) || strings.HasSuffix(name, ".index") {
			paths = append(paths, name)
		}
		return nil
	}
	uri := c.storage.URI()
	if root, local := strings.CutPrefix(uri, "file://"); local {
		// WalkDir uses directory entries, avoiding a stat for every data file.
		err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			if err := context.Cause(ctx); err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			relative, err := filepath.Rel(root, name)
			if err != nil {
				return err
			}
			return collect(filepath.ToSlash(relative), 0)
		})
		return paths, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan local Storage metadata")
	}
	if !strings.HasPrefix(uri, "s3://") && !strings.HasPrefix(uri, "gcs://") {
		err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, collect)
		return paths, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage metadata")
	}

	// These backends support ordered listing with StartAfter. Small directories
	// use normal listing; a full page of data files lets us skip the remaining
	// sequences in that directory without adding requests for small directories.
	var after string
	for {
		var (
			next          string
			directory     string
			dataFiles     int
			cursorIgnored bool
		)
		err := c.storage.WalkDir(ctx, &storeapi.WalkOption{StartAfter: after, ListCount: storageScanPageSize}, func(name string, size int64) error {
			if name <= after {
				cursorIgnored = true
				return io.EOF
			}
			filename := path.Base(name)
			if strings.HasPrefix(filename, "CDC") && strings.HasSuffix(filename, c.fileExtension) {
				if parent := path.Dir(name); parent != directory {
					directory, dataFiles = parent, 0
				}
				dataFiles++
				if dataFiles >= storageScanPageSize {
					dataFiles = 0
					_, err := cloudstorage.ParseFileIndexFromFileName(filename, c.fileExtension)
					var key cloudstorage.DMLPathKey
					if err == nil && key.ParseIndexFilePath(c.dateSeparator, path.Join(directory, "meta", "CDC.index")) == nil {
						// All CDC data-file names precede CDD in byte order. The
						// meta directory and later sibling keys remain visible.
						next = path.Join(directory, "CDD")
						return io.EOF
					}
				}
			}
			return collect(name, size)
		})
		if err != nil && !(errors.Is(err, io.EOF) && (next != "" || cursorIgnored)) {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "discover Storage metadata")
		}
		if cursorIgnored {
			// Preserve discovery on compatible services that ignore StartAfter.
			paths = nil
			err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, collect)
			return paths, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage metadata")
		}
		if next == "" {
			return paths, nil
		}
		after = next
	}
}
