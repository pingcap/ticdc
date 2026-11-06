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

package source

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	storageScanInterval = 2 * time.Second
	storageScanPageSize = 1000
)

type storageMetadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

type storageIndexRange struct {
	start uint64
	end   uint64
}

type storageIndex struct {
	path  string
	key   cloudstorage.DMLPathKey
	index cloudstorage.FileIndex
}

type storageInput struct {
	key   cloudstorage.DMLPathKey
	index cloudstorage.FileIndex
	end   uint64 // The pending range is expanded one file at a time.
}
type storageTableKey struct {
	schema    string
	table     string
	partition int64
}

type storageReader struct {
	tableIDs         map[storageTableKey]int64
	nextTableID      int64
	storage          storeapi.Storage
	memory           *budget.Budget
	continuing       atomic.Bool
	dateSeparator    config.DateSeparator
	fileExtension    string
	fileIndexWidth   int
	sourceCheckpoint uint64
	watermark        uint64
	schemas          map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile
	fileIndices      map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks    map[string]uint64
	pendingSchema    chan struct{} // Header decoding determines the next file groups.
	mu               sync.Mutex
	inputs           []storageInput
	scanned          bool
}

// newStorageReader opens the storage described by the upstream URI.
func newStorageReader(ctx context.Context, opts Options) *storageReader {
	upstreamURI := opts.UpstreamURI
	replicaConfig := opts.ReplicaConfig
	protocol := opts.CodecConfig.Protocol
	memory := opts.Budget
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		log.Panic("Storage client initialization failed", zap.Error(err))
	}
	c := &storageReader{
		storage:        storage,
		memory:         memory,
		dateSeparator:  util.GetOrZero(replicaConfig.Sink.DateSeparator),
		fileExtension:  helper.GetFileExtension(protocol),
		fileIndexWidth: util.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		schemas:        make(map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile),
		fileIndices:    make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		ddlWatermarks:  make(map[string]uint64),
		tableIDs:       make(map[storageTableKey]int64),
	}
	log.Info("storage reader initialized", zap.String("protocol", protocol.String()))
	return c
}

func (c *storageReader) readFile(ctx context.Context, path string) ([]byte, error) {
	reader, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage file")
	}
	defer closeStorageFile(reader, path)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage file")
	}
	return data, nil
}

func (c *storageReader) ddlWatermark(table string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ddlWatermarks[table]
}

func (c *storageReader) scanFiles(ctx context.Context) (map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange, error) {
	// Read already rejects older schema versions after observing their DDL.
	// Retire those caches at the next scan, after its captured inputs are drained.
	for key := range c.fileIndices {
		if key.TableVersion < c.ddlWatermark(key.GetKey()) {
			delete(c.fileIndices, key)
		}
	}
	for key := range c.schemas {
		if key.TableVersion < c.ddlWatermark(key.GetKey()) {
			delete(c.schemas, key)
		}
	}
	files := make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange)
	var paths []*storageIndex
	metadata, err := c.metadataPaths(ctx)
	if err != nil {
		return nil, err
	}
	for _, path := range metadata {
		if cloudstorage.IsSchemaFile(path) {
			key, added, err := c.readSchema(ctx, path)
			if err != nil {
				return nil, err
			}
			if added {
				files[cloudstorage.NewSchemaFileDMLPathKey(key)] = nil
			}
			continue
		}
		var key cloudstorage.DMLPathKey
		if err := key.ParseIndexFilePath(c.dateSeparator, path); err != nil {
			continue // Ignore unsupported index paths in a shared Storage directory.
		}
		if (c.sourceCheckpoint > 0 && key.TableVersion > c.sourceCheckpoint) || key.TableVersion < c.ddlWatermark(key.GetKey()) {
			continue
		}
		paths = append(paths, &storageIndex{
			path: path,
			key:  key,
		})
	}
	if err := c.readIndices(ctx, paths); err != nil {
		return nil, err
	}
	for _, path := range paths {
		key, index := path.key, path.index
		indices := c.fileIndices[key]
		if indices == nil {
			indices = make(map[cloudstorage.FileIndexKey]uint64)
			c.fileIndices[key] = indices
		}
		if _, known := indices[index.FileIndexKey]; !known {
			indices[index.FileIndexKey] = 0
		}

		previous := indices[index.FileIndexKey]
		if index.Idx > previous {
			if files[key] == nil {
				files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
			}
			files[key][index.FileIndexKey] = storageIndexRange{
				start: previous + 1,
				end:   index.Idx,
			}
		}
	}
	return files, nil
}

func (c *storageReader) readIndices(ctx context.Context, paths []*storageIndex) error {
	g, ctx := errgroup.WithContext(ctx)
	workers := min(8, len(paths))
	for worker := range workers {
		g.Go(func() error {
			return c.readIndexWorker(ctx, paths, worker, workers)
		})
	}
	return g.Wait()
}

func (c *storageReader) readIndexWorker(ctx context.Context, paths []*storageIndex, worker, workers int) error {
	for index := worker; index < len(paths) && ctx.Err() == nil; index += workers {
		path := paths[index]
		data, err := c.readFile(ctx, path.path)
		if err != nil {
			return err
		}
		path.index, err = cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
		if err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.sourceCheckpoint > 0 && key.TableVersion > c.sourceCheckpoint) || c.schemas[key] != nil || key.TableVersion < c.ddlWatermark(key.GetKey()) {
		return key, false, nil
	}
	data, err := c.readFile(ctx, path)
	if err != nil {
		return key, false, err
	}
	var file cloudstorage.SchemaFile
	if err := json.Unmarshal(data, &file); err != nil {
		return key, false, errors.WrapError(errors.ErrCodecDecode, err, "decode Storage schema")
	}
	checksumText := strings.TrimSuffix(path[strings.LastIndex(path, "_")+1:], ".json")
	checksum, err := strconv.ParseUint(checksumText, 10, 32)
	if err != nil {
		return key, false, errors.WrapError(errors.ErrStorageSinkInvalidFileName, err, "parse Storage schema checksum")
	}
	if file.Checksum() != uint32(checksum) || key.TableVersion != file.TableVersion {
		return key, false, errors.ErrCodecDecode.FastGenByArgs("Storage schema checksum or table version does not match its path")
	}
	c.schemas[key] = &file
	return key, true, nil
}

func (*storageReader) Streams() []Stream {
	return nil
}

func (c *storageReader) Read(ctx context.Context) ([]*Input, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		c.mu.Lock()
		pendingSchema := c.pendingSchema
		c.mu.Unlock()
		if pendingSchema != nil {
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-pendingSchema:
				c.mu.Lock()
				if c.pendingSchema == pendingSchema {
					c.pendingSchema = nil
				}
				c.mu.Unlock()
			}
		}
		if len(c.inputs) == 0 {
			if c.scanned && c.sourceCheckpoint > c.watermark {
				c.watermark = c.sourceCheckpoint
				// The metadata checkpoint covers every file in the completed scan.
				return []*Input{{Watermark: c.sourceCheckpoint}}, nil
			}
			if err := c.scanInputs(ctx); err != nil {
				return nil, err
			}
			continue
		}
		input := c.inputs[0]
		key := input.key
		tableKey := key.GetKey()
		ddlWatermark := c.ddlWatermark(tableKey)
		switch {
		case input.index.Idx < input.end && key.TableVersion >= ddlWatermark:
			c.inputs[0].index.Idx++
		default:
			c.inputs[0] = storageInput{}
			c.inputs = c.inputs[1:]
		}
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			log.Panic("Storage DML file has no matching schema", zap.String("schema", key.Schema), zap.String("table", key.Table), zap.Uint64("tableVersion", key.TableVersion))
		}
		if key.IsSchemaFileDMLPathKey() && (schema.Query == "" || key.TableVersion <= ddlWatermark) {
			continue
		}
		if key.TableVersion < ddlWatermark {
			c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
			continue
		}
		if key.IsSchemaFileDMLPathKey() {
			pendingSchema = make(chan struct{})
			c.mu.Lock()
			c.pendingSchema = pendingSchema
			c.mu.Unlock()
			return []*Input{{File: &File{Schema: schema}}}, nil
		}
		idKey := storageTableKey{
			schema:    key.Schema,
			table:     key.Table,
			partition: key.PartitionNum,
		}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
		}
		data, err := c.readDMLFile(ctx, input, schema, tableID)
		if err != nil {
			return nil, err
		}
		return []*Input{data}, nil
	}
}

func (c *storageReader) scanInputs(ctx context.Context) error {
	if c.scanned {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(storageScanInterval):
		}
	}
	c.scanned = true
	exists, err := c.storage.FileExists(ctx, "metadata")
	if err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err, "check Storage metadata")
	}
	if exists {
		data, err := c.readFile(ctx, "metadata")
		if err != nil {
			return err
		}
		var metadata storageMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "decode Storage metadata")
		}
		c.sourceCheckpoint = max(c.sourceCheckpoint, metadata.CheckpointTs)
	}
	files, err := c.scanFiles(ctx)
	if err != nil {
		return err
	}
	keys := slices.SortedFunc(maps.Keys(files), cloudstorage.CompareDMLPathKey)
	for _, key := range keys {
		if key.IsSchemaFileDMLPathKey() {
			c.inputs = append(c.inputs, storageInput{key: key})
			continue
		}
		for indexKey, span := range files[key] {
			c.inputs = append(c.inputs, storageInput{
				key: key,
				index: cloudstorage.FileIndex{
					FileIndexKey: indexKey,
					Idx:          span.start,
				},
				end: span.end,
			})
		}
	}
	return nil
}

func (c *storageReader) readDMLFile(ctx context.Context, input storageInput, schema *cloudstorage.SchemaFile, tableID int64) (*Input, error) {
	path := input.key.GenerateDMLFilePath(&input.index, c.fileExtension, c.fileIndexWidth)
	file, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage DML file")
	}
	defer closeStorageFile(file, path)
	size, err := file.GetFileSize()
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "get Storage DML file size")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage DML file")
	}
	continuing := c.continuing.Load()
	if continuing {
		c.memory.ForceAcquire(size)
	}
	if !continuing {
		if err := c.memory.Acquire(ctx, size); err != nil {
			return nil, err
		}
	}
	c.fileIndices[input.key][input.index.FileIndexKey] = input.index.Idx
	return &Input{
		Value: data,
		Bytes: size,
		File:  &File{Schema: schema, TableID: tableID},
	}, nil
}

func (c *storageReader) ContinueReading(enabled bool) {
	c.continuing.Store(enabled)
}

func (c *storageReader) Semantics() Semantics {
	return Semantics{}
}

func (*storageReader) Advance(Progress) bool {
	return true
}

func (c *storageReader) MarkProcessed(tables []event.SchemaTableName, timestamp uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, table := range tables {
		key := common.QuoteSchema(table.SchemaName, table.TableName)
		c.ddlWatermarks[key] = max(c.ddlWatermarks[key], timestamp)
	}
	// The schema DDL is delivered, so the rows that depend on it can be read.
	if c.pendingSchema != nil {
		close(c.pendingSchema)
		c.pendingSchema = nil
	}
}

func (c *storageReader) Close() {
	c.storage.Close()
}

func closeStorageFile(file io.Closer, path string) {
	if err := file.Close(); err != nil {
		log.Warn("storage input file close failed", zap.String("path", path), zap.Error(err))
	}
}

// Discover metadata on every scan so new tables, dates and dispatcher indexes
// remain visible. Data-file contents and sequence numbers come from the indexes.
func (c *storageReader) metadataPaths(ctx context.Context) ([]string, error) {
	var paths []string
	uri := c.storage.URI()
	if root, local := strings.CutPrefix(uri, "file://"); local {
		// WalkDir uses directory entries, avoiding a stat for every data file.
		err := filepath.WalkDir(root, func(name string, entry fs.DirEntry, err error) error {
			return collectLocalMetadata(ctx, root, &paths, name, entry, err)
		})
		return paths, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan local Storage metadata")
	}
	if !strings.HasPrefix(uri, "s3://") && !strings.HasPrefix(uri, "gcs://") {
		return c.listMetadata(ctx)
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
		err := c.storage.WalkDir(ctx, &storeapi.WalkOption{StartAfter: after, ListCount: storageScanPageSize}, func(name string, _ int64) error {
			var err error
			next, cursorIgnored, err = c.scanMetadataEntry(after, name, &paths, &directory, &dataFiles)
			return err
		})
		if err != nil && (!errors.Is(err, io.EOF) || (next == "" && !cursorIgnored)) {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "discover Storage metadata")
		}
		if cursorIgnored {
			// Preserve discovery on compatible services that ignore StartAfter.
			return c.listMetadata(ctx)
		}
		if next == "" {
			return paths, nil
		}
		after = next
	}
}

func (c *storageReader) listMetadata(ctx context.Context) ([]string, error) {
	var paths []string
	err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, func(name string, _ int64) error {
		return collectStorageMetadata(&paths, name)
	})
	return paths, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage metadata")
}

func isStorageMetadata(name string) bool {
	return cloudstorage.IsSchemaFile(name) || strings.HasSuffix(name, ".index")
}

func collectLocalMetadata(ctx context.Context, root string, paths *[]string, name string, entry fs.DirEntry, err error) error {
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
	name = filepath.ToSlash(relative)
	if isStorageMetadata(name) {
		*paths = append(*paths, name)
	}
	return nil
}

func (c *storageReader) scanMetadataEntry(after, name string, paths *[]string, directory *string, dataFiles *int) (string, bool, error) {
	if name <= after {
		return "", true, io.EOF
	}
	filename := path.Base(name)
	if strings.HasPrefix(filename, "CDC") && strings.HasSuffix(filename, c.fileExtension) {
		if parent := path.Dir(name); parent != *directory {
			*directory, *dataFiles = parent, 0
		}
		*dataFiles++
		if *dataFiles >= storageScanPageSize {
			*dataFiles = 0
			_, err := cloudstorage.ParseFileIndexFromFileName(filename, c.fileExtension)
			var key cloudstorage.DMLPathKey
			if err == nil && key.ParseIndexFilePath(c.dateSeparator, path.Join(*directory, "meta", "CDC.index")) == nil {
				// Skip data-file names while preserving metadata and later siblings.
				return path.Join(*directory, "CDD"), false, io.EOF
			}
		}
	}
	return "", false, collectStorageMetadata(paths, name)
}

func collectStorageMetadata(paths *[]string, name string) error {
	if isStorageMetadata(name) {
		*paths = append(*paths, name)
	}
	return nil
}
