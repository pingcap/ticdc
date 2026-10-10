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
	"encoding/json"
	"io"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const storageScanInterval = 2 * time.Second

type storageMetadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

type storageIndexRange struct {
	start uint64
	end   uint64
}

type storageInput struct {
	key      cloudstorage.DMLPathKey
	index    cloudstorage.FileIndex
	end      uint64 // The pending range is expanded one file at a time.
	groupEnd bool
	sort     bool
}
type storageReader struct {
	storage        storeapi.Storage
	memory         *memoryUsage
	dateSeparator  config.DateSeparator
	fileExtension  string
	fileIndexWidth int
	checkpoint     uint64
	schemas        map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile
	fileIndices    map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks  map[string]uint64
	mu             sync.Mutex
	records        []*ack
	inputs         []storageInput
	scanned        bool
}

func newStorageReader(ctx context.Context, upstreamURI *url.URL, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (*storageReader, error) {
	if upstreamURI.Scheme == config.FileScheme && upstreamURI.Path == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("file upstream-uri must include a path")
	}
	if upstreamURI.Scheme != config.FileScheme && upstreamURI.Host == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("object storage upstream-uri must include a bucket")
	}
	if err := replicaConfig.ValidateAndAdjust(upstreamURI); err != nil {
		return nil, err
	}
	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, err
	}
	if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
		return nil, errors.ErrStorageSinkInvalidConfig.FastGenByArgs("Storage consumer requires csv or canal-json")
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		return nil, err
	}

	c := &storageReader{
		storage: storage, memory: memory,
		dateSeparator: putil.GetOrZero(replicaConfig.Sink.DateSeparator), fileExtension: helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		schemas:        make(map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile), fileIndices: make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		ddlWatermarks: make(map[string]uint64),
	}
	log.Info("Storage reader initialized", zap.String("protocol", protocol.String()))
	return c, nil
}

func (c *storageReader) readFile(ctx context.Context, path string, limit int64) (data []byte, err error) {
	if limit <= 0 {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer has no buffer capacity for a Storage file")
	}
	reader, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage file")
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = errors.WrapError(errors.ErrExternalStorageAPI, closeErr, "close Storage file")
		}
	}()
	data, err = io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage file")
	}
	if int64(len(data)) > limit {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage file exceeds the consumer buffer limit")
	}
	return data, nil
}

func (c *storageReader) scanFiles(ctx context.Context) (map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange, error) {
	files := make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange)
	err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, _ int64) error {
		if cloudstorage.IsSchemaFile(path) {
			key, added, err := c.readSchema(ctx, path)
			if err != nil {
				return err
			}
			if added {
				files[cloudstorage.NewSchemaFileDMLPathKey(key)] = nil
			}
			return nil
		}
		if !strings.HasSuffix(path, ".index") {
			return nil
		}
		var key cloudstorage.DMLPathKey
		if err := key.ParseIndexFilePath(c.dateSeparator, path); err != nil {
			return nil //nolint:nilerr // Ignore unsupported index paths in a shared Storage directory.
		}
		if c.checkpoint > 0 && key.TableVersion > c.checkpoint {
			return nil
		}
		data, err := c.readFile(ctx, path, maxRecordBytes)
		if err != nil {
			return err
		}
		index, err := cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
		if err != nil {
			return err
		}
		indices := c.fileIndices[key]
		if indices == nil {
			if len(c.fileIndices) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage path cache exceeds its count limit")
			}
			indices = make(map[cloudstorage.FileIndexKey]uint64)
			c.fileIndices[key] = indices
			bytes := int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
			if err := c.memory.reserve(ctx, bytes); err != nil {
				return err
			}
		}
		if _, known := indices[index.FileIndexKey]; !known {
			if len(indices) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage index cache exceeds its resource limit")
			}
			indices[index.FileIndexKey] = 0
			bytes := int64(len(index.DispatcherID) + 128)
			if err := c.memory.reserve(ctx, bytes); err != nil {
				return err
			}
		}

		previous := indices[index.FileIndexKey]
		if index.Idx > previous {
			if files[key] == nil {
				files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
			}
			files[key][index.FileIndexKey] = storageIndexRange{start: previous + 1, end: index.Idx}
		}
		return nil
	})
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage files")
	}
	return files, nil
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || c.schemas[key] != nil {
		return key, false, nil
	}
	data, err := c.readFile(ctx, path, maxRecordBytes)
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
	bytes := int64(len(data))*4 + 1024
	if err := c.memory.reserve(ctx, bytes); err != nil {
		return key, false, err
	}
	c.schemas[key] = &file
	return key, true, nil
}

func (c *storageReader) Read(ctx context.Context) (*readData, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if len(c.inputs) == 0 {
			if c.scanned {
				select {
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				case <-time.After(storageScanInterval):
				}
			}
			c.scanned = true
			exists, err := c.storage.FileExists(ctx, "metadata")
			if err != nil {
				return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "check Storage metadata")
			}
			if exists {
				data, err := c.readFile(ctx, "metadata", maxRecordBytes)
				if err != nil {
					return nil, err
				}
				var metadata storageMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					return nil, errors.WrapError(errors.ErrCodecDecode, err, "decode Storage metadata")
				}
				c.checkpoint = max(c.checkpoint, metadata.CheckpointTs)
			}
			files, err := c.scanFiles(ctx)
			if err != nil {
				return nil, err
			}
			keys := slices.SortedFunc(maps.Keys(files), cloudstorage.CompareDMLPathKey)
			for _, key := range keys {
				if key.IsSchemaFileDMLPathKey() {
					if err := c.memory.reserve(ctx, 256); err != nil {
						return nil, err
					}
					c.inputs = append(c.inputs, storageInput{key: key})
					continue
				}
				sortBeforeWrite := false
				for indexKey := range files[key] {
					sortBeforeWrite = sortBeforeWrite || indexKey.EnableTableAcrossNodes
				}
				for indexKey, span := range files[key] {
					if err := c.memory.reserve(ctx, 256); err != nil {
						return nil, err
					}
					c.inputs = append(c.inputs, storageInput{key: key, index: cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: span.start}, end: span.end, sort: sortBeforeWrite})
				}
				if err := c.memory.reserve(ctx, 256); err != nil {
					return nil, err
				}
				c.inputs = append(c.inputs, storageInput{key: key, groupEnd: true})
			}
			continue
		}
		input := c.inputs[0]
		key := input.key
		tableKey := key.GetKey()
		if input.index.Idx < input.end && key.TableVersion >= c.ddlWatermarks[tableKey] {
			c.inputs[0].index.Idx++
		} else {
			c.inputs[0] = storageInput{}
			c.inputs = c.inputs[1:]
			c.memory.release(256)
		}
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			return nil, errors.ErrCodecDecode.FastGenByArgs("Storage DML file has no matching schema")
		}
		if key.IsSchemaFileDMLPathKey() {
			if schema.Query == "" || key.TableVersion <= c.ddlWatermarks[tableKey] {
				continue
			}
			size := int64(len(schema.Query) + len(key.Schema) + len(key.Table) + 1024)
			record, err := c.memory.newAck(ctx, size)
			if err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.records = append(c.records, record)
			c.mu.Unlock()
			return &readData{storage: &input, schema: schema, record: record}, nil
		}
		if key.TableVersion < c.ddlWatermarks[tableKey] {
			if !input.groupEnd {
				c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
			}
			continue
		}
		if input.groupEnd {
			return &readData{storage: &input, schema: schema}, nil
		}
		path := key.GenerateDMLFilePath(&input.index, c.fileExtension, c.fileIndexWidth)
		file, err := c.storage.Open(ctx, path, nil)
		if err != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage DML file")
		}
		size, err := file.GetFileSize()
		if err != nil {
			_ = file.Close()
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "get Storage DML file size")
		}
		if size < 0 {
			_ = file.Close()
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage DML file has a negative size")
		}
		record, err := c.memory.newAck(ctx, size+256)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, size+1))
		closeErr := file.Close()
		if readErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, readErr, "read Storage DML file")
		}
		if closeErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, closeErr, "close Storage DML file")
		}
		if int64(len(data)) != size {
			return nil, errors.ErrExternalStorageAPI.FastGenByArgs("Storage DML file size changed while reading")
		}
		c.mu.Lock()
		c.records = append(c.records, record)
		c.mu.Unlock()
		c.fileIndices[key][input.index.FileIndexKey] = input.index.Idx
		return &readData{value: data, storage: &input, schema: schema, record: record}, nil
	}
}

func (c *storageReader) Confirm(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, record := range c.records {
		refs := record.refs.Load()
		if refs < 0 {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Storage input completed more than once")
		}
		if refs != 0 {
			break
		}
		c.memory.confirm(record)
		count++
	}
	c.records = slices.Delete(c.records, 0, count)
	return nil
}

func (c *storageReader) Close() error {
	c.storage.Close()
	return nil
}
