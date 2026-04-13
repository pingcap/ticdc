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

package consumer

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

var ErrWalkDirEnd = perrors.Normalize("walk dir end", perrors.RFCCodeText("CDC:ErrWalkDirEnd"))

type newFileDiscoverer interface {
	discoverAndDownloadNewTableVersions(ctx context.Context, schema, table string) ([]types.VersionKey, error)
	downloadSchemaFilesWithScanRange(
		ctx context.Context,
		schema, table string,
		startVersionKey string,
		endVersionKey string,
		endDataPath string,
	) ([]types.VersionKey, error)
	getNewFilesForSchemaPathKey(ctx context.Context, schema, table string, version *types.VersionKey) (map[cloudstorage.DmlPathKey]fileIndexRange, error)
	getNewFilesForSchemaPathKeyWithEndPath(
		ctx context.Context,
		schema, table string,
		version uint64,
		startDataPath string,
		endDataPath string,
	) (map[cloudstorage.DmlPathKey]fileIndexRange, error)
}

func NewNewFileDiscoverer(c *S3Consumer, enableListByFileIndex bool) newFileDiscoverer {
	if enableListByFileIndex {
		return &indexBasedNewFileDiscoverer{
			consumer:      c,
			lastSchemaSeq: make(map[schemaKey]uint64),
			lastDataSeq:   make(map[cloudstorage.SchemaPathKey]uint64),
		}
	}
	return &directoryBasedNewFileDiscoverer{
		consumer: c,
	}
}

type directoryBasedNewFileDiscoverer struct {
	consumer *S3Consumer
}

func (c *directoryBasedNewFileDiscoverer) discoverAndDownloadNewTableVersions(
	ctx context.Context,
	schema, table string,
) ([]types.VersionKey, error) {
	currentVersion := c.consumer.currentTableVersion.GetCurrentTableVersion(schema, table)
	metaSubDir := fmt.Sprintf("%s/%s/meta/", schema, table)
	opt := &storage.WalkOption{
		SubDir:     metaSubDir,
		ObjPrefix:  "schema_",
		StartAfter: currentVersion.VersionPath,
	}

	var scanVersions []types.VersionKey
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	if err := func() error {
		if err := c.consumer.acquireWalkSlot(ctx); err != nil {
			return errors.Trace(err)
		}
		defer c.consumer.releaseWalkSlot()
		return c.consumer.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
			if !cloudstorage.IsSchemaFile(filePath) {
				return nil
			}
			var schemaKey cloudstorage.SchemaPathKey
			_, err := schemaKey.ParseSchemaFilePath(filePath)
			if err != nil {
				log.Error("failed to parse schema file path, skipping",
					zap.String("path", filePath),
					zap.Error(err))
				return nil
			}
			version := schemaKey.TableVersion
			if version > currentVersion.Version {
				if _, exists := newVersionPaths[schemaKey]; !exists {
					scanVersions = append(scanVersions, types.VersionKey{
						Version:     version,
						VersionPath: filePath,
					})
				}
				newVersionPaths[schemaKey] = filePath
			}
			return nil
		})
	}(); err != nil {
		return nil, errors.Trace(err)
	}

	// download new version schema files concurrently
	if err := c.consumer.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}

	if currentVersion.Version > 0 {
		scanVersions = append(scanVersions, currentVersion)
	}
	return scanVersions, nil
}

func (c *directoryBasedNewFileDiscoverer) downloadSchemaFilesWithScanRange(
	ctx context.Context,
	schema, table string,
	startVersionKey string,
	endVersionKey string,
	endDataPath string,
) ([]types.VersionKey, error) {
	metaSubDir := fmt.Sprintf("%s/%s/meta/", schema, table)
	opt := &storage.WalkOption{
		SubDir:     metaSubDir,
		ObjPrefix:  "schema_",
		StartAfter: startVersionKey,
	}

	var startSchemaKey, endSchemaKey cloudstorage.SchemaPathKey
	_, err := startSchemaKey.ParseSchemaFilePath(startVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = endSchemaKey.ParseSchemaFilePath(endVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var scanVersions []types.VersionKey
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	scanVersions = append(scanVersions, types.VersionKey{
		Version:     startSchemaKey.TableVersion,
		VersionPath: startVersionKey,
	})
	newVersionPaths[startSchemaKey] = startVersionKey
	if err := func() error {
		if err := c.consumer.acquireWalkSlot(ctx); err != nil {
			return errors.Trace(err)
		}
		defer c.consumer.releaseWalkSlot()
		return c.consumer.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
			if endVersionKey < filePath {
				return ErrWalkDirEnd
			}
			if !cloudstorage.IsSchemaFile(filePath) {
				return nil
			}
			var schemaKey cloudstorage.SchemaPathKey
			_, err := schemaKey.ParseSchemaFilePath(filePath)
			if err != nil {
				log.Error("failed to parse schema file path, skipping",
					zap.String("path", filePath),
					zap.Error(err))
				return nil
			}
			if schemaKey.TableVersion > startSchemaKey.TableVersion {
				if _, exists := newVersionPaths[schemaKey]; !exists {
					scanVersions = append(scanVersions, types.VersionKey{
						Version:     schemaKey.TableVersion,
						VersionPath: filePath,
					})
				}
				newVersionPaths[schemaKey] = filePath
			}
			return nil
		})
	}(); err != nil && !errors.Is(err, ErrWalkDirEnd) {
		return nil, errors.Trace(err)
	}

	if err := c.consumer.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}

	c.consumer.currentTableVersion.UpdateCurrentTableVersion(schema, table, types.VersionKey{
		Version:     endSchemaKey.TableVersion,
		VersionPath: endVersionKey,
		DataPath:    endDataPath,
	})

	return scanVersions, nil
}

func (c *directoryBasedNewFileDiscoverer) getNewFilesForSchemaPathKey(
	ctx context.Context,
	schema, table string,
	version *types.VersionKey,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	schemaPrefix := path.Join(schema, table, fmt.Sprintf("%d", version.Version))
	opt := &storage.WalkOption{
		SubDir:     schemaPrefix,
		StartAfter: version.DataPath,
	}

	newTableDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
	maxFilePath := ""
	if err := func() error {
		if err := c.consumer.acquireWalkSlot(ctx); err != nil {
			return errors.Trace(err)
		}
		defer c.consumer.releaseWalkSlot()
		return c.consumer.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
			// Try to parse DML file path if it matches the expected extension
			if strings.HasSuffix(filePath, c.consumer.fileExtension) {
				var dmlkey cloudstorage.DmlPathKey
				fileIdx, err := dmlkey.ParseDMLFilePath(c.consumer.dateSeparator, filePath)
				if err != nil {
					log.Error("failed to parse dml file path, skipping",
						zap.String("path", filePath),
						zap.Error(err))
					return nil
				}
				updateTableDMLIdxMap(newTableDMLIdxMap, dmlkey, fileIdx)
				maxFilePath = filePath
			}
			return nil
		})
	}(); err != nil {
		return nil, errors.Trace(err)
	}
	if len(maxFilePath) > 0 {
		version.DataPath = maxFilePath
	}
	return c.consumer.tableDMLIdx.DiffNewTableDMLIdxMap(newTableDMLIdxMap), nil
}

func (c *directoryBasedNewFileDiscoverer) getNewFilesForSchemaPathKeyWithEndPath(
	ctx context.Context,
	schema, table string,
	version uint64,
	startDataPath string,
	endDataPath string,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	schemaPrefix := path.Join(schema, table, fmt.Sprintf("%d", version))
	opt := &storage.WalkOption{
		SubDir:     schemaPrefix,
		StartAfter: startDataPath,
	}
	{
		var startDmlkey cloudstorage.DmlPathKey
		startFileIdx, err := startDmlkey.ParseDMLFilePath(c.consumer.dateSeparator, startDataPath)
		if err != nil {
			log.Error("failed to parse start dml file path, skipping",
				zap.String("path", startDataPath),
				zap.Error(err))
		} else {
			c.consumer.tableDMLIdx.UpdateDMLIdxMapByStartPath(startDmlkey, startFileIdx)
		}
	}

	newTableDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
	if err := func() error {
		if err := c.consumer.acquireWalkSlot(ctx); err != nil {
			return errors.Trace(err)
		}
		defer c.consumer.releaseWalkSlot()
		return c.consumer.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
			if endDataPath < filePath {
				return ErrWalkDirEnd
			}
			// Try to parse DML file path if it matches the expected extension
			if strings.HasSuffix(filePath, c.consumer.fileExtension) {
				var dmlkey cloudstorage.DmlPathKey
				fileIdx, err := dmlkey.ParseDMLFilePath(c.consumer.dateSeparator, filePath)
				if err != nil {
					log.Error("failed to parse dml file path, skipping",
						zap.String("path", filePath),
						zap.Error(err))
					return nil
				}
				updateTableDMLIdxMap(newTableDMLIdxMap, dmlkey, fileIdx)
			}
			return nil
		})
	}(); err != nil && !errors.Is(err, ErrWalkDirEnd) {
		return nil, errors.Trace(err)
	}
	return c.consumer.tableDMLIdx.DiffNewTableDMLIdxMap(newTableDMLIdxMap), nil
}

type indexBasedNewFileDiscoverer struct {
	consumer      *S3Consumer
	mu            sync.RWMutex
	lastSchemaSeq map[schemaKey]uint64
	lastDataSeq   map[cloudstorage.SchemaPathKey]uint64
}

func (c *indexBasedNewFileDiscoverer) getLastSchemaSeq(key schemaKey) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastSchemaSeq[key]
}

func (c *indexBasedNewFileDiscoverer) setLastSchemaSeq(key schemaKey, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSchemaSeq[key] = seq
}

func (c *indexBasedNewFileDiscoverer) getLastDataSeq(key cloudstorage.SchemaPathKey) uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastDataSeq[key]
}

func (c *indexBasedNewFileDiscoverer) setLastDataSeq(key cloudstorage.SchemaPathKey, seq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastDataSeq[key] = seq
}

func (c *indexBasedNewFileDiscoverer) discoverAndDownloadNewTableVersions(
	ctx context.Context,
	schema, table string,
) ([]types.VersionKey, error) {
	currentVersion := c.consumer.currentTableVersion.GetCurrentTableVersion(schema, table)
	metaIndexPath := cloudstorage.GenerateSchemaMetaIndexFilePath(schema, table)
	content, err := c.consumer.s3Storage.ReadFile(ctx, metaIndexPath)
	if err != nil {
		if util.IsNotExistInExtStorage(err) {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	latestSeq, err := cloudstorage.FetchSchemaMetaIndexFromFileName(strings.TrimSpace(string(content)))
	if err != nil {
		return nil, errors.Annotatef(err, "invalid schema meta index value: %s", strings.TrimSpace(string(content)))
	}

	scanVersions := make([]types.VersionKey, 0)
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	tableKey := schemaKey{schema: schema, table: table}
	startSeq := c.getLastSchemaSeq(tableKey)
	for seq := startSeq + 1; seq <= latestSeq; seq++ {
		versionPath := cloudstorage.GenerateSchemaMetaIndexDataFilePath(schema, table, seq)
		data, err := c.consumer.s3Storage.ReadFile(ctx, versionPath)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to read schema version index file: %s", versionPath)
		}
		schemaPath := strings.TrimSpace(string(data))
		if !cloudstorage.IsSchemaFile(schemaPath) {
			return nil, errors.Errorf("schema version index points to invalid schema path: %s", schemaPath)
		}
		var schemaKey cloudstorage.SchemaPathKey
		if _, err := schemaKey.ParseSchemaFilePath(schemaPath); err != nil {
			return nil, errors.Trace(err)
		}
		version := schemaKey.TableVersion
		if version > currentVersion.Version {
			if _, exists := newVersionPaths[schemaKey]; !exists {
				scanVersions = append(scanVersions, types.VersionKey{
					Version:     schemaKey.TableVersion,
					VersionPath: schemaPath,
				})
			}
			newVersionPaths[schemaKey] = schemaPath
		}
	}
	if len(newVersionPaths) > 0 {
		if err := c.consumer.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
			return nil, errors.Trace(err)
		}
	}
	c.setLastSchemaSeq(tableKey, latestSeq)
	if currentVersion.Version > 0 {
		scanVersions = append(scanVersions, currentVersion)
	}
	return scanVersions, nil
}

func (c *indexBasedNewFileDiscoverer) downloadSchemaFilesWithScanRange(
	ctx context.Context,
	schema, table string,
	startVersionKey string,
	endVersionKey string,
	endDataPath string,
) ([]types.VersionKey, error) {
	var startSchemaKey, endSchemaKey cloudstorage.SchemaPathKey
	_, err := startSchemaKey.ParseSchemaFilePath(startVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = endSchemaKey.ParseSchemaFilePath(endVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	scanVersions := make([]types.VersionKey, 0, int(endSchemaKey.TableVersion-startSchemaKey.TableVersion+1))
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	scanVersions = append(scanVersions, types.VersionKey{
		Version:     startSchemaKey.TableVersion,
		VersionPath: startVersionKey,
	})
	newVersionPaths[startSchemaKey] = startVersionKey
	metaIndexPath := cloudstorage.GenerateSchemaMetaIndexFilePath(schema, table)
	content, err := c.consumer.s3Storage.ReadFile(ctx, metaIndexPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	latestSeq, err := cloudstorage.FetchSchemaMetaIndexFromFileName(strings.TrimSpace(string(content)))
	if err != nil {
		return nil, errors.Annotatef(err, "invalid schema meta index value: %s", strings.TrimSpace(string(content)))
	}
	for seq := uint64(1); seq <= latestSeq; seq++ {
		versionPath := cloudstorage.GenerateSchemaMetaIndexDataFilePath(schema, table, seq)
		data, err := c.consumer.s3Storage.ReadFile(ctx, versionPath)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to read schema version index file: %s", versionPath)
		}
		schemaPath := strings.TrimSpace(string(data))
		if !cloudstorage.IsSchemaFile(schemaPath) {
			return nil, errors.Errorf("schema version index points to invalid schema path: %s", schemaPath)
		}
		var schemaKey cloudstorage.SchemaPathKey
		if _, err := schemaKey.ParseSchemaFilePath(schemaPath); err != nil {
			return nil, errors.Trace(err)
		}
		if endSchemaKey.TableVersion < schemaKey.TableVersion {
			latestSeq = seq - 1
			break
		}
		if schemaKey.TableVersion > startSchemaKey.TableVersion {
			if _, exists := newVersionPaths[schemaKey]; !exists {
				scanVersions = append(scanVersions, types.VersionKey{
					Version:     schemaKey.TableVersion,
					VersionPath: schemaPath,
				})
			}
			newVersionPaths[schemaKey] = schemaPath
		}
	}
	if err := c.consumer.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}
	c.consumer.currentTableVersion.UpdateCurrentTableVersion(schema, table, types.VersionKey{
		Version:     endSchemaKey.TableVersion,
		VersionPath: endVersionKey,
		DataPath:    endDataPath,
	})
	c.setLastSchemaSeq(schemaKey{schema: schema, table: table}, latestSeq)
	return scanVersions, nil
}

func (c *indexBasedNewFileDiscoverer) readIndexFile(
	ctx context.Context,
	schema, table string,
	version uint64,
) (uint64, bool, error) {
	indexFilePath := path.Join(schema, table, fmt.Sprintf("%d", version), "meta/CDC.index")
	content, err := c.consumer.s3Storage.ReadFile(ctx, indexFilePath)
	if err != nil {
		if util.IsNotExistInExtStorage(err) {
			return 0, false, nil
		}
		return 0, false, errors.Trace(err)
	}
	seq, err := cloudstorage.FetchIndexFromFileName(strings.TrimSpace(string(content)), c.consumer.fileExtension)
	return seq, true, errors.Trace(err)
}

func (c *indexBasedNewFileDiscoverer) getNewFilesForSchemaPathKey(
	ctx context.Context,
	schema, table string,
	version *types.VersionKey,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	latestSeq, exists, err := c.readIndexFile(ctx, schema, table, version.Version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, nil
	}

	schemaPathKey := cloudstorage.SchemaPathKey{Schema: schema, Table: table, TableVersion: version.Version}
	newFiles := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	startSeq := c.getLastDataSeq(schemaPathKey)
	newFiles[cloudstorage.DmlPathKey{SchemaPathKey: schemaPathKey, PartitionNum: 0, Date: ""}] = fileIndexRange{
		cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}: indexRange{
			start: startSeq + 1,
			end:   latestSeq,
		},
	}
	c.setLastDataSeq(schemaPathKey, latestSeq)
	return newFiles, nil
}

func (c *indexBasedNewFileDiscoverer) getNewFilesForSchemaPathKeyWithEndPath(
	ctx context.Context,
	schema, table string,
	version uint64,
	startDataPath string,
	endDataPath string,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	latestSeq, exists, err := c.readIndexFile(ctx, schema, table, version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, errors.Errorf("index file not found: %s.%s", schema, table)
	}
	startSeq := uint64(0)
	if startDataPath != "" {
		var startDmlKey cloudstorage.DmlPathKey
		startFileIdx, err := startDmlKey.ParseDMLFilePath(c.consumer.dateSeparator, startDataPath)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to parse start dml file path: %s", startDataPath)
		}
		startSeq = startFileIdx.Idx
	}

	endSeq := latestSeq
	if endDataPath != "" {
		var endDmlKey cloudstorage.DmlPathKey
		endFileIdx, err := endDmlKey.ParseDMLFilePath(c.consumer.dateSeparator, endDataPath)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to parse end dml file path: %s", endDataPath)
		}
		if endFileIdx.Idx < endSeq {
			endSeq = endFileIdx.Idx
		}
	}
	if endSeq <= startSeq {
		return map[cloudstorage.DmlPathKey]fileIndexRange{}, nil
	}

	schemaPathKey := cloudstorage.SchemaPathKey{Schema: schema, Table: table, TableVersion: version}
	dmlKey := cloudstorage.DmlPathKey{SchemaPathKey: schemaPathKey, PartitionNum: 0, Date: ""}
	newFiles := map[cloudstorage.DmlPathKey]fileIndexRange{
		dmlKey: {
			cloudstorage.FileIndexKey{DispatcherID: "", EnableTableAcrossNodes: false}: {
				start: startSeq + 1,
				end:   endSeq,
			},
		},
	}
	c.setLastDataSeq(schemaPathKey, endSeq)
	return newFiles, nil
}
