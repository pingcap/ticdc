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
	"maps"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const (
	storageScanInterval = 2 * time.Second
	// The deepest data directory is <schema>/<table>/<version>/<partition>/<date>.
	storageDiscoveryDepth = 5
	defaultS3Region       = "us-east-1"
)

type storageMetadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

type storageIndexRange struct {
	start uint64
	end   uint64
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
	// Listing a directory needs a client that returns common prefixes.
	scheme     string
	bucket     string
	bucketPath string
	localPath  string
	s3Client   *s3.Client
}

// newStorageReader opens the storage described by the upstream URI.
func newStorageReader(ctx context.Context, opts Options) *storageReader {
	replicaConfig := opts.ReplicaConfig
	storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, opts.UpstreamURI.String())
	if err != nil {
		log.Panic("Storage client initialization failed", zap.Error(err))
	}
	reader := &storageReader{
		storage:        storage,
		dateSeparator:  util.GetOrZero(replicaConfig.Sink.DateSeparator),
		fileExtension:  helper.GetFileExtension(opts.CodecConfig.Protocol),
		fileIndexWidth: util.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		scheme:         opts.UpstreamURI.Scheme,
		schemas:        make(map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile),
		fileIndices:    make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		ddlWatermarks:  make(map[string]uint64),
		tableIDs:       make(map[storageTableKey]int64),
	}
	switch reader.scheme {
	case "file", "local":
		reader.localPath = opts.UpstreamURI.Path
	case "s3", "ks3", "oss":
		reader.bucket = opts.UpstreamURI.Host
		reader.bucketPath = opts.UpstreamURI.Path
		reader.s3Client = newS3Client(ctx, opts.UpstreamURI)
	default:
		log.Panic("Storage consumer does not support the storage schema", zap.String("scheme", reader.scheme))
	}
	return reader
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
	if err := c.discoverDir(ctx, "", 0, files); err != nil {
		return nil, err
	}
	return files, nil
}

// discoverDir finds data directories and schema files below dir. Only
// directories are listed; the data files themselves are read from the index.
func (c *storageReader) discoverDir(
	ctx context.Context, dir string, depth int,
	files map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange,
) error {
	dirs, _, err := c.listEntries(ctx, dir)
	if err != nil {
		return err
	}
	meta := path.Join(dir, "meta")
	if slices.Contains(dirs, "meta") {
		_, names, err := c.listEntries(ctx, meta)
		if err != nil {
			return err
		}
		indices := make([]string, 0, len(names))
		for _, name := range names {
			switch {
			case strings.HasSuffix(name, ".index"):
				indices = append(indices, name)
			case cloudstorage.IsSchemaFile(name):
				key, added, err := c.readSchema(ctx, path.Join(meta, name))
				if err != nil {
					return err
				}
				if added {
					files[cloudstorage.NewSchemaFileDMLPathKey(key)] = nil
				}
			}
		}
		if len(indices) > 0 {
			return c.readIndices(ctx, dir, indices, files)
		}
	}
	if depth == storageDiscoveryDepth {
		return nil
	}
	for _, child := range dirs {
		if child == "meta" {
			continue
		}
		if err := c.discoverDir(ctx, path.Join(dir, child), depth+1, files); err != nil {
			return err
		}
	}
	return nil
}

func (c *storageReader) readIndices(
	ctx context.Context, dir string, names []string,
	files map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange,
) error {
	for _, name := range names {
		if err := c.addIndex(ctx, path.Join(dir, "meta", name), files); err != nil {
			return err
		}
	}
	return nil
}

func (c *storageReader) addIndex(
	ctx context.Context, indexPath string,
	files map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange,
) error {
	var key cloudstorage.DMLPathKey
	if err := key.ParseIndexFilePath(c.dateSeparator, indexPath); err != nil {
		return errors.WrapError(errors.ErrStorageSinkInvalidFileName, err, "parse Storage index path")
	}
	if (c.sourceCheckpoint > 0 && key.TableVersion > c.sourceCheckpoint) || key.TableVersion < c.ddlWatermark(key.GetKey()) {
		return nil
	}
	index, err := c.readIndex(ctx, indexPath)
	if err != nil {
		return err
	}
	indices := c.fileIndices[key]
	if indices == nil {
		indices = make(map[cloudstorage.FileIndexKey]uint64)
		c.fileIndices[key] = indices
	}
	previous := indices[index.FileIndexKey]
	indices[index.FileIndexKey] = max(previous, index.Idx)
	if index.Idx > previous {
		if files[key] == nil {
			files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
		}
		files[key][index.FileIndexKey] = storageIndexRange{
			start: previous + 1,
			end:   index.Idx,
		}
	}
	return nil
}

// listEntries returns the immediate child directories and files of dir.
func (c *storageReader) listEntries(ctx context.Context, dir string) ([]string, []string, error) {
	if c.localPath != "" {
		entries, err := os.ReadDir(filepath.Join(c.localPath, filepath.FromSlash(dir)))
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		if err != nil {
			return nil, nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "list Storage directory")
		}
		dirs := make([]string, 0, len(entries))
		files := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				dirs = append(dirs, entry.Name())
			} else {
				files = append(files, entry.Name())
			}
		}
		slices.Sort(dirs)
		slices.Sort(files)
		return dirs, files, nil
	}
	prefix := c.s3Prefix(dir)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	var dirs, files []string
	for {
		output, err := c.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "list Storage directory")
		}
		for _, commonPrefix := range output.CommonPrefixes {
			name := strings.Trim(strings.TrimPrefix(aws.ToString(commonPrefix.Prefix), prefix), "/")
			if name != "" {
				dirs = append(dirs, name)
			}
		}
		for _, object := range output.Contents {
			name := strings.TrimPrefix(aws.ToString(object.Key), prefix)
			if name != "" && !strings.Contains(name, "/") {
				files = append(files, name)
			}
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		input.ContinuationToken = output.NextContinuationToken
	}
	slices.Sort(dirs)
	slices.Sort(files)
	return dirs, files, nil
}

func (c *storageReader) s3Prefix(dir string) string {
	prefix := path.Join(strings.Trim(c.bucketPath, "/"), strings.Trim(dir, "/"))
	if prefix != "" {
		prefix += "/"
	}
	return prefix
}

func (c *storageReader) readIndex(ctx context.Context, path string) (cloudstorage.FileIndex, error) {
	data, err := c.storage.ReadFile(ctx, path)
	if err != nil {
		return cloudstorage.FileIndex{}, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage index")
	}
	return cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.sourceCheckpoint > 0 && key.TableVersion > c.sourceCheckpoint) || c.schemas[key] != nil || key.TableVersion < c.ddlWatermark(key.GetKey()) {
		return key, false, nil
	}
	data, err := c.storage.ReadFile(ctx, path)
	if err != nil {
		return key, false, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage schema")
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

func (c *storageReader) Read(ctx context.Context) ([]*Payload, error) {
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
			return []*Payload{{Watermark: c.sourceCheckpoint}}, nil
		}
		if err := c.scanInputs(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	input := c.inputs[0]
	key := input.key
	ddlWatermark := c.ddlWatermark(key.GetKey())
	switch {
	case input.index.Idx < input.end && key.TableVersion >= ddlWatermark:
		c.inputs[0].index.Idx++
	default:
		c.inputs[0] = storageInput{}
		c.inputs = c.inputs[1:]
	}
	schema := c.schemas[key.SchemaPathKey]
	if schema == nil {
		log.Panic("Storage DML file has no matching schema", zap.String("schema", key.Schema), zap.String("table", key.Table), zap.Uint64("tableVersion", key.TableVersion))
	}
	if key.IsSchemaFileDMLPathKey() && (schema.Query == "" || key.TableVersion <= ddlWatermark) {
		return nil, nil
	}
	if key.TableVersion < ddlWatermark {
		c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
		return nil, nil
	}
	if key.IsSchemaFileDMLPathKey() {
		pendingSchema = make(chan struct{})
		c.mu.Lock()
		c.pendingSchema = pendingSchema
		c.mu.Unlock()
		return []*Payload{{File: &File{Schema: schema}}}, nil
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
	return []*Payload{data}, nil
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
		data, err := c.storage.ReadFile(ctx, "metadata")
		if err != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage metadata")
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

func (c *storageReader) readDMLFile(ctx context.Context, input storageInput, schema *cloudstorage.SchemaFile, tableID int64) (*Payload, error) {
	path := input.key.GenerateDMLFilePath(&input.index, c.fileExtension, c.fileIndexWidth)
	data, err := c.storage.ReadFile(ctx, path)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage DML file")
	}
	c.fileIndices[input.key][input.index.FileIndexKey] = input.index.Idx
	return &Payload{
		Value: data,
		Bytes: int64(len(data)),
		File:  &File{Schema: schema, TableID: tableID},
	}, nil
}

// storageReader scans files in order, so its inputs carry no partition.
func (*storageReader) Partitions() int32 {
	return 0
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

func newS3Client(ctx context.Context, uri *url.URL) *s3.Client {
	values := uri.Query()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(s3Region(values)),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			values.Get("access-key"),
			values.Get("secret-access-key"),
			values.Get("session-token"),
		)),
	)
	if err != nil {
		log.Panic("Storage client initialization failed", zap.Error(err))
	}
	options := s3ClientOptions(values)
	client := s3.NewFromConfig(cfg, options...)
	if configuredS3Region(values) != "" || !shouldDetectS3BucketRegion(values) {
		return client
	}

	region, err := manager.GetBucketRegion(ctx, client, uri.Host, func(o *s3.Options) {
		if client.Options().Credentials != nil {
			o.Credentials = client.Options().Credentials
		}
		if s3Endpoint(values) != "" {
			o.UsePathStyle = client.Options().UsePathStyle
		}
	})
	if err != nil {
		log.Panic("detect Storage bucket region failed", zap.String("bucket", uri.Host), zap.Error(err))
	}
	if region == "" {
		region = defaultS3Region
	}
	if region != cfg.Region {
		cfg.Region = region
		client = s3.NewFromConfig(cfg, options...)
	}
	return client
}

func s3ClientOptions(values url.Values) []func(*s3.Options) {
	var options []func(*s3.Options)
	if endpoint := s3Endpoint(values); endpoint != "" {
		options = append(options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}
	return options
}

func configuredS3Region(values url.Values) string {
	if region := values.Get("s3.region"); region != "" {
		return region
	}
	return values.Get("region")
}

func s3Region(values url.Values) string {
	if region := configuredS3Region(values); region != "" {
		return region
	}
	return defaultS3Region
}

func shouldDetectS3BucketRegion(values url.Values) bool {
	provider := values.Get("s3.provider")
	if provider == "" {
		provider = values.Get("provider")
	}
	if provider != "" && provider != "aws" {
		return false
	}
	return s3Endpoint(values) == "" || provider == "aws"
}

func s3Endpoint(values url.Values) string {
	if endpoint := values.Get("s3.endpoint"); endpoint != "" {
		return endpoint
	}
	return values.Get("endpoint")
}
