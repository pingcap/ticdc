// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultDirectoryName = "cloudstorage-sink-spool"
	segmentFilePrefix    = "segment-"
	segmentFileExt       = ".log"

	// Use the same order of magnitude as the storage sink's default file size.
	// One segment is large enough for sequential local writes, but still small
	// enough to roll over and reclaim files without waiting too long.
	defaultSegmentCapacity = int64(64 * 1024 * 1024)

	// Keep enough local disk space for temporary downstream slowdowns by
	// default, while still requiring operators to size the quota explicitly
	// for larger workloads.
	defaultDiskQuotaBytes = int64(10 * 1024 * 1024 * 1024)

	// Keep only a small hot working set in memory. Most queued data can move to
	// local files so the writer is less likely to keep growing memory usage.
	defaultMemoryRatio = 0.2

	// Pause PostEnqueue callbacks only after local usage is already fairly high,
	// so the upstream side is not slowed down too early.
	defaultHighWatermarkRatio = 0.8

	// Resume pending PostEnqueue callbacks only after usage has dropped enough
	// to avoid bouncing immediately back into the paused state.
	defaultLowWatermarkRatio = 0.6
)

type options struct {
	// rootDir is the base directory used to build one changefeed's spool
	// directory. If empty, use TiCDC's data dir as the base directory.
	rootDir string

	// diskQuotaBytes is the disk budget for local spool files.
	// spool still derives in-memory and watermark thresholds from it, but the
	// runtime contract exposed by spool-disk-quota only constrains spilled bytes.
	diskQuotaBytes int64

	// segmentCapacity is the largest size of one segment file before spool rolls to the next file.
	segmentCapacity int64

	// memoryRatio is the fraction of diskQuotaBytes kept in memory before spilling to disk.
	memoryRatio float64
	// highWatermarkRatio is the ratio that starts pausing PostEnqueue callbacks.
	highWatermarkRatio float64
	// lowWatermarkRatio is the ratio that resumes pending PostEnqueue callbacks.
	lowWatermarkRatio float64
}

type option func(*options)

func WithRootDir(rootDir string) option {
	return func(options *options) {
		options.rootDir = rootDir
	}
}

func WithDiskQuotaBytes(quotaBytes int64) option {
	return func(options *options) {
		if quotaBytes == 0 {
			return
		}
		if quotaBytes < 0 {
			log.Warn(
				"spool option is invalid, use default",
				zap.String("field", "quota-bytes"),
				zap.Int64("original", quotaBytes),
				zap.Int64("default", defaultDiskQuotaBytes),
			)
			return
		}
		options.diskQuotaBytes = quotaBytes
	}
}

func WithSegmentBytes(segmentBytes int64) option {
	return func(options *options) {
		if segmentBytes == 0 {
			return
		}
		if segmentBytes < 0 {
			log.Warn(
				"spool option is invalid, use default",
				zap.String("field", "segment-bytes"),
				zap.Int64("original", segmentBytes),
				zap.Int64("default", defaultSegmentCapacity),
			)
			return
		}
		options.segmentCapacity = segmentBytes
	}
}

func WithMemoryRatio(memoryRatio float64) option {
	return func(options *options) {
		if memoryRatio == 0 {
			return
		}
		if memoryRatio < 0 || memoryRatio >= 1 {
			log.Warn(
				"spool option is invalid, use default",
				zap.String("field", "memory-ratio"),
				zap.Float64("original", memoryRatio),
				zap.Float64("default", defaultMemoryRatio),
			)
			return
		}
		options.memoryRatio = memoryRatio
	}
}

func WithHighWatermarkRatio(highWatermarkRatio float64) option {
	return func(options *options) {
		if highWatermarkRatio == 0 {
			return
		}
		if highWatermarkRatio < 0 || highWatermarkRatio >= 1 {
			log.Warn(
				"spool option is invalid, use default",
				zap.String("field", "high-watermark-ratio"),
				zap.Float64("original", highWatermarkRatio),
				zap.Float64("default", defaultHighWatermarkRatio),
			)
			return
		}
		options.highWatermarkRatio = highWatermarkRatio
	}
}

func WithLowWatermarkRatio(lowWatermarkRatio float64) option {
	return func(options *options) {
		if lowWatermarkRatio == 0 {
			return
		}
		if lowWatermarkRatio < 0 || lowWatermarkRatio >= 1 {
			log.Warn(
				"spool option is invalid, use default",
				zap.String("field", "low-watermark-ratio"),
				zap.Float64("original", lowWatermarkRatio),
				zap.Float64("default", defaultLowWatermarkRatio),
			)
			return
		}
		options.lowWatermarkRatio = lowWatermarkRatio
	}
}

type segmentID uint64

// Spool keeps encoded DML messages after a writer shard has accepted them and
// before that writer shard has flushed them to external storage.
//
// The producer is the cloud storage writer path: after encoderGroup has
// produced encoded messages for a task, writer.Enqueue calls Spool.Enqueue to
// hand those messages to local spool storage.
//
// The consumer is also the cloud storage writer path: when the writer flushes a
// batch, it calls Spool.Load to read the queued messages back, then calls
// Spool.Release after a successful flush or Spool.Discard when the batch is
// ignored.
type Spool struct {
	keyspace   string
	changefeed string

	// workDir is where this spool instance keeps its temporary segment files.
	workDir string

	// mu protects the mutable runtime state below:
	// quota state transitions, nextSegmentID, activeSegment, segments, and the
	// per-segment metadata updated during append, load, release, and rotate.
	mu sync.Mutex

	// closed makes Close idempotent and blocks new writes after shutdown starts.
	closed atomic.Bool

	// quota tracks bytes through budget and applies spool-specific wake and metrics policy.
	quota *quotaController
	// segmentCapacity is the size limit for a single segment file.
	segmentCapacity int64
	// metricLoadedBytes records the byte distribution of loads from spilled files.
	metricLoadedBytes prometheus.Observer
	// metricRotatedCount records how many times spool rolls to a new segment.
	metricRotatedCount prometheus.Counter
	// metricSegmentCount records how many live segment files this spool currently owns.
	metricSegmentCount prometheus.Gauge

	// nextSegmentID is the next local file sequence number to allocate.
	nextSegmentID segmentID
	// activeSegment is the current append target for spilled blobs.
	activeSegment *segment
	// segments keeps every live segment so Load/Release can find it by ID.
	segments map[segmentID]*segment
}

// segment is one append-only local file that stores spilled message batches.
type segment struct {
	// id becomes part of the segment file name and lookup key.
	id segmentID
	// path is the on-disk location of this segment file.
	path string
	// file is the opened segment file handle.
	file *os.File
	// size is the current file size in bytes.
	size int64
	// refCnt counts how many entries still point to this segment.
	refCnt int64
}

// segmentLocation tells Load and Release where one spilled batch lives inside a segment file.
type segmentLocation struct {
	// id tells which segment file stores this blob.
	id segmentID
	// offset is the starting byte offset inside the segment file.
	offset int64
	// length is the blob length in bytes.
	length int64
}

// Entry is the handle the writer keeps after putting one encoded batch into spool.
type Entry struct {
	// memoryMsgs holds encoded messages directly when the entry stays in memory.
	memoryMsgs []*common.Message
	// location points to the on-disk blob when the entry has been spilled.
	location *segmentLocation

	// postFlushCallbacks are detached from encoded messages and kept until the
	// entry is either acknowledged by a successful flush or discarded.
	postFlushCallbacks []func()

	// accountingBytes is the size charged against the spool quota.
	accountingBytes int64
	// fileBytes is the payload size that will later be written to the final data file.
	fileBytes uint64
}

type entrySize struct {
	accountingBytes int64
	fileBytes       uint64
}

// EnqueueAction tells the caller how spool wants the next encoded batch to proceed.
type EnqueueAction int

const (
	// EnqueueActionAccepted means spool has accepted the batch and returned an entry.
	EnqueueActionAccepted EnqueueAction = iota
	// EnqueueActionAcceptedOversized means spool accepted the batch in memory
	// because the batch itself is larger than the configured disk quota and
	// should be flushed immediately.
	EnqueueActionAcceptedOversized
	// EnqueueActionWaitDiskQuota means the caller should flush what it already has,
	// wait for disk quota to be released, and then retry the enqueue.
	EnqueueActionWaitDiskQuota
)

// FileBytes returns the payload bytes counted towards writer task sizing.
func (e *Entry) FileBytes() uint64 {
	if e == nil {
		return 0
	}
	return e.fileBytes
}

// IsSpilled returns whether this entry is persisted in local segment files.
func (e *Entry) IsSpilled() bool {
	return e != nil && e.location != nil
}

// InMemory returns whether this entry is still held in memory.
func (e *Entry) InMemory() bool {
	return e != nil && e.memoryMsgs != nil
}

// New return a spool that manages unflushed data.
func New(
	changefeedID commonType.ChangeFeedID,
	opts ...option,
) (*Spool, error) {
	cfg := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	normalizeOptions(cfg)
	workDir := resolveWorkDir(changefeedID, cfg.rootDir)
	if err := prepareWorkDir(workDir); err != nil {
		return nil, err
	}

	var (
		keyspace   = changefeedID.Keyspace()
		changefeed = changefeedID.Name()
	)
	spool := &Spool{
		keyspace:           keyspace,
		changefeed:         changefeed,
		workDir:            workDir,
		quota:              newQuotaController(changefeedID, cfg),
		segmentCapacity:    cfg.segmentCapacity,
		metricLoadedBytes:  metrics.CloudStorageLoadBytesHistogram.WithLabelValues(keyspace, changefeed),
		metricRotatedCount: metrics.CloudStorageRotateCountCounter.WithLabelValues(keyspace, changefeed),
		metricSegmentCount: metrics.CloudStorageSpoolSegmentCountGauge.WithLabelValues(keyspace, changefeed),
		segments:           make(map[segmentID]*segment),
	}
	return spool, nil
}

func defaultOptions() *options {
	return &options{
		diskQuotaBytes:     defaultDiskQuotaBytes,
		segmentCapacity:    defaultSegmentCapacity,
		memoryRatio:        defaultMemoryRatio,
		highWatermarkRatio: defaultHighWatermarkRatio,
		lowWatermarkRatio:  defaultLowWatermarkRatio,
	}
}

func normalizeOptions(cfg *options) {
	if cfg.lowWatermarkRatio < cfg.highWatermarkRatio {
		return
	}
	log.Warn(
		"spool watermark ratio is invalid, use default",
		zap.Float64("low", cfg.lowWatermarkRatio),
		zap.Float64("high", cfg.highWatermarkRatio),
		zap.Float64("defaultLow", defaultLowWatermarkRatio),
		zap.Float64("defaultHigh", defaultHighWatermarkRatio),
	)
	cfg.lowWatermarkRatio = defaultLowWatermarkRatio
	cfg.highWatermarkRatio = defaultHighWatermarkRatio
}

func resolveWorkDir(changefeedID commonType.ChangeFeedID, rootDir string) string {
	baseDir := rootDir
	if baseDir == "" {
		baseDir = config.GetGlobalServerConfig().DataDir
		if baseDir == "" {
			baseDir = os.TempDir()
		}
		baseDir = filepath.Join(baseDir, defaultDirectoryName)
	}

	return filepath.Join(
		baseDir,
		changefeedID.Keyspace(),
		changefeedID.Name(),
	)
}

// prepareWorkDir recreates the changefeed-specific spool directory from
// scratch. The final workDir is fully owned by one spool instance, so startup
// can clear the whole directory before creating fresh segment files.
func prepareWorkDir(workDir string) error {
	if err := os.RemoveAll(workDir); err != nil {
		return errors.WrapError(errors.ErrCheckDirValid, err)
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return errors.WrapError(errors.ErrCheckDirValid, err)
	}
	return nil
}

// TryEnqueue decides how spool should handle the next batch under one lock.
// It either accepts the batch normally, accepts it as an oversized in-memory
// batch that should be flushed immediately, or asks the caller to wait for disk
// quota and retry.
func (s *Spool) TryEnqueue(
	msgs []*common.Message,
	postEnqueue func(),
) (EnqueueAction, *Entry, error) {
	size := calculateEntrySize(msgs)
	if size.accountingBytes == 0 {
		return EnqueueActionAccepted, &Entry{}, nil
	}

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return EnqueueActionAccepted, nil, errors.ErrInternalCheckFailed.GenWithStackByArgs("spool is closed")
	}
	shouldSpill := s.quota.shouldSpill(size.accountingBytes)
	if shouldSpill && s.quota.entryExceedsDiskQuota(size.accountingBytes) {
		entry, postEnqueueToRun, err := s.acceptEntryLocked(msgs, postEnqueue, size, true)
		s.mu.Unlock()
		if postEnqueueToRun != nil {
			postEnqueueToRun()
		}
		return EnqueueActionAcceptedOversized, entry, err
	}
	if shouldSpill && s.quota.spillWouldExceedDiskQuota(size.accountingBytes) {
		s.mu.Unlock()
		return EnqueueActionWaitDiskQuota, nil, nil
	}

	entry, postEnqueueToRun, err := s.acceptEntryLocked(msgs, postEnqueue, size, false)
	s.mu.Unlock()
	if postEnqueueToRun != nil {
		postEnqueueToRun()
	}
	return EnqueueActionAccepted, entry, err
}

// WaitForDiskQuota waits until the next spilled entry of the same size would
// fit into the configured local spool disk budget.
func (s *Spool) WaitForDiskQuota(ctx context.Context, msgs []*common.Message) error {
	size := calculateEntrySize(msgs)
	if size.accountingBytes == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		s.quota.metricDiskQuotaWait.Observe(time.Since(start).Seconds())
	}()
	for {
		s.mu.Lock()
		if s.closed.Load() {
			s.mu.Unlock()
			return errors.ErrInternalCheckFailed.GenWithStackByArgs("spool is closed")
		}
		if !s.quota.spillWouldExceedDiskQuota(size.accountingBytes) {
			s.mu.Unlock()
			return nil
		}
		waiterID, waitCh := s.quota.addDiskQuotaWaiter()
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.quota.removeDiskQuotaWaiter(waiterID)
			return errors.Trace(context.Cause(ctx))
		case <-waitCh:
		}
	}
}

// Enqueue appends encoded messages to spool.
func (s *Spool) Enqueue(
	msgs []*common.Message,
	postEnqueue func(),
) (*Entry, error) {
	size := calculateEntrySize(msgs)
	if size.accountingBytes == 0 {
		return &Entry{}, nil
	}

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return nil, errors.ErrInternalCheckFailed.GenWithStackByArgs("spool is closed")
	}
	entry, postEnqueueToRun, err := s.acceptEntryLocked(msgs, postEnqueue, size, false)
	s.mu.Unlock()
	if postEnqueueToRun != nil {
		postEnqueueToRun()
	}
	return entry, err
}

func (s *Spool) acceptEntryLocked(
	msgs []*common.Message,
	postEnqueue func(),
	size entrySize,
	forceInMemory bool,
) (*Entry, func(), error) {
	entry := &Entry{
		accountingBytes: size.accountingBytes,
		fileBytes:       size.fileBytes,
	}
	shouldSpill := !forceInMemory && s.quota.shouldSpill(entry.accountingBytes)
	if shouldSpill {
		blob := serializeMessages(msgs)
		location, err := s.appendBlobLocked(blob)
		if err != nil {
			return nil, nil, err
		}
		entry.location = location
	}
	if !shouldSpill {
		entry.memoryMsgs = msgs
	}
	entry.postFlushCallbacks = detachPostFlushCallbacks(msgs)
	postEnqueueToRun := s.quota.acquire(entry.accountingBytes, shouldSpill, postEnqueue)
	return entry, postEnqueueToRun, nil
}

func detachPostFlushCallbacks(msgs []*common.Message) []func() {
	var postFlushCallbacks []func()
	for _, msg := range msgs {
		if msg.Callback != nil {
			postFlushCallbacks = append(postFlushCallbacks, msg.Callback)
			msg.Callback = nil
		}
	}
	return postFlushCallbacks
}

type MessageReader struct {
	memoryMsgs         []*common.Message
	memoryIndex        int
	serializedReader   *serializedMessageReader
	postFlushCallbacks []func()
}

func (r *MessageReader) Next() (key, value []byte, rowCount int, ok bool, err error) {
	if r == nil {
		return nil, nil, 0, false, nil
	}
	if r.memoryMsgs != nil {
		if r.memoryIndex >= len(r.memoryMsgs) {
			return nil, nil, 0, false, nil
		}
		msg := r.memoryMsgs[r.memoryIndex]
		r.memoryIndex++
		return msg.Key, msg.Value, msg.GetRowsCount(), true, nil
	}
	if r.serializedReader == nil {
		return nil, nil, 0, false, nil
	}
	return r.serializedReader.next()
}

func (r *MessageReader) PostFlushCallbacks() []func() {
	if r == nil {
		return nil
	}
	return r.postFlushCallbacks
}

func (s *Spool) NewMessageReader(entry *Entry) (*MessageReader, error) {
	if entry == nil {
		return &MessageReader{}, nil
	}
	if entry.memoryMsgs != nil {
		return &MessageReader{
			memoryMsgs:         entry.memoryMsgs,
			postFlushCallbacks: entry.postFlushCallbacks,
		}, nil
	}
	if entry.location == nil {
		return &MessageReader{
			postFlushCallbacks: entry.postFlushCallbacks,
		}, nil
	}

	s.mu.Lock()
	spoolSegment := s.segments[entry.location.id]
	if spoolSegment == nil {
		s.mu.Unlock()
		return nil, errors.ErrInternalCheckFailed.GenWithStack(
			"spool segment %d not found",
			entry.location.id,
		)
	}
	file := spoolSegment.file
	offset := entry.location.offset
	length := entry.location.length
	s.mu.Unlock()

	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		return nil, errors.WrapError(errors.ErrUnexpected, err, "read spool segment file")
	}
	reader, err := newSerializedMessageReader(buf)
	if err != nil {
		return nil, err
	}
	s.metricLoadedBytes.Observe(float64(length))
	return &MessageReader{
		serializedReader:   reader,
		postFlushCallbacks: entry.postFlushCallbacks,
	}, nil
}

// Release releases memory or spilled bytes held by an entry.
func (s *Spool) Release(entry *Entry) {
	if entry == nil || entryConsumed(entry) {
		return
	}
	location, accountingBytes, spilled := consumeEntry(entry)

	s.mu.Lock()
	if s.closed.Load() {
		s.mu.Unlock()
		return
	}

	if spilled {
		seg := s.segments[location.id]
		if seg != nil {
			seg.refCnt--
			if seg.refCnt == 0 && s.activeSegment != seg {
				if err := seg.file.Close(); err != nil {
					log.Warn("close spool segment file failed",
						zap.String("keyspace", s.keyspace), zap.String("changefeed", s.changefeed),
						zap.Error(err))
				}
				if err := os.Remove(seg.path); err != nil {
					log.Warn(
						"remove spool segment file failed",
						zap.String("keyspace", s.keyspace), zap.String("changefeed", s.changefeed),
						zap.String("path", seg.path), zap.Error(err))
				}
				delete(s.segments, seg.id)
				s.metricSegmentCount.Set(float64(len(s.segments)))
			}
		}
	}
	postEnqueueCallbacks := s.quota.release(accountingBytes, spilled)
	s.mu.Unlock()

	for _, postEnqueueCallback := range postEnqueueCallbacks {
		if postEnqueueCallback != nil {
			postEnqueueCallback()
		}
	}
}

// Discard runs the entry postFlush callbacks and then releases its local spool
// resources. It's called when the corresponding flushed data should be ignored.
func (s *Spool) Discard(entry *Entry) {
	if entry == nil {
		return
	}

	for _, postFlushCallback := range takePostFlushCallbacks(entry) {
		if postFlushCallback != nil {
			postFlushCallback()
		}
	}
	s.Release(entry)
}

// Close closes spool and removes the segment files created by this spool instance.
func (s *Spool) Close() {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}

	s.mu.Lock()
	for _, seg := range s.segments {
		if seg.file != nil {
			if err := seg.file.Close(); err != nil {
				log.Warn("close spool segment failed",
					zap.String("keyspace", s.keyspace), zap.String("changefeed", s.changefeed),
					zap.String("path", seg.path), zap.Error(err))
			}
		}
	}
	s.mu.Unlock()

	if err := os.RemoveAll(s.workDir); err != nil {
		log.Warn("remove spool files failed",
			zap.String("keyspace", s.keyspace), zap.String("changefeed", s.changefeed),
			zap.String("path", s.workDir), zap.Error(err))
	}
	metrics.CloudStorageLoadBytesHistogram.DeleteLabelValues(s.keyspace, s.changefeed)
	metrics.CloudStorageRotateCountCounter.DeleteLabelValues(s.keyspace, s.changefeed)
	metrics.CloudStorageSpoolSegmentCountGauge.DeleteLabelValues(s.keyspace, s.changefeed)
	s.quota.deleteMetrics()
}

func (s *Spool) appendBlobLocked(blob []byte) (*segmentLocation, error) {
	seg, err := s.getWritableSegmentLocked(int64(len(blob)))
	if err != nil {
		return nil, err
	}
	offset := seg.size
	n, err := seg.file.Write(blob)
	if err != nil {
		return nil, errors.WrapError(errors.ErrUnexpected, err, "write spool segment file")
	}
	if n != len(blob) {
		return nil, errors.ErrUnexpected.GenWithStack(
			"short write to spool segment, expected %d got %d",
			len(blob),
			n,
		)
	}
	seg.size += int64(n)
	seg.refCnt++

	return &segmentLocation{
		id:     seg.id,
		offset: offset,
		length: int64(n),
	}, nil
}

func (s *Spool) getWritableSegmentLocked(needBytes int64) (*segment, error) {
	if s.activeSegment != nil && s.activeSegment.size+needBytes <= s.segmentCapacity {
		return s.activeSegment, nil
	}

	if err := s.rotateLocked(); err != nil {
		return nil, err
	}
	return s.activeSegment, nil
}

func (s *Spool) rotateLocked() error {
	s.nextSegmentID++
	segmentID := s.nextSegmentID
	path := filepath.Join(
		s.workDir,
		fmt.Sprintf("%s%06d%s", segmentFilePrefix, segmentID, segmentFileExt),
	)
	// External damage to the spool directory becomes fatal here. Once spool
	// needs a new segment and cannot create it, it can no longer promise that
	// new pending data will stay locally readable until flush.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return errors.WrapError(errors.ErrUnexpected, err, "open spool segment file")
	}
	seg := &segment{
		id:   segmentID,
		path: path,
		file: file,
	}
	s.segments[segmentID] = seg
	s.activeSegment = seg
	s.metricRotatedCount.Inc()
	s.metricSegmentCount.Set(float64(len(s.segments)))
	return nil
}

func takePostFlushCallbacks(entry *Entry) []func() {
	postFlushCallbacks := entry.postFlushCallbacks
	entry.postFlushCallbacks = nil
	return postFlushCallbacks
}

func entryConsumed(entry *Entry) bool {
	return entry.location == nil &&
		entry.memoryMsgs == nil &&
		entry.accountingBytes == 0 &&
		entry.fileBytes == 0 &&
		entry.postFlushCallbacks == nil
}

func consumeEntry(entry *Entry) (*segmentLocation, int64, bool) {
	location := entry.location
	accountingBytes := entry.accountingBytes
	spilled := location != nil

	entry.memoryMsgs = nil
	entry.location = nil
	entry.postFlushCallbacks = nil
	entry.accountingBytes = 0
	entry.fileBytes = 0
	return location, accountingBytes, spilled
}

func calculateEntrySize(msgs []*common.Message) entrySize {
	size := entrySize{}
	if len(msgs) == 0 {
		return size
	}

	size.accountingBytes = serializedMessageCountBytes
	for _, msg := range msgs {
		size.accountingBytes += int64(serializedMessageHeaderBytes + len(msg.Key) + len(msg.Value))
		size.fileBytes += uint64(len(msg.Value))
	}
	return size
}
