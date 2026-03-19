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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const (
	defaultDirectoryName = "cloudstorage-sink-spool"
	segmentFilePrefix    = "segment-"
	segmentFileExt       = ".log"

	defaultSegmentBytes = int64(64 * 1024 * 1024)

	defaultMemoryRatio        = 0.2
	defaultHighWatermarkRatio = 0.8
	defaultLowWatermarkRatio  = 0.6
)

// Options tells spool where to keep temporary files and how much local space it may use.
type Options struct {
	// RootDir is the local directory used by spool. If empty, use a
	// changefeed-specific directory under TiCDC's data dir.
	RootDir string

	// QuotaBytes is the total local budget shared by in-memory data and spilled files.
	QuotaBytes int64

	// SegmentBytes is the largest size of one segment file before spool rolls to the next file.
	SegmentBytes int64

	// MemoryRatio is the fraction of QuotaBytes kept in memory before spilling to disk.
	MemoryRatio float64
	// HighWatermarkRatio is the ratio that starts suppressing wake callbacks.
	HighWatermarkRatio float64
	// LowWatermarkRatio is the ratio that resumes suppressed wake callbacks.
	LowWatermarkRatio float64
}

// Manager keeps encoded messages in local spool storage until the writer flushes them to object storage.
type Manager struct {
	// rootDir is where this spool instance keeps its temporary segment files.
	rootDir string

	// mu protects the mutable runtime state below.
	mu sync.Mutex

	// closed makes Close idempotent and blocks new writes after shutdown starts.
	closed bool

	// quota tracks bytes through budgetCore and applies spool-specific wake and metrics policy.
	quota *quotaAdapter
	// segmentBytes is the size limit for a single segment file.
	segmentBytes int64

	// nextSegmentID is the next local file sequence number to allocate.
	nextSegmentID uint64
	// activeSegment is the current append target for spilled blobs.
	activeSegment *segment
	// segments keeps every live segment so Load/Release can find it by ID.
	segments map[uint64]*segment
}

// segment is one append-only local file that stores spilled message batches.
type segment struct {
	// id becomes part of the segment file name and lookup key.
	id uint64
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
	// segmentID tells which segment file stores this blob.
	segmentID uint64
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

	// callbacks are deferred until the entry is either acknowledged or discarded.
	callbacks []func()

	// accountingBytes is the size charged against the spool quota.
	accountingBytes int64
	// fileBytes is the payload size that will later be written to the final data file.
	fileBytes uint64
}

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

// New creates a per-changefeed spool manager.
func New(
	changefeedID commonType.ChangeFeedID,
	opts *Options,
) (*Manager, error) {
	options := withDefaultOptions(opts)
	if err := validateOptions(options); err != nil {
		return nil, err
	}

	rootDir := resolveRootDir(changefeedID, options.RootDir)
	if err := prepareRootDir(rootDir); err != nil {
		return nil, err
	}

	manager := &Manager{
		rootDir:      rootDir,
		quota:        newQuotaAdapter(changefeedID, options),
		segmentBytes: options.SegmentBytes,
		segments:     make(map[uint64]*segment),
	}
	return manager, nil
}

func validateOptions(options *Options) error {
	if options.SegmentBytes <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"spool segment size must be greater than 0, but got %d",
			options.SegmentBytes,
		)
	}
	return validateBudgetOptions(options)
}

func withDefaultOptions(opts *Options) *Options {
	result := &Options{
		SegmentBytes:       defaultSegmentBytes,
		MemoryRatio:        defaultMemoryRatio,
		HighWatermarkRatio: defaultHighWatermarkRatio,
		LowWatermarkRatio:  defaultLowWatermarkRatio,
	}
	if opts == nil {
		return result
	}
	if opts.RootDir != "" {
		result.RootDir = opts.RootDir
	}
	result.QuotaBytes = opts.QuotaBytes
	if opts.SegmentBytes != 0 {
		result.SegmentBytes = opts.SegmentBytes
	}
	if opts.MemoryRatio != 0 {
		result.MemoryRatio = opts.MemoryRatio
	}
	if opts.HighWatermarkRatio != 0 {
		result.HighWatermarkRatio = opts.HighWatermarkRatio
	}
	if opts.LowWatermarkRatio != 0 {
		result.LowWatermarkRatio = opts.LowWatermarkRatio
	}
	return result
}

func resolveRootDir(changefeedID commonType.ChangeFeedID, rootDir string) string {
	if rootDir != "" {
		return rootDir
	}

	dataDir := config.GetGlobalServerConfig().DataDir
	if dataDir == "" {
		dataDir = os.TempDir()
	}
	return filepath.Join(
		dataDir,
		defaultDirectoryName,
		changefeedID.Keyspace(),
		fmt.Sprintf("%s-%s", changefeedID.Name(), changefeedID.ID().String()),
	)
}

// prepareRootDir starts each spool instance from a clean set of spool-owned
// segment files. The intent is to avoid appending to stale segment logs from a
// previous run, while leaving any unrelated files under Options.RootDir intact.
func prepareRootDir(rootDir string) error {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return errors.WrapError(errors.ErrCheckDirValid, err)
	}

	return removeSpoolFiles(rootDir)
}

// removeSpoolFiles only removes files created by spool itself. We use
// RemoveAll on each matched path so cleanup still works if the file was
// replaced with a symlink or some other filesystem entry, but we never delete
// the whole root directory because callers may keep unrelated files there.
func removeSpoolFiles(rootDir string) error {
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.WrapError(errors.ErrCheckDirValid, err)
	}
	for _, entry := range entries {
		if !isSegmentFile(entry) {
			continue
		}

		path := filepath.Join(rootDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return errors.WrapError(errors.ErrCheckDirValid, err)
		}
	}
	return nil
}

func isSegmentFile(entry os.DirEntry) bool {
	if entry.IsDir() {
		return false
	}
	name := entry.Name()
	if !strings.HasPrefix(name, segmentFilePrefix) {
		return false
	}
	if filepath.Ext(name) != segmentFileExt {
		return false
	}

	segmentID := strings.TrimPrefix(name, segmentFilePrefix)
	segmentID = strings.TrimSuffix(segmentID, segmentFileExt)
	if segmentID == "" {
		return false
	}
	_, err := strconv.ParseUint(segmentID, 10, 64)
	return err == nil
}

// Enqueue appends encoded messages to spool.
func (s *Manager) Enqueue(
	msgs []*common.Message,
	postEnqueue func(),
) (*Entry, error) {
	if len(msgs) == 0 {
		return &Entry{}, nil
	}

	entry := &Entry{
		accountingBytes: serializedMessageCountBytes,
	}
	for _, msg := range msgs {
		entry.accountingBytes += int64(serializedMessageHeaderBytes + len(msg.Key) + len(msg.Value))
		// todo: why only counter the value part ?
		entry.fileBytes += uint64(len(msg.Value))
	}

	var callbacksToRun []func()

	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		for _, callback := range callbacksToRun {
			if callback != nil {
				callback()
			}
		}
	}()

	if s.closed {
		return nil, errors.ErrInternalCheckFailed.GenWithStackByArgs("spool manager is closed")
	}

	shouldSpill := s.quota.shouldSpill(entry.accountingBytes)
	if shouldSpill {
		blob := serializeMessages(msgs)
		location, err := s.appendBlobLocked(blob)
		if err != nil {
			return nil, err
		}
		entry.location = location
	}
	if !shouldSpill {
		entry.memoryMsgs = msgs
	}
	entry.callbacks = detachCallbacks(msgs)
	callbacksToRun = s.quota.reserve(entry.accountingBytes, shouldSpill, postEnqueue)

	return entry, nil
}

func detachCallbacks(msgs []*common.Message) []func() {
	var callbacks []func()
	for _, msg := range msgs {
		if msg.Callback != nil {
			callbacks = append(callbacks, msg.Callback)
			msg.Callback = nil
		}
	}
	return callbacks
}

func runCallbacks(callbacks []func()) {
	for _, callback := range callbacks {
		if callback != nil {
			callback()
		}
	}
}

// Load fetches messages from memory or spilled segments.
func (s *Manager) Load(entry *Entry) ([]*common.Message, []func(), error) {
	if entry == nil {
		return nil, nil, nil
	}
	if entry.memoryMsgs != nil {
		return entry.memoryMsgs, entry.callbacks, nil
	}
	if entry.location == nil {
		return nil, entry.callbacks, nil
	}

	s.mu.Lock()
	spoolSegment := s.segments[entry.location.segmentID]
	if spoolSegment == nil {
		s.mu.Unlock()
		return nil, nil, errors.ErrInternalCheckFailed.GenWithStack(
			"spool segment %d not found",
			entry.location.segmentID,
		)
	}
	file := spoolSegment.file
	offset := entry.location.offset
	length := entry.location.length
	s.mu.Unlock()

	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		return nil, nil, errors.WrapError(errors.ErrUnexpected, err, "read spool segment file")
	}
	msgs, err := deserializeMessages(buf)
	if err != nil {
		return nil, nil, err
	}
	return msgs, entry.callbacks, nil
}

// Release releases memory or spilled bytes held by an entry.
func (s *Manager) Release(entry *Entry) {
	if entry == nil || entryConsumed(entry) {
		return
	}

	location, accountingBytes, spilled := consumeEntry(entry)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	if spilled {
		spoolSegment := s.segments[location.segmentID]
		if spoolSegment != nil {
			spoolSegment.refCnt--
			if spoolSegment.refCnt == 0 && s.activeSegment != spoolSegment {
				if err := spoolSegment.file.Close(); err != nil {
					log.Warn("close spool segment file failed", zap.Error(err))
				}
				if err := os.Remove(spoolSegment.path); err != nil {
					log.Warn(
						"remove spool segment file failed",
						zap.Error(err),
						zap.String("path", spoolSegment.path),
					)
				}
				delete(s.segments, spoolSegment.id)
			}
		}
	}
	callbacksToRun := s.quota.release(accountingBytes, spilled)
	s.mu.Unlock()

	for _, callback := range callbacksToRun {
		if callback != nil {
			callback()
		}
	}
}

// Discard runs the entry callbacks and then releases its local spool resources.
func (s *Manager) Discard(entry *Entry) {
	if entry == nil {
		return
	}

	runCallbacks(takeCallbacks(entry))
	s.Release(entry)
}

// Close closes spool and removes the segment files created by this spool instance.
func (s *Manager) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for _, spoolSegment := range s.segments {
		if spoolSegment.file != nil {
			if err := spoolSegment.file.Close(); err != nil {
				log.Warn("close spool segment failed",
					zap.String("path", spoolSegment.path),
					zap.Error(err))
			}
		}
	}
	s.quota.reset()
	s.mu.Unlock()

	if err := removeSpoolFiles(s.rootDir); err != nil {
		log.Warn("remove spool files failed",
			zap.String("path", s.rootDir),
			zap.Error(err))
	}
	s.quota.deleteMetrics()
}

func (s *Manager) appendBlobLocked(blob []byte) (*segmentLocation, error) {
	spoolSegment, err := s.getWritableSegmentLocked(int64(len(blob)))
	if err != nil {
		return nil, err
	}
	offset := spoolSegment.size
	n, err := spoolSegment.file.Write(blob)
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
	spoolSegment.size += int64(n)
	spoolSegment.refCnt++

	return &segmentLocation{
		segmentID: spoolSegment.id,
		offset:    offset,
		length:    int64(n),
	}, nil
}

func (s *Manager) getWritableSegmentLocked(needBytes int64) (*segment, error) {
	if s.activeSegment == nil || s.activeSegment.size+needBytes > s.segmentBytes {
		if err := s.rotateLocked(); err != nil {
			return nil, err
		}
	}
	return s.activeSegment, nil
}

func (s *Manager) rotateLocked() error {
	s.nextSegmentID++
	segmentID := s.nextSegmentID
	path := filepath.Join(
		s.rootDir,
		fmt.Sprintf("%s%06d%s", segmentFilePrefix, segmentID, segmentFileExt),
	)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return errors.WrapError(errors.ErrUnexpected, err, "open spool segment file")
	}
	spoolSegment := &segment{
		id:   segmentID,
		path: path,
		file: file,
	}
	s.segments[segmentID] = spoolSegment
	s.activeSegment = spoolSegment
	return nil
}

func takeCallbacks(entry *Entry) []func() {
	callbacks := entry.callbacks
	entry.callbacks = nil
	return callbacks
}

func entryConsumed(entry *Entry) bool {
	return entry.location == nil &&
		entry.memoryMsgs == nil &&
		entry.accountingBytes == 0 &&
		entry.fileBytes == 0 &&
		entry.callbacks == nil
}

func consumeEntry(entry *Entry) (*segmentLocation, int64, bool) {
	location := entry.location
	accountingBytes := entry.accountingBytes
	spilled := location != nil

	entry.memoryMsgs = nil
	entry.location = nil
	entry.callbacks = nil
	entry.accountingBytes = 0
	entry.fileBytes = 0
	return location, accountingBytes, spilled
}
