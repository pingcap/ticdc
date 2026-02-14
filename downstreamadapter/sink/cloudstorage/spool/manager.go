// Copyright 2025 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
)

const (
	defaultDirectoryName = "cloudstorage-sink-spool"

	defaultSegmentBytes = int64(64 * 1024 * 1024)

	defaultMemoryRatio        = 0.2
	defaultHighWatermarkRatio = 0.8
	defaultLowWatermarkRatio  = 0.6
)

// Options controls spool behavior.
type Options struct {
	RootDir string

	SegmentBytes int64

	MemoryRatio        float64
	HighWatermarkRatio float64
	LowWatermarkRatio  float64
}

// Manager stores encoded messages in memory first and spills to disk when needed.
type Manager struct {
	changefeedID commonType.ChangeFeedID
	rootDir      string

	quotaBytes int64

	memoryQuotaBytes   int64
	highWatermarkBytes int64
	lowWatermarkBytes  int64
	segmentBytes       int64

	metricMemoryBytes     interface{ Set(float64) }
	metricDiskBytes       interface{ Set(float64) }
	metricTotalBytes      interface{ Set(float64) }
	metricWakeSuppressed  interface{ Inc() }
	metricKeyspace        string
	metricChangefeedLabel string

	mu sync.Mutex

	memoryBytes int64
	diskBytes   int64

	wakeSuppressed bool
	pendingWake    []func()

	closed bool

	nextSegmentID uint64
	activeSegment *segment
	segments      map[uint64]*segment
}

type segment struct {
	id      uint64
	path    string
	file    *os.File
	size    int64
	refCnt  int64
	written int64
}

type pointer struct {
	segmentID uint64
	offset    int64
	length    int64
}

// Entry records where encoded messages are stored and how to release them.
type Entry struct {
	memoryMsgs []*codeccommon.Message
	pointer    *pointer

	callbacks []func()

	accountingBytes int64
	fileBytes       uint64
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
	return e != nil && e.pointer != nil
}

// InMemory returns whether this entry is still held in memory.
func (e *Entry) InMemory() bool {
	return e != nil && e.memoryMsgs != nil
}

// New creates a per-changefeed spool manager.
func New(
	changefeedID commonType.ChangeFeedID,
	quotaBytes int64,
	opts *Options,
) (*Manager, error) {
	options := withDefaultOptions(opts)
	if quotaBytes <= 0 {
		return nil, errors.Errorf("invalid spool quota %d", quotaBytes)
	}
	if options.SegmentBytes <= 0 {
		return nil, errors.Errorf("invalid spool segment size %d", options.SegmentBytes)
	}
	if options.MemoryRatio <= 0 || options.MemoryRatio >= 1 {
		return nil, errors.Errorf("invalid spool memory ratio %f", options.MemoryRatio)
	}
	if options.LowWatermarkRatio <= 0 || options.LowWatermarkRatio >= 1 {
		return nil, errors.Errorf("invalid spool low watermark ratio %f", options.LowWatermarkRatio)
	}
	if options.HighWatermarkRatio <= 0 || options.HighWatermarkRatio >= 1 {
		return nil, errors.Errorf("invalid spool high watermark ratio %f", options.HighWatermarkRatio)
	}
	if options.LowWatermarkRatio >= options.HighWatermarkRatio {
		return nil, errors.Errorf(
			"invalid spool watermark ratio, low: %f high: %f",
			options.LowWatermarkRatio,
			options.HighWatermarkRatio,
		)
	}

	rootDir := options.RootDir
	if rootDir == "" {
		dataDir := config.GetGlobalServerConfig().DataDir
		if dataDir == "" {
			dataDir = os.TempDir()
		}
		rootDir = filepath.Join(
			dataDir,
			defaultDirectoryName,
			changefeedID.Keyspace(),
			fmt.Sprintf("%s-%s", changefeedID.Name(), changefeedID.ID().String()),
		)
	}

	if err := os.RemoveAll(rootDir); err != nil {
		return nil, errors.Trace(err)
	}
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, errors.Trace(err)
	}

	changefeedLabel := changefeedID.ID().String()
	manager := &Manager{
		changefeedID: changefeedID,
		rootDir:      rootDir,
		quotaBytes:   quotaBytes,

		memoryQuotaBytes:   int64(float64(quotaBytes) * options.MemoryRatio),
		highWatermarkBytes: int64(float64(quotaBytes) * options.HighWatermarkRatio),
		lowWatermarkBytes:  int64(float64(quotaBytes) * options.LowWatermarkRatio),
		segmentBytes:       options.SegmentBytes,

		metricMemoryBytes: metrics.CloudStorageSpoolMemoryBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricDiskBytes: metrics.CloudStorageSpoolDiskBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricTotalBytes: metrics.CloudStorageSpoolTotalBytesGauge.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricWakeSuppressed: metrics.CloudStorageWakeSuppressedCounter.WithLabelValues(
			changefeedID.Keyspace(), changefeedLabel),
		metricKeyspace:        changefeedID.Keyspace(),
		metricChangefeedLabel: changefeedLabel,

		segments: make(map[uint64]*segment),
	}
	manager.updateMetricsLocked()
	return manager, nil
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
	if opts.SegmentBytes > 0 {
		result.SegmentBytes = opts.SegmentBytes
	}
	if opts.MemoryRatio > 0 {
		result.MemoryRatio = opts.MemoryRatio
	}
	if opts.HighWatermarkRatio > 0 {
		result.HighWatermarkRatio = opts.HighWatermarkRatio
	}
	if opts.LowWatermarkRatio > 0 {
		result.LowWatermarkRatio = opts.LowWatermarkRatio
	}
	return result
}

// Enqueue appends encoded messages to spool.
func (s *Manager) Enqueue(
	msgs []*codeccommon.Message,
	onEnqueued func(),
) (*Entry, error) {
	if len(msgs) == 0 {
		return &Entry{}, nil
	}

	entry := &Entry{
		callbacks: make([]func(), 0, len(msgs)),
	}
	for _, msg := range msgs {
		entry.accountingBytes += int64(len(msg.Key) + len(msg.Value))
		entry.fileBytes += uint64(len(msg.Value))
		entry.callbacks = append(entry.callbacks, msg.Callback)
		msg.Callback = nil
	}

	callbacksToRun := make([]func(), 0, 1)
	needSuppressedMetric := false

	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		for _, callback := range callbacksToRun {
			if callback != nil {
				callback()
			}
		}
		if needSuppressedMetric {
			s.metricWakeSuppressed.Inc()
		}
	}()

	if s.closed {
		return nil, errors.New("spool is closed")
	}

	shouldSpill := s.memoryBytes+entry.accountingBytes > s.memoryQuotaBytes
	if shouldSpill {
		blob, err := serializeMessages(msgs)
		if err != nil {
			return nil, errors.Trace(err)
		}
		pointerValue, err := s.appendBlobLocked(blob)
		if err != nil {
			return nil, err
		}
		entry.pointer = pointerValue
		s.diskBytes += entry.accountingBytes
	} else {
		entry.memoryMsgs = msgs
		s.memoryBytes += entry.accountingBytes
	}

	totalBytes := s.memoryBytes + s.diskBytes
	if totalBytes > s.highWatermarkBytes {
		s.wakeSuppressed = true
	}
	if s.wakeSuppressed {
		if onEnqueued != nil {
			s.pendingWake = append(s.pendingWake, onEnqueued)
			needSuppressedMetric = true
		}
	} else if onEnqueued != nil {
		callbacksToRun = append(callbacksToRun, onEnqueued)
	}
	s.updateMetricsLocked()

	return entry, nil
}

// Load fetches messages from memory or spilled segments.
func (s *Manager) Load(entry *Entry) ([]*codeccommon.Message, []func(), error) {
	if entry == nil {
		return nil, nil, nil
	}
	if entry.memoryMsgs != nil {
		return entry.memoryMsgs, entry.callbacks, nil
	}
	if entry.pointer == nil {
		return nil, entry.callbacks, nil
	}

	s.mu.Lock()
	spoolSegment := s.segments[entry.pointer.segmentID]
	if spoolSegment == nil {
		s.mu.Unlock()
		return nil, nil, errors.Errorf("spool segment %d not found", entry.pointer.segmentID)
	}
	file := spoolSegment.file
	offset := entry.pointer.offset
	length := entry.pointer.length
	s.mu.Unlock()

	buf := make([]byte, length)
	if _, err := file.ReadAt(buf, offset); err != nil {
		return nil, nil, errors.Trace(err)
	}
	msgs, err := deserializeMessages(buf)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return msgs, entry.callbacks, nil
}

// Release releases memory or spilled bytes held by an entry.
func (s *Manager) Release(entry *Entry) {
	if entry == nil {
		return
	}

	callbacksToRun := make([]func(), 0, len(s.pendingWake))
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	if entry.pointer != nil {
		spoolSegment := s.segments[entry.pointer.segmentID]
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
		s.diskBytes -= entry.accountingBytes
	} else {
		s.memoryBytes -= entry.accountingBytes
	}
	if s.memoryBytes < 0 {
		s.memoryBytes = 0
	}
	if s.diskBytes < 0 {
		s.diskBytes = 0
	}

	totalBytes := s.memoryBytes + s.diskBytes
	if s.wakeSuppressed && totalBytes <= s.lowWatermarkBytes {
		s.wakeSuppressed = false
		callbacksToRun = append(callbacksToRun, s.pendingWake...)
		s.pendingWake = s.pendingWake[:0]
	}
	s.updateMetricsLocked()
	s.mu.Unlock()

	for _, callback := range callbacksToRun {
		if callback != nil {
			callback()
		}
	}
}

// Close closes spool and removes temporary files.
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
	s.memoryBytes = 0
	s.diskBytes = 0
	s.pendingWake = nil
	s.updateMetricsLocked()
	s.mu.Unlock()

	if err := os.RemoveAll(s.rootDir); err != nil {
		log.Warn("remove spool directory failed",
			zap.String("path", s.rootDir),
			zap.Error(err))
	}

	metrics.CloudStorageSpoolMemoryBytesGauge.DeleteLabelValues(s.metricKeyspace, s.metricChangefeedLabel)
	metrics.CloudStorageSpoolDiskBytesGauge.DeleteLabelValues(s.metricKeyspace, s.metricChangefeedLabel)
	metrics.CloudStorageSpoolTotalBytesGauge.DeleteLabelValues(s.metricKeyspace, s.metricChangefeedLabel)
	metrics.CloudStorageWakeSuppressedCounter.DeleteLabelValues(s.metricKeyspace, s.metricChangefeedLabel)
}

func (s *Manager) appendBlobLocked(blob []byte) (*pointer, error) {
	spoolSegment, err := s.getWritableSegmentLocked(int64(len(blob)))
	if err != nil {
		return nil, err
	}
	offset := spoolSegment.size
	n, err := spoolSegment.file.Write(blob)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != len(blob) {
		return nil, errors.Errorf("short write to spool segment, expected %d got %d", len(blob), n)
	}
	spoolSegment.size += int64(n)
	spoolSegment.written += int64(n)
	spoolSegment.refCnt++

	return &pointer{
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
	path := filepath.Join(s.rootDir, fmt.Sprintf("segment-%06d.log", segmentID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return errors.Trace(err)
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

func (s *Manager) updateMetricsLocked() {
	s.metricMemoryBytes.Set(float64(s.memoryBytes))
	s.metricDiskBytes.Set(float64(s.diskBytes))
	s.metricTotalBytes.Set(float64(s.memoryBytes + s.diskBytes))
}
