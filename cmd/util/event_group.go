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

package util

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/spill"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	eventsGroupSpillDirPattern = "ticdc-events-group-*"
	payloadSpillPattern        = "payload-*.spill"
	spillIndexDir              = "index"
)

const (
	defaultSpillSegmentBytes    = 128 * 1024 * 1024
	defaultSpillSegmentMessages = 100000
	defaultPendingHighBytes     = 1024 * 1024 * 1024
	defaultPendingLowBytes      = 512 * 1024 * 1024
	defaultResolveBatchBytes    = 64 * 1024 * 1024
	defaultResolveBatchMessages = 10000
	defaultMessageMetadataBytes = 128
	defaultIndexBatchMessages   = 10000
	defaultDiskCheckBytes       = 16 * 1024 * 1024
	defaultDiskUsageLimit       = 0.90
	defaultIndexCacheBytes      = 32 * 1024 * 1024
	defaultIndexMemTableBytes   = 16 * 1024 * 1024
	spillRecordLengthBytes      = 8
	eventIndexKeyBytes          = 3 * 8
	eventIndexValueBytes        = 6 * 8
)

type spillConfig struct {
	segmentBytes         int64
	segmentMessages      int
	pendingHighBytes     int64
	pendingLowBytes      int64
	resolveBatchBytes    int64
	resolveBatchMessages int
	messageMetadataBytes int64
	indexBatchMessages   int
	diskCheckBytes       int64
	diskUsageLimit       float64
}

func defaultSpillConfig() spillConfig {
	return spillConfig{
		segmentBytes:         defaultSpillSegmentBytes,
		segmentMessages:      defaultSpillSegmentMessages,
		pendingHighBytes:     defaultPendingHighBytes,
		pendingLowBytes:      defaultPendingLowBytes,
		resolveBatchBytes:    defaultResolveBatchBytes,
		resolveBatchMessages: defaultResolveBatchMessages,
		messageMetadataBytes: defaultMessageMetadataBytes,
		indexBatchMessages:   defaultIndexBatchMessages,
		diskCheckBytes:       defaultDiskCheckBytes,
		diskUsageLimit:       defaultDiskUsageLimit,
	}
}

// ResolveLimit bounds one batch restored from spill.
type ResolveLimit struct {
	MaxBytes    int64
	MaxMessages int
}

// SpillStats reports process-wide payload I/O and live spill state.
type SpillStats struct {
	PendingBytes       int64
	PayloadWriteBytes  int64
	PayloadReadBytes   int64
	PayloadWriteCount  int64
	PayloadReadCount   int64
	PayloadDecodeCount int64
	LivePayloads       int
	LiveSegments       int
	IndexWriteCount    int64
	IndexReadCount     int64
	AppliedEventCount  int64
}

type filesystemUsage struct {
	usedBytes  uint64
	totalBytes uint64
}

type payloadLocation struct {
	segmentID uint64
	handle    spill.Handle
}

type spillSegment struct {
	id            uint64
	file          *spill.RecordFile
	bytes         int64
	payloadCount  int
	pendingEvents int64
}

type registeredRestorer struct {
	decode func([]byte) ([]*codeccommon.DMLMessage, error)
	refs   int64
}

type payloadCacheKey struct {
	segmentID uint64
	offset    int64
}

type payloadCacheEntry struct {
	key      payloadCacheKey
	messages []*codeccommon.DMLMessage
	bytes    int64
	pins     int
	element  *list.Element
}

type spilledMessage struct {
	key            []byte
	commitTs       uint64
	location       payloadLocation
	dmlIndex       uint64
	restorerID     uint64
	sourcePosition int64
}

// SpillStore owns a disk-backed ordered event index and append-only payload
// segments shared by all event groups in one consumer. Pending DML descriptors
// live in Pebble rather than one Go object per row. Only a bounded restore cache
// and segment-level reference counts remain in memory. Filesystem usage is
// checked periodically while appending and before every applied-range delete;
// crossing the hard limit latches an error so the consumer terminates.
type SpillStore struct {
	id                  uint64
	config              spillConfig
	rootDir             string
	index               *pebble.DB
	indexCache          *pebble.Cache
	indexBatch          *pebble.Batch
	indexBatchCount     int
	segments            map[uint64]*spillSegment
	activeSegment       *spillSegment
	restorers           map[uint64]*registeredRestorer
	cache               map[payloadCacheKey]*payloadCacheEntry
	cacheLRU            list.List
	cacheBytes          int64
	pendingBytes        int64
	livePayloads        int
	draining            bool
	nextSegmentID       uint64
	nextGroupID         uint64
	nextSequence        uint64
	bytesSinceDiskCheck int64
	terminalErr         error

	readRecord func(*spill.RecordFile, spill.Handle) ([]byte, error)
	diskUsage  func(string) (filesystemUsage, error)
	stats      SpillStats
}

var nextSpillStoreID atomic.Uint64

// NewSpillStore creates a process-wide store with the default spill limits.
func NewSpillStore() *SpillStore {
	return newSpillStore(defaultSpillConfig())
}

func newSpillStore(config spillConfig) *SpillStore {
	return &SpillStore{
		id:        nextSpillStoreID.Add(1),
		config:    config,
		segments:  make(map[uint64]*spillSegment),
		restorers: make(map[uint64]*registeredRestorer),
		cache:     make(map[payloadCacheKey]*payloadCacheEntry),
		readRecord: func(file *spill.RecordFile, handle spill.Handle) ([]byte, error) {
			return file.Read(handle)
		},
		diskUsage: getFilesystemUsage,
	}
}

// ResolveLimit returns the configured per-batch restore limit.
func (s *SpillStore) ResolveLimit() ResolveLimit {
	return ResolveLimit{
		MaxBytes:    s.config.resolveBatchBytes,
		MaxMessages: s.config.resolveBatchMessages,
	}
}

// PendingBytes returns conservatively accounted payload, decoded cache, and
// logical index bytes. Physical index usage is covered by the filesystem guard.
func (s *SpillStore) PendingBytes() int64 {
	return s.pendingBytes
}

// ShouldDrain reports whether pending spill data has crossed the high watermark and not yet fallen below the low watermark.
func (s *SpillStore) ShouldDrain() bool {
	return s.draining
}

// Stats returns a snapshot of process-wide spill activity.
func (s *SpillStore) Stats() SpillStats {
	stats := s.stats
	stats.PendingBytes = s.pendingBytes
	stats.LivePayloads = s.livePayloads
	stats.LiveSegments = len(s.segments)
	return stats
}

func (s *SpillStore) addPending(bytes int64) {
	if bytes <= 0 {
		return
	}
	s.pendingBytes += bytes
	if !s.draining && s.pendingBytes >= s.config.pendingHighBytes {
		s.draining = true
		log.Info("spill pending bytes reached high watermark",
			zap.Int64("pendingBytes", s.pendingBytes),
			zap.Int64("highWatermarkBytes", s.config.pendingHighBytes),
			zap.Int64("lowWatermarkBytes", s.config.pendingLowBytes))
	}
}

func (s *SpillStore) releasePending(bytes int64) {
	if bytes <= 0 {
		return
	}
	s.pendingBytes -= bytes
	if s.pendingBytes < 0 {
		log.Panic("spill pending bytes underflow",
			zap.Int64("releasedBytes", bytes), zap.Int64("pendingBytes", s.pendingBytes))
	}
	if s.draining && s.pendingBytes <= s.config.pendingLowBytes {
		s.draining = false
		log.Info("spill pending bytes fell below low watermark",
			zap.Int64("pendingBytes", s.pendingBytes),
			zap.Int64("highWatermarkBytes", s.config.pendingHighBytes),
			zap.Int64("lowWatermarkBytes", s.config.pendingLowBytes))
	}
}

// EventsGroup stores change event messages.
type EventsGroup struct {
	Partition int32
	tableID   int64

	id             uint64
	store          *SpillStore
	ownsStore      bool
	pendingCount   int64
	segmentRefs    map[uint64]int64
	restorerRefs   map[uint64]int64
	batchPending   bool
	outOfOrder     bool
	lastAppendedTs uint64
	resolvedTs     uint64
	appliedTs      uint64
	postRestore    func(*codeccommon.DMLMessage, int64) *codeccommon.DMLMessage
	HighWatermark  uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64, stores ...*SpillStore) *EventsGroup {
	store := NewSpillStore()
	ownsStore := true
	if len(stores) != 0 && stores[0] != nil {
		store = stores[0]
		ownsStore = false
	}
	store.nextGroupID++
	return &EventsGroup{
		Partition:    partition,
		tableID:      tableID,
		id:           store.nextGroupID,
		store:        store,
		ownsStore:    ownsStore,
		segmentRefs:  make(map[uint64]int64),
		restorerRefs: make(map[uint64]int64),
	}
}

// SetPostRestore installs a bounded group-level hook for source metadata that
// must be applied after lazy decoding. Unlike the old per-input closure, this
// hook does not grow with the backlog.
func (g *EventsGroup) SetPostRestore(
	restore func(*codeccommon.DMLMessage, int64) *codeccommon.DMLMessage,
) {
	g.postRestore = restore
}

func (s *SpillStore) ensureOpen() error {
	if s.terminalErr != nil {
		return s.terminalErr
	}
	if s.index != nil {
		return nil
	}

	rootDir, err := os.MkdirTemp(os.TempDir(), eventsGroupSpillDirPattern)
	if err != nil {
		return errors.WrapError(errors.ErrSpillFileOp, err, "create spill store directory")
	}
	s.rootDir = rootDir
	if err := s.checkDiskUsage(0, true); err != nil {
		return err
	}

	cache := pebble.NewCache(defaultIndexCacheBytes)
	options := &pebble.Options{
		Cache:                       cache,
		DisableWAL:                  true,
		MaxOpenFiles:                128,
		MemTableSize:                defaultIndexMemTableBytes,
		MemTableStopWritesThreshold: 2,
		MaxConcurrentCompactions:    func() int { return 1 },
	}
	index, err := pebble.Open(filepath.Join(rootDir, spillIndexDir), options)
	if err != nil {
		cache.Unref()
		return errors.WrapError(errors.ErrSpillFileOp, err, "open spill event index")
	}
	s.indexCache = cache
	s.index = index
	s.indexBatch = index.NewBatch()
	return nil
}

func (s *SpillStore) newSegment() error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.checkDiskUsage(0, true); err != nil {
		return err
	}
	file, err := spill.NewRecordFile(s.rootDir, payloadSpillPattern)
	if err != nil {
		return err
	}
	s.nextSegmentID++
	segment := &spillSegment{id: s.nextSegmentID, file: file}
	s.segments[segment.id] = segment
	s.activeSegment = segment
	return nil
}

func (s *SpillStore) prepareSegment(recordBytes int64) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	segment := s.activeSegment
	if segment != nil && (segment.bytes+recordBytes > s.config.segmentBytes ||
		segment.payloadCount >= s.config.segmentMessages) {
		s.activeSegment = nil
	}
	if s.activeSegment == nil {
		return s.newSegment()
	}
	return nil
}

func (s *SpillStore) sealFullSegment() {
	segment := s.activeSegment
	if segment == nil {
		return
	}
	if segment.bytes >= s.config.segmentBytes ||
		segment.payloadCount >= s.config.segmentMessages {
		s.activeSegment = nil
	}
}

func spillMessageDataSize(key, value []byte) int64 {
	if len(key) == 0 && len(value) == 0 {
		return spillRecordLengthBytes + 1
	}
	return spillRecordLengthBytes + 2*8 + int64(len(key)) + int64(len(value))
}

func appendMessageData(file *spill.RecordFile, key, value []byte) (spill.Handle, error) {
	if len(key) == 0 && len(value) == 0 {
		return file.AppendChunks([]byte{0})
	}
	var keyLen, valueLen [8]byte
	binary.BigEndian.PutUint64(keyLen[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLen[:], uint64(len(value)))
	return file.AppendChunks(keyLen[:], key, valueLen[:], value)
}

func (s *SpillStore) acquirePayload(data *codeccommon.DMLMessageData) (payloadLocation, error) {
	if segmentID, offset, length, ok := data.SpillLocation(s.id); ok {
		if _, exists := s.segments[segmentID]; exists {
			return payloadLocation{
				segmentID: segmentID,
				handle:    spill.Handle{Offset: offset, Length: length},
			}, nil
		}
	}
	recordBytes := spillMessageDataSize(data.Key, data.Value)
	if err := s.checkDiskUsage(recordBytes, false); err != nil {
		return payloadLocation{}, err
	}
	if err := s.prepareSegment(recordBytes); err != nil {
		return payloadLocation{}, err
	}
	segment := s.activeSegment
	handle, err := appendMessageData(segment.file, data.Key, data.Value)
	if err != nil {
		return payloadLocation{}, err
	}
	data.SetSpillLocation(s.id, segment.id, handle.Offset, handle.Length)
	segment.bytes += recordBytes
	segment.payloadCount++
	s.livePayloads++
	s.addPending(recordBytes)
	s.stats.PayloadWriteBytes += int64(handle.Length)
	s.stats.PayloadWriteCount++
	s.sealFullSegment()
	return payloadLocation{segmentID: segment.id, handle: handle}, nil
}

func (s *SpillStore) cleanupSegment(segment *spillSegment) {
	if s.activeSegment == segment {
		s.activeSegment = nil
	}
	s.evictSegmentCache(segment.id)
	if err := segment.file.Cleanup(); err != nil {
		log.Warn("cleanup spill segment failed", zap.String("path", segment.file.Path()), zap.Error(err))
		return
	}
	s.releasePending(segment.bytes)
	s.livePayloads -= segment.payloadCount
	delete(s.segments, segment.id)
}

func (s *SpillStore) appendEventIndex(key, value []byte) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := s.checkDiskUsage(int64(len(key)+len(value)), false); err != nil {
		return err
	}
	if err := s.indexBatch.Set(key, value, nil); err != nil {
		return errors.WrapError(errors.ErrSpillFileOp, err, "append spill event index")
	}
	s.indexBatchCount++
	if s.indexBatchCount >= s.config.indexBatchMessages {
		return s.flushEventIndex()
	}
	return nil
}

func (s *SpillStore) flushEventIndex() error {
	if s.indexBatchCount == 0 {
		return nil
	}
	if err := s.indexBatch.Commit(pebble.NoSync); err != nil {
		return errors.WrapError(errors.ErrSpillFileOp, err, "commit spill event index")
	}
	if err := s.indexBatch.Close(); err != nil {
		return errors.WrapError(errors.ErrSpillFileOp, err, "close committed spill event index batch")
	}
	s.indexBatch = s.index.NewBatch()
	s.indexBatchCount = 0
	return nil
}

func encodeEventIndexKey(groupID, commitTs, sequence uint64) []byte {
	key := make([]byte, eventIndexKeyBytes)
	binary.BigEndian.PutUint64(key[0:8], groupID)
	binary.BigEndian.PutUint64(key[8:16], commitTs)
	binary.BigEndian.PutUint64(key[16:24], sequence)
	return key
}

func eventIndexBounds(groupID uint64) ([]byte, []byte) {
	lower := make([]byte, 8)
	upper := make([]byte, 8)
	binary.BigEndian.PutUint64(lower, groupID)
	binary.BigEndian.PutUint64(upper, groupID+1)
	return lower, upper
}

func encodeEventIndexValue(
	location payloadLocation, dmlIndex, restorerID uint64, sourcePosition int64,
) []byte {
	value := make([]byte, eventIndexValueBytes)
	binary.BigEndian.PutUint64(value[0:8], location.segmentID)
	binary.BigEndian.PutUint64(value[8:16], uint64(location.handle.Offset))
	binary.BigEndian.PutUint64(value[16:24], location.handle.Length)
	binary.BigEndian.PutUint64(value[24:32], dmlIndex)
	binary.BigEndian.PutUint64(value[32:40], restorerID)
	binary.BigEndian.PutUint64(value[40:48], uint64(sourcePosition))
	return value
}

func decodeEventIndexCommitTs(key []byte) (uint64, error) {
	if len(key) != eventIndexKeyBytes {
		return 0, errors.ErrSpillFileOp.FastGenByArgs("invalid spill event index key")
	}
	return binary.BigEndian.Uint64(key[8:16]), nil
}

func decodeSpilledMessage(key, value []byte) (spilledMessage, error) {
	commitTs, err := decodeEventIndexCommitTs(key)
	if err != nil {
		return spilledMessage{}, err
	}
	if len(value) != eventIndexValueBytes {
		return spilledMessage{}, errors.ErrSpillFileOp.FastGenByArgs("invalid spill event index value")
	}
	return spilledMessage{
		key:      append([]byte(nil), key...),
		commitTs: commitTs,
		location: payloadLocation{
			segmentID: binary.BigEndian.Uint64(value[0:8]),
			handle: spill.Handle{
				Offset: int64(binary.BigEndian.Uint64(value[8:16])),
				Length: binary.BigEndian.Uint64(value[16:24]),
			},
		},
		dmlIndex:       binary.BigEndian.Uint64(value[24:32]),
		restorerID:     binary.BigEndian.Uint64(value[32:40]),
		sourcePosition: int64(binary.BigEndian.Uint64(value[40:48])),
	}, nil
}

func (s *SpillStore) trimPayloadCache() {
	byteLimit := s.config.resolveBatchBytes
	if byteLimit <= 0 {
		byteLimit = defaultResolveBatchBytes
	}
	entryLimit := s.config.resolveBatchMessages
	if entryLimit <= 0 {
		entryLimit = defaultResolveBatchMessages
	}
	for s.cacheBytes > byteLimit || len(s.cache) > entryLimit {
		var victim *payloadCacheEntry
		for element := s.cacheLRU.Back(); element != nil; element = element.Prev() {
			entry := element.Value.(*payloadCacheEntry)
			if entry.pins == 0 {
				victim = entry
				break
			}
		}
		if victim == nil {
			return
		}
		s.removePayloadCacheEntry(victim)
	}
}

func (s *SpillStore) removePayloadCacheEntry(entry *payloadCacheEntry) {
	if entry == nil || entry.element == nil {
		return
	}
	delete(s.cache, entry.key)
	s.cacheLRU.Remove(entry.element)
	entry.element = nil
	entry.messages = nil
	s.cacheBytes -= entry.bytes
	s.releasePending(entry.bytes)
}

func (s *SpillStore) unpinPayloads(payloads []*payloadCacheEntry) {
	for _, payload := range payloads {
		payload.pins--
		if payload.pins < 0 {
			log.Panic("spill payload cache pin underflow",
				zap.Uint64("segmentID", payload.key.segmentID), zap.Int64("offset", payload.key.offset))
		}
	}
	s.trimPayloadCache()
}

func (s *SpillStore) evictSegmentCache(segmentID uint64) {
	for _, entry := range s.cache {
		if entry.key.segmentID == segmentID {
			s.removePayloadCacheEntry(entry)
		}
	}
}

func (g *EventsGroup) releaseEvent(segmentID, restorerID uint64, count int64) {
	g.releaseSegmentRefs(segmentID, count)
	g.releaseRestorerRefs(restorerID, count)
}

func (g *EventsGroup) releaseSegmentRefs(segmentID uint64, count int64) {
	segment := g.store.segments[segmentID]
	if segment == nil || segment.pendingEvents < count || g.segmentRefs[segmentID] < count {
		log.Panic("spill segment reference underflow",
			zap.Uint64("segmentID", segmentID), zap.Int64("released", count))
	}
	segment.pendingEvents -= count
	g.segmentRefs[segmentID] -= count
	if g.segmentRefs[segmentID] == 0 {
		delete(g.segmentRefs, segmentID)
	}
	if segment.pendingEvents == 0 {
		g.store.cleanupSegment(segment)
	}
}

func (g *EventsGroup) releaseRestorerRefs(restorerID uint64, count int64) {
	restorer := g.store.restorers[restorerID]
	if restorer == nil || restorer.refs < count || g.restorerRefs[restorerID] < count {
		log.Panic("spill restorer reference underflow",
			zap.Uint64("restorerID", restorerID), zap.Int64("released", count))
	}
	restorer.refs -= count
	g.restorerRefs[restorerID] -= count
	if g.restorerRefs[restorerID] == 0 {
		delete(g.restorerRefs, restorerID)
	}
	if restorer.refs == 0 {
		delete(g.store.restorers, restorerID)
	}
}

func getFilesystemUsage(path string) (filesystemUsage, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return filesystemUsage{}, errors.WrapError(errors.ErrSpillFileOp, err, "read spill filesystem usage")
	}
	if stat.Bsize <= 0 {
		return filesystemUsage{}, errors.ErrSpillFileOp.FastGenByArgs("spill filesystem has invalid block size")
	}
	blockSize := uint64(stat.Bsize)
	totalBytes := uint64(stat.Blocks) * blockSize
	availableBytes := uint64(stat.Bavail) * blockSize
	if availableBytes > totalBytes {
		return filesystemUsage{}, errors.ErrSpillFileOp.FastGenByArgs("spill filesystem has invalid available blocks")
	}
	return filesystemUsage{usedBytes: totalBytes - availableBytes, totalBytes: totalBytes}, nil
}

func (s *SpillStore) checkDiskUsage(nextWriteBytes int64, force bool) error {
	if s.terminalErr != nil {
		return s.terminalErr
	}
	s.bytesSinceDiskCheck += nextWriteBytes
	if !force && s.config.diskCheckBytes > 0 && s.bytesSinceDiskCheck < s.config.diskCheckBytes {
		return nil
	}
	path := s.rootDir
	if path == "" {
		path = os.TempDir()
	}
	usage, err := s.diskUsage(path)
	if err != nil {
		return err
	}
	s.bytesSinceDiskCheck = 0
	if usage.totalBytes == 0 {
		return errors.ErrSpillFileOp.FastGenByArgs("spill filesystem has zero capacity")
	}
	projectedUsed := usage.usedBytes
	if nextWriteBytes > 0 && uint64(nextWriteBytes) <= ^uint64(0)-projectedUsed {
		projectedUsed += uint64(nextWriteBytes)
	}
	ratio := float64(projectedUsed) / float64(usage.totalBytes)
	if ratio <= s.config.diskUsageLimit {
		return nil
	}
	message := fmt.Sprintf(
		"spill filesystem usage %.2f%% exceeds %.2f%% limit: path=%s usedBytes=%d totalBytes=%d",
		ratio*100, s.config.diskUsageLimit*100, path, projectedUsed, usage.totalBytes)
	s.terminalErr = errors.ErrSpillFileOp.FastGenByArgs(message)
	log.Error("spill filesystem usage limit exceeded; stop consumer",
		zap.String("path", path),
		zap.Uint64("usedBytes", projectedUsed),
		zap.Uint64("totalBytes", usage.totalBytes),
		zap.Float64("usagePercent", ratio*100),
		zap.Float64("limitPercent", s.config.diskUsageLimit*100))
	return s.terminalErr
}

// Cleanup removes all temporary index and payload state when a consumer stops.
func (s *SpillStore) Cleanup() error {
	var cleanupErr error
	if s.indexBatch != nil {
		if err := s.indexBatch.Close(); err != nil && cleanupErr == nil {
			cleanupErr = errors.WrapError(errors.ErrSpillFileOp, err, "close spill event index batch")
		}
		s.indexBatch = nil
	}
	if s.index != nil {
		if err := s.index.Close(); err != nil && cleanupErr == nil {
			cleanupErr = errors.WrapError(errors.ErrSpillFileOp, err, "close spill event index")
		}
		s.index = nil
	}
	if s.indexCache != nil {
		s.indexCache.Unref()
		s.indexCache = nil
	}
	for _, segment := range s.segments {
		if err := segment.file.Cleanup(); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
	}
	if s.rootDir != "" {
		if err := os.RemoveAll(s.rootDir); err != nil && cleanupErr == nil {
			cleanupErr = errors.WrapError(errors.ErrSpillFileOp, err, "remove spill store directory")
		}
	}
	s.rootDir = ""
	s.activeSegment = nil
	clear(s.segments)
	clear(s.restorers)
	clear(s.cache)
	s.cacheLRU.Init()
	s.cacheBytes = 0
	s.pendingBytes = 0
	s.livePayloads = 0
	s.draining = false
	s.indexBatchCount = 0
	return cleanupErr
}

// AppendMessage appends an opaque codec payload to the spill file. It does
// not call DMLMessage.ToDMLEvent; the restored message remains lazy until the
// consumer flushes it against a watermark or DDL barrier.
func (g *EventsGroup) AppendMessage(
	message *codeccommon.DMLMessage,
) error {
	if message == nil {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot spill nil DML message")
	}
	messageData, dmlIndex := message.SpillData()
	if messageData == nil || messageData.Restorer == nil || messageData.Restorer.Decode == nil {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot spill DML message without decode function")
	}
	commitTs := message.GetCommitTs()
	location, err := g.store.acquirePayload(messageData)
	if err != nil {
		return err
	}
	if g.pendingCount > 0 && commitTs < g.lastAppendedTs {
		g.outOfOrder = true
	}
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}
	g.lastAppendedTs = commitTs
	g.store.nextSequence++
	key := encodeEventIndexKey(g.id, commitTs, g.store.nextSequence)
	value := encodeEventIndexValue(location, dmlIndex, messageData.Restorer.ID, messageData.SourcePosition)
	if err := g.store.appendEventIndex(key, value); err != nil {
		return err
	}

	segment := g.store.segments[location.segmentID]
	segment.pendingEvents++
	restorer := g.store.restorers[messageData.Restorer.ID]
	if restorer == nil {
		restorer = &registeredRestorer{decode: messageData.Restorer.Decode}
		g.store.restorers[messageData.Restorer.ID] = restorer
	}
	restorer.refs++
	g.pendingCount++
	g.segmentRefs[location.segmentID]++
	g.restorerRefs[messageData.Restorer.ID]++
	g.store.addPending(g.store.config.messageMetadataBytes)
	g.store.stats.IndexWriteCount++
	return nil
}

// ResolveBatch owns a prepared group prefix until the downstream confirms it.
type ResolveBatch struct {
	Messages      []*codeccommon.DMLMessage
	ResolvedBytes int64
	group         *EventsGroup
	entries       []spilledMessage
	payloads      []*payloadCacheEntry
	acked         bool
}

// Ack releases a prepared group prefix after downstream flush callbacks complete.
func (b *ResolveBatch) Ack() error {
	if b == nil || b.acked {
		return nil
	}
	if err := b.group.ack(b); err != nil {
		return err
	}
	b.acked = true
	return nil
}

// PrepareResolve restores one bounded group prefix without removing it.
func (g *EventsGroup) PrepareResolve(
	resolve uint64, limit ResolveLimit,
) (*ResolveBatch, bool, error) {
	if g.batchPending {
		return nil, false, errors.ErrSpillFileOp.FastGenByArgs("events group already has a pending resolve batch")
	}
	if resolve > g.resolvedTs {
		g.resolvedTs = resolve
	}
	if g.pendingCount == 0 {
		return nil, false, nil
	}
	if err := g.store.flushEventIndex(); err != nil {
		return nil, false, err
	}

	lower, upper := eventIndexBounds(g.id)
	iterator, err := g.store.index.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, false, errors.WrapError(errors.ErrSpillFileOp, err, "create spill event iterator")
	}
	defer iterator.Close()

	entries := make([]spilledMessage, 0, boundedMessageCapacity(limit))
	seenPayloads := make(map[payloadCacheKey]struct{})
	var plannedBytes int64
	var lastCommitTs uint64
	valid := iterator.First()
	for valid {
		commitTs, err := decodeEventIndexCommitTs(iterator.Key())
		if err != nil {
			return nil, false, err
		}
		if commitTs > g.resolvedTs {
			break
		}
		entry, err := decodeSpilledMessage(iterator.Key(), iterator.Value())
		if err != nil {
			return nil, false, err
		}
		payloadKey := payloadCacheKey{segmentID: entry.location.segmentID, offset: entry.location.handle.Offset}
		additionalBytes := int64(0)
		if _, ok := seenPayloads[payloadKey]; !ok {
			if _, cached := g.store.cache[payloadKey]; !cached {
				additionalBytes = int64(entry.location.handle.Length)
			}
		}
		if len(entries) > 0 && exceedsResolveLimit(len(entries), plannedBytes, additionalBytes, limit) &&
			commitTs != lastCommitTs {
			break
		}
		entries = append(entries, entry)
		plannedBytes += additionalBytes
		seenPayloads[payloadKey] = struct{}{}
		lastCommitTs = commitTs
		valid = iterator.Next()
	}
	if err := iterator.Error(); err != nil {
		return nil, false, errors.WrapError(errors.ErrSpillFileOp, err, "iterate spill event index")
	}
	if len(entries) == 0 {
		return nil, false, nil
	}
	hasMore := false
	if valid {
		nextCommitTs, err := decodeEventIndexCommitTs(iterator.Key())
		if err != nil {
			return nil, false, err
		}
		hasMore = nextCommitTs <= g.resolvedTs
	}
	if g.outOfOrder {
		log.Warn("DML events were appended out of order; restore from ordered spill index",
			zap.Int32("partition", g.Partition),
			zap.Int64("tableID", g.tableID),
			zap.Uint64("resolveTs", g.resolvedTs),
			zap.Int("resolved", len(entries)))
		g.outOfOrder = false
	}
	batch := &ResolveBatch{
		Messages: make([]*codeccommon.DMLMessage, 0, len(entries)),
		group:    g,
		entries:  entries,
	}
	loaded := make(map[payloadCacheKey]*payloadCacheEntry)
	for _, message := range entries {
		payloadKey := payloadCacheKey{segmentID: message.location.segmentID, offset: message.location.handle.Offset}
		payload := loaded[payloadKey]
		readBytes := int64(0)
		if payload == nil {
			payload, readBytes, err = g.store.loadAndPinPayload(message)
			if err != nil {
				g.store.unpinPayloads(batch.payloads)
				return nil, false, err
			}
			loaded[payloadKey] = payload
			batch.payloads = append(batch.payloads, payload)
		}
		if message.dmlIndex >= uint64(len(payload.messages)) {
			g.store.unpinPayloads(batch.payloads)
			return nil, false, errors.ErrSpillFileOp.FastGenByArgs("DML spill message index is out of range")
		}
		restored := payload.messages[message.dmlIndex]
		if restored == nil {
			g.store.unpinPayloads(batch.payloads)
			return nil, false, errors.ErrSpillFileOp.FastGenByArgs("DML spill message is nil")
		}
		if g.postRestore != nil {
			restored = g.postRestore(restored, message.sourcePosition)
		}
		if restored == nil {
			g.store.unpinPayloads(batch.payloads)
			return nil, false, errors.ErrSpillFileOp.FastGenByArgs("post-restore returned nil DML message")
		}
		batch.Messages = append(batch.Messages, restored)
		batch.ResolvedBytes += readBytes
	}
	g.store.stats.IndexReadCount += int64(len(entries))
	g.batchPending = true
	return batch, hasMore, nil
}

func boundedMessageCapacity(limit ResolveLimit) int {
	if limit.MaxMessages <= 0 || limit.MaxMessages > defaultResolveBatchMessages {
		return defaultResolveBatchMessages
	}
	return limit.MaxMessages
}

func exceedsResolveLimit(count int, bytes, additionalBytes int64, limit ResolveLimit) bool {
	maxBytes := limit.MaxBytes
	if maxBytes <= 0 {
		maxBytes = math.MaxInt64
	}
	maxMessages := limit.MaxMessages
	if maxMessages <= 0 {
		maxMessages = int(^uint(0) >> 1)
	}
	return count >= maxMessages || bytes+additionalBytes > maxBytes
}

func (s *SpillStore) loadAndPinPayload(message spilledMessage) (*payloadCacheEntry, int64, error) {
	key := payloadCacheKey{segmentID: message.location.segmentID, offset: message.location.handle.Offset}
	if cached := s.cache[key]; cached != nil {
		cached.pins++
		s.cacheLRU.MoveToFront(cached.element)
		return cached, 0, nil
	}
	segment := s.segments[message.location.segmentID]
	if segment == nil || segment.file == nil {
		return nil, 0, errors.ErrSpillFileOp.FastGenByArgs("spill payload segment is missing")
	}
	restorer := s.restorers[message.restorerID]
	if restorer == nil || restorer.decode == nil {
		return nil, 0, errors.ErrSpillFileOp.FastGenByArgs("DML spill restorer is missing")
	}
	data, err := s.readRecord(segment.file, message.location.handle)
	if err != nil {
		return nil, 0, err
	}
	messages, err := restorer.decode(data)
	if err != nil {
		return nil, 0, err
	}
	entry := &payloadCacheEntry{
		key:      key,
		messages: messages,
		bytes: int64(message.location.handle.Length) +
			int64(len(messages))*s.config.messageMetadataBytes,
		pins: 1,
	}
	entry.element = s.cacheLRU.PushFront(entry)
	s.cache[key] = entry
	s.cacheBytes += entry.bytes
	s.addPending(entry.bytes)
	s.stats.PayloadReadBytes += int64(message.location.handle.Length)
	s.stats.PayloadReadCount++
	s.stats.PayloadDecodeCount++
	s.trimPayloadCache()
	return entry, int64(message.location.handle.Length), nil
}

// ResolveInto appends all messages with CommitTs <= resolve into dst in commit-ts order.
func (g *EventsGroup) ResolveInto(
	resolve uint64, dst []*codeccommon.DMLMessage,
) ([]*codeccommon.DMLMessage, error) {
	dst, _, _, err := g.ResolveIntoBatch(resolve, dst, ResolveLimit{})
	return dst, err
}

// ResolveIntoBatch appends one bounded batch of messages with CommitTs <= resolve into dst in commit-ts order.
// A single commit-ts group can exceed the limits so that one transaction is never split.
func (g *EventsGroup) ResolveIntoBatch(
	resolve uint64, dst []*codeccommon.DMLMessage, limit ResolveLimit,
) ([]*codeccommon.DMLMessage, bool, int64, error) {
	batch, hasMore, err := g.PrepareResolve(resolve, limit)
	if err != nil || batch == nil {
		return dst, hasMore, 0, err
	}
	dst = append(dst, batch.Messages...)
	resolvedBytes := batch.ResolvedBytes
	if err := batch.Ack(); err != nil {
		return dst, hasMore, resolvedBytes, err
	}
	return dst, hasMore, resolvedBytes, nil
}

func (g *EventsGroup) ack(batch *ResolveBatch) error {
	if len(batch.entries) == 0 {
		g.batchPending = false
		g.store.unpinPayloads(batch.payloads)
		return nil
	}
	if err := g.store.checkDiskUsage(eventIndexKeyBytes+eventIndexValueBytes, true); err != nil {
		return err
	}
	start := batch.entries[0].key
	end := append(append([]byte(nil), batch.entries[len(batch.entries)-1].key...), 0)
	if err := g.store.index.DeleteRange(start, end, pebble.NoSync); err != nil {
		return errors.WrapError(errors.ErrSpillFileOp, err, "delete applied spill event range")
	}
	for _, message := range batch.entries {
		g.releaseEvent(message.location.segmentID, message.restorerID, 1)
	}
	g.pendingCount -= int64(len(batch.entries))
	g.store.releasePending(int64(len(batch.entries)) * g.store.config.messageMetadataBytes)
	g.store.stats.AppliedEventCount += int64(len(batch.entries))
	lastCommitTs := batch.entries[len(batch.entries)-1].commitTs
	if lastCommitTs > g.appliedTs {
		g.appliedTs = lastCommitTs
	}
	g.batchPending = false
	g.store.unpinPayloads(batch.payloads)
	if g.pendingCount == 0 && g.ownsStore {
		return g.store.Cleanup()
	}
	return nil
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() ([]*codeccommon.DMLMessage, error) {
	return g.ResolveInto(math.MaxUint64, nil)
}

// Cleanup removes pending spill records when the consumer is stopping.
func (g *EventsGroup) Cleanup() error {
	if g.batchPending {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot clean events group with pending resolve batch")
	}
	if g.pendingCount != 0 && g.store.index != nil {
		if err := g.store.flushEventIndex(); err != nil {
			return err
		}
		lower, upper := eventIndexBounds(g.id)
		if err := g.store.index.DeleteRange(lower, upper, pebble.NoSync); err != nil {
			return errors.WrapError(errors.ErrSpillFileOp, err, "delete spill event group")
		}
		for segmentID, count := range g.segmentRefs {
			g.releaseSegmentRefs(segmentID, count)
		}
		for restorerID, count := range g.restorerRefs {
			g.releaseRestorerRefs(restorerID, count)
		}
		g.store.releasePending(g.pendingCount * g.store.config.messageMetadataBytes)
	}
	g.pendingCount = 0
	clear(g.segmentRefs)
	clear(g.restorerRefs)
	g.batchPending = false
	if g.ownsStore {
		return g.store.Cleanup()
	}
	return nil
}

// DMLMessagesToEvents materializes messages and merges compatible adjacent
// messages before they are handed to the downstream sink.
func DMLMessagesToEvents(messages []*codeccommon.DMLMessage) []*commonEvent.DMLEvent {
	events := make([]*commonEvent.DMLEvent, 0, len(messages))
	for _, message := range messages {
		events = appendOrMergeDMLEvent(events, message.ToDMLEvent())
	}
	return events
}

func appendOrMergeDMLEvent(events []*commonEvent.DMLEvent, row *commonEvent.DMLEvent) []*commonEvent.DMLEvent {
	if len(events) == 0 || !sameDMLTransaction(events[len(events)-1], row) {
		return append(events, row)
	}

	last := events[len(events)-1]
	lastRowTypeCount := len(last.RowTypes)
	rowRowTypeCount := len(row.RowTypes)
	last.Rows.Append(row.Rows, 0, row.Rows.NumRows())
	last.RowTypes = append(last.RowTypes, row.RowTypes...)
	last.RowKeys = appendOptionalDMLValues(last.RowKeys, row.RowKeys, lastRowTypeCount, rowRowTypeCount)
	last.Checksum = appendOptionalDMLValues(last.Checksum, row.Checksum, lastRowTypeCount, rowRowTypeCount)
	last.Length += row.Length
	last.ApproximateSize += row.ApproximateSize
	last.PostTxnEnqueued = append(last.PostTxnEnqueued, row.PostTxnEnqueued...)
	last.PostTxnFlushed = append(last.PostTxnFlushed, row.PostTxnFlushed...)
	return events
}

func sameDMLTransaction(last, row *commonEvent.DMLEvent) bool {
	return last != nil && row != nil && last.CommitTs == row.CommitTs
}

func appendOptionalDMLValues[T any](last, row []T, lastRowTypeCount, rowRowTypeCount int) []T {
	if len(last) == 0 && len(row) != 0 {
		last = make([]T, lastRowTypeCount)
	} else if len(last) != 0 && len(row) == 0 {
		row = make([]T, rowRowTypeCount)
	}
	return append(last, row...)
}

// NewDMLMessageData preserves one original codec input. The decoder is used
// only during ResolveInto, after the DDL or watermark barrier selects schema.
func NewDMLMessageData(decoder codeccommon.Decoder, key, value []byte) *codeccommon.DMLMessageData {
	return NewDMLMessageDataWithDecoderFactory(key, value,
		func([]byte, []byte) (codeccommon.Decoder, error) { return decoder, nil })
}

// NewDMLMessageDataWithDecoderFactory is for decoders such as CSV whose
// input is supplied during construction rather than through AddKeyValue.
func NewDMLMessageDataWithDecoderFactory(
	key, value []byte,
	decoderFactory func([]byte, []byte) (codeccommon.Decoder, error),
) *codeccommon.DMLMessageData {
	return codeccommon.NewDMLMessageData(key, value,
		func(data []byte) ([]*codeccommon.DMLMessage, error) {
			key, value, err := unmarshalDMLMessageData(data)
			if err != nil {
				return nil, err
			}
			decoder, err := decoderFactory(key, value)
			if err != nil {
				return nil, errors.WrapError(errors.ErrSpillFileOp, err, "create DML spill decoder")
			}
			return restoreDMLMessages(decoder, key, value)
		})
}

func restoreDMLMessages(
	decoder codeccommon.Decoder, key, value []byte,
) ([]*codeccommon.DMLMessage, error) {
	decoder.AddKeyValue(key, value)
	messages := make([]*codeccommon.DMLMessage, 0, 1)
	for {
		messageType, hasNext := decoder.HasNext()
		if !hasNext {
			if len(messages) == 0 {
				return nil, errors.ErrSpillFileOp.FastGenByArgs("DML spill payload has no message")
			}
			return messages, nil
		}
		if messageType != codeccommon.MessageTypeRow {
			return nil, errors.ErrSpillFileOp.FastGenByArgs("DML spill payload contains a non-DML message")
		}
		message := decoder.NextDMLMessage()
		if message == nil {
			return nil, errors.ErrSpillFileOp.FastGenByArgs("DML spill payload cannot be restored")
		}
		messages = append(messages, message)
	}
}

func unmarshalDMLMessageData(data []byte) ([]byte, []byte, error) {
	key, data, err := readSpillBytes(data)
	if err != nil {
		return nil, nil, err
	}
	value, data, err := readSpillBytes(data)
	if err != nil {
		return nil, nil, err
	}
	if len(data) != 0 {
		return nil, nil, errors.ErrSpillFileOp.FastGenByArgs("unexpected trailing DML spill data")
	}
	return key, value, nil
}

func readSpillBytes(data []byte) ([]byte, []byte, error) {
	length, data, err := readSpillUint64(data)
	if err != nil {
		return nil, nil, err
	}
	if length > uint64(len(data)) {
		return nil, nil, errors.ErrSpillFileOp.FastGenByArgs("invalid DML spill payload length")
	}
	return data[:length], data[length:], nil
}

func readSpillUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, errors.ErrSpillFileOp.FastGenByArgs("truncated DML spill data")
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], nil
}
