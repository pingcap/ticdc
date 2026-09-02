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
	"encoding/binary"
	"math"
	"os"
	"sort"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/spill"
	"go.uber.org/zap"
)

const eventsGroupSpillPattern = "ticdc-events-group-*.spill"

const (
	defaultSpillSegmentBytes    = 128 * 1024 * 1024
	defaultSpillSegmentMessages = 100000
	defaultPendingHighBytes     = 1024 * 1024 * 1024
	defaultPendingLowBytes      = 512 * 1024 * 1024
	defaultResolveBatchBytes    = 64 * 1024 * 1024
	defaultResolveBatchMessages = 10000
	defaultMessageMetadataBytes = 128
	spillRecordLengthBytes      = 8
)

type spillConfig struct {
	segmentBytes         int64
	segmentMessages      int
	pendingHighBytes     int64
	pendingLowBytes      int64
	resolveBatchBytes    int64
	resolveBatchMessages int
	messageMetadataBytes int64
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
}

// SpillStore owns spill payloads and segments across all event groups in one consumer.
// Its watermarks request draining; they are not a hard quota and never reject an append.
type SpillStore struct {
	config        spillConfig
	segments      map[*spillSegment]struct{}
	activeSegment *spillSegment
	payloads      map[uint64]*spillPayload
	pendingBytes  int64
	draining      bool

	readRecord func(*spill.RecordFile, spill.Handle) ([]byte, error)
	stats      SpillStats
}

// NewSpillStore creates a process-wide store with the default spill limits.
func NewSpillStore() *SpillStore {
	return newSpillStore(defaultSpillConfig())
}

func newSpillStore(config spillConfig) *SpillStore {
	return &SpillStore{
		config:   config,
		segments: make(map[*spillSegment]struct{}),
		payloads: make(map[uint64]*spillPayload),
		readRecord: func(file *spill.RecordFile, handle spill.Handle) ([]byte, error) {
			return file.Read(handle)
		},
	}
}

// ResolveLimit returns the configured per-batch restore limit.
func (s *SpillStore) ResolveLimit() ResolveLimit {
	return ResolveLimit{
		MaxBytes:    s.config.resolveBatchBytes,
		MaxMessages: s.config.resolveBatchMessages,
	}
}

// PendingBytes returns conservatively accounted payload, decoded cache, and metadata bytes.
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
	stats.LivePayloads = len(s.payloads)
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

type spillSegment struct {
	file            *spill.RecordFile
	bytes           int64
	payloadCount    int
	pendingPayloads int
}

type spillPayload struct {
	id           uint64
	segment      *spillSegment
	handle       spill.Handle
	decode       func([]byte) ([]*codeccommon.DMLMessage, error)
	decoded      []*codeccommon.DMLMessage
	decodedBytes int64
	loaded       bool
	refs         int
}

type spilledMessage struct {
	commitTs uint64
	payload  *spillPayload
	dmlIndex uint64
}

// EventsGroup stores change event messages.
type EventsGroup struct {
	Partition int32
	tableID   int64

	messages      []spilledMessage
	store         *SpillStore
	batchPending  bool
	outOfOrder    bool
	HighWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64, stores ...*SpillStore) *EventsGroup {
	store := NewSpillStore()
	if len(stores) != 0 && stores[0] != nil {
		store = stores[0]
	}
	return &EventsGroup{
		Partition: partition,
		tableID:   tableID,
		messages:  make([]spilledMessage, 0, 1024),
		store:     store,
	}
}

func (s *SpillStore) newSegment() error {
	file, err := spill.NewRecordFile(os.TempDir(), eventsGroupSpillPattern)
	if err != nil {
		return err
	}
	segment := &spillSegment{file: file}
	s.segments[segment] = struct{}{}
	s.activeSegment = segment
	return nil
}

func (s *SpillStore) prepareSegment(recordBytes int64) error {
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

func (s *SpillStore) acquirePayload(data *codeccommon.DMLMessageData) (*spillPayload, error) {
	if payload, ok := s.payloads[data.ID]; ok {
		return payload, nil
	}
	recordBytes := spillMessageDataSize(data.Key, data.Value)
	if err := s.prepareSegment(recordBytes); err != nil {
		return nil, err
	}
	segment := s.activeSegment
	handle, err := appendMessageData(segment.file, data.Key, data.Value)
	if err != nil {
		return nil, err
	}
	payload := &spillPayload{
		id:      data.ID,
		segment: segment,
		handle:  handle,
		decode:  data.Decode,
	}
	s.payloads[data.ID] = payload
	segment.bytes += recordBytes
	segment.payloadCount++
	segment.pendingPayloads++
	s.addPending(recordBytes)
	s.stats.PayloadWriteBytes += int64(handle.Length)
	s.stats.PayloadWriteCount++
	s.sealFullSegment()
	return payload, nil
}

func (s *SpillStore) retainPayload(payload *spillPayload) {
	payload.refs++
	s.addPending(s.config.messageMetadataBytes)
}

func (s *SpillStore) loadPayload(payload *spillPayload) (int64, error) {
	if payload.loaded {
		return 0, nil
	}
	if payload.segment == nil || payload.segment.file == nil {
		return 0, errors.ErrSpillFileOp.FastGenByArgs("spill payload segment is missing")
	}
	data, err := s.readRecord(payload.segment.file, payload.handle)
	if err != nil {
		return 0, err
	}
	messages, err := payload.decode(data)
	if err != nil {
		return 0, err
	}
	payload.decoded = messages
	payload.loaded = true
	payload.decodedBytes = int64(payload.handle.Length)
	s.addPending(payload.decodedBytes)
	s.stats.PayloadReadBytes += int64(payload.handle.Length)
	s.stats.PayloadReadCount++
	s.stats.PayloadDecodeCount++
	return int64(payload.handle.Length), nil
}

func (s *SpillStore) messageAt(payload *spillPayload, index uint64) (*codeccommon.DMLMessage, int64, error) {
	readBytes, err := s.loadPayload(payload)
	if err != nil {
		return nil, 0, err
	}
	if index >= uint64(len(payload.decoded)) {
		return nil, 0, errors.ErrSpillFileOp.FastGenByArgs("DML spill message index is out of range")
	}
	message := payload.decoded[index]
	if message == nil {
		return nil, 0, errors.ErrSpillFileOp.FastGenByArgs("DML spill message was already released")
	}
	return message, readBytes, nil
}

func (s *SpillStore) releasePayloadRef(payload *spillPayload, index uint64) {
	if payload.loaded && index < uint64(len(payload.decoded)) {
		payload.decoded[index] = nil
	}
	payload.refs--
	s.releasePending(s.config.messageMetadataBytes)
	if payload.refs != 0 {
		return
	}
	if payload.loaded {
		s.releasePending(payload.decodedBytes)
	}
	payload.decoded = nil
	payload.decode = nil
	delete(s.payloads, payload.id)

	segment := payload.segment
	segment.pendingPayloads--
	if segment.pendingPayloads == 0 {
		s.cleanupSegment(segment)
	}
}

func (s *SpillStore) cleanupSegment(segment *spillSegment) {
	if s.activeSegment == segment {
		s.activeSegment = nil
	}
	if err := segment.file.Cleanup(); err != nil {
		log.Warn("cleanup spill segment failed", zap.String("path", segment.file.Path()), zap.Error(err))
		return
	}
	s.releasePending(segment.bytes)
	delete(s.segments, segment)
}

// Cleanup removes all remaining payload and segment state when a consumer stops.
func (s *SpillStore) Cleanup() error {
	for _, payload := range s.payloads {
		s.releasePending(int64(payload.refs) * s.config.messageMetadataBytes)
		if payload.loaded {
			s.releasePending(payload.decodedBytes)
		}
		payload.decoded = nil
		payload.decode = nil
	}
	clear(s.payloads)
	s.activeSegment = nil
	var cleanupErr error
	for segment := range s.segments {
		if err := segment.file.Cleanup(); err != nil {
			if cleanupErr == nil {
				cleanupErr = err
			}
			continue
		}
		s.releasePending(segment.bytes)
		delete(s.segments, segment)
	}
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
	if messageData == nil || messageData.Decode == nil {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot spill DML message without decode function")
	}
	commitTs := message.GetCommitTs()
	payload, err := g.store.acquirePayload(messageData)
	if err != nil {
		return err
	}
	if len(g.messages) > 0 && commitTs < g.messages[len(g.messages)-1].commitTs {
		g.outOfOrder = true
	}
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}
	g.messages = append(g.messages, spilledMessage{
		commitTs: commitTs,
		payload:  payload,
		dmlIndex: dmlIndex,
	})
	g.store.retainPayload(payload)
	return nil
}

// ResolveBatch owns a prepared group prefix until the downstream confirms it.
type ResolveBatch struct {
	Messages      []*codeccommon.DMLMessage
	ResolvedBytes int64
	group         *EventsGroup
	count         int
	acked         bool
}

// Ack releases a prepared group prefix after downstream flush callbacks complete.
func (b *ResolveBatch) Ack() {
	if b == nil || b.acked {
		return
	}
	b.group.ack(b.count)
	b.acked = true
}

// PrepareResolve restores one bounded group prefix without removing it.
func (g *EventsGroup) PrepareResolve(
	resolve uint64, limit ResolveLimit,
) (*ResolveBatch, bool, error) {
	if g.batchPending {
		return nil, false, errors.ErrSpillFileOp.FastGenByArgs("events group already has a pending resolve batch")
	}
	if len(g.messages) == 0 {
		return nil, false, nil
	}
	if g.outOfOrder {
		sort.SliceStable(g.messages, func(i, j int) bool {
			return g.messages[i].commitTs < g.messages[j].commitTs
		})
	}
	resolvedCount := sort.Search(len(g.messages), func(i int) bool {
		return g.messages[i].commitTs > resolve
	})
	if g.outOfOrder {
		log.Warn("DML events are out of order before flush, sort them",
			zap.Int32("partition", g.Partition),
			zap.Int64("tableID", g.tableID),
			zap.Uint64("resolveTs", resolve),
			zap.Int("resolved", resolvedCount))
		g.outOfOrder = false
	}
	if resolvedCount == 0 {
		return nil, false, nil
	}

	batchCount := boundedResolvedBatch(g.messages[:resolvedCount], limit)
	batch := &ResolveBatch{
		Messages: make([]*codeccommon.DMLMessage, 0, batchCount),
		group:    g,
		count:    batchCount,
	}
	for _, message := range g.messages[:batchCount] {
		restored, readBytes, err := g.store.messageAt(message.payload, message.dmlIndex)
		if err != nil {
			return nil, false, err
		}
		batch.Messages = append(batch.Messages, restored)
		batch.ResolvedBytes += readBytes
	}
	g.batchPending = true
	return batch, batchCount < resolvedCount, nil
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
	batch.Ack()
	return dst, hasMore, resolvedBytes, nil
}

func boundedResolvedBatch(messages []spilledMessage, limit ResolveLimit) int {
	if len(messages) == 0 {
		return 0
	}
	maxBytes := limit.MaxBytes
	if maxBytes <= 0 {
		maxBytes = math.MaxInt64
	}
	maxMessages := limit.MaxMessages
	if maxMessages <= 0 {
		maxMessages = int(^uint(0) >> 1)
	}

	seenPayloads := make(map[*spillPayload]struct{})
	var bytes int64
	for i, message := range messages {
		additionalBytes := int64(0)
		if !message.payload.loaded {
			if _, ok := seenPayloads[message.payload]; !ok {
				additionalBytes = int64(message.payload.handle.Length)
			}
		}
		exceedsLimit := i > 0 && (i >= maxMessages || bytes+additionalBytes > maxBytes)
		if exceedsLimit && message.commitTs != messages[i-1].commitTs {
			return i
		}
		bytes += additionalBytes
		seenPayloads[message.payload] = struct{}{}
	}
	return len(messages)
}

func (g *EventsGroup) ack(count int) {
	for i := range count {
		message := &g.messages[i]
		g.store.releasePayloadRef(message.payload, message.dmlIndex)
	}
	remainingCount := len(g.messages) - count
	copy(g.messages, g.messages[count:])
	clear(g.messages[remainingCount:])
	if remainingCount == 0 {
		g.messages = nil
	} else if cap(g.messages) > 2*remainingCount && cap(g.messages) > 1024 {
		remaining := make([]spilledMessage, remainingCount)
		copy(remaining, g.messages[:remainingCount])
		g.messages = remaining
	} else {
		g.messages = g.messages[:remainingCount]
	}
	g.batchPending = false
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() ([]*codeccommon.DMLMessage, error) {
	return g.ResolveInto(math.MaxUint64, nil)
}

// Cleanup removes pending spill records when the consumer is stopping.
func (g *EventsGroup) Cleanup() error {
	for i := range g.messages {
		message := &g.messages[i]
		g.store.releasePayloadRef(message.payload, message.dmlIndex)
	}
	clear(g.messages)
	g.messages = nil
	g.batchPending = false
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
