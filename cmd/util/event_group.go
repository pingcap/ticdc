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
	"sync"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/spill"
	"go.uber.org/zap"
)

const eventsGroupSpillPattern = "ticdc-events-group-*.spill"

type spilledMessage struct {
	commitTs uint64
	handle   spill.Handle
	dmlIndex uint64
	restore  func([]byte, uint64) (*codeccommon.DMLMessage, error)
}

// EventsGroup stores change event messages.
type EventsGroup struct {
	Partition int32
	tableID   int64

	messages      []spilledMessage
	spillFile     *spill.RecordFile
	spillHandles  map[uint64]spill.Handle
	outOfOrder    bool
	HighWatermark uint64
}

// NewEventsGroup will create new event group.
func NewEventsGroup(partition int32, tableID int64) *EventsGroup {
	return &EventsGroup{
		Partition: partition,
		tableID:   tableID,
		messages:  make([]spilledMessage, 0, 1024),
	}
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
	if messageData == nil || messageData.Restore == nil {
		return errors.ErrSpillFileOp.FastGenByArgs("cannot spill DML message without restore function")
	}
	commitTs := message.GetCommitTs()

	if g.spillFile == nil {
		var err error
		g.spillFile, err = spill.NewRecordFile(os.TempDir(), eventsGroupSpillPattern)
		if err != nil {
			return err
		}
	}
	if g.spillHandles == nil {
		g.spillHandles = make(map[uint64]spill.Handle)
	}
	handle, ok := g.spillHandles[messageData.ID]
	if !ok {
		data := marshalDMLMessageData(messageData.Key, messageData.Value)
		if len(data) == 0 {
			// A lazy message may not need an input payload (for example, a Simple
			// decoder message released from its table-info cache). RecordFile rejects
			// empty records, so retain a marker while keeping the event lazy.
			data = []byte{0}
		}
		var err error
		handle, err = g.spillFile.Append(data)
		if err != nil {
			return err
		}
		g.spillHandles[messageData.ID] = handle
	}
	if len(g.messages) > 0 && commitTs < g.messages[len(g.messages)-1].commitTs {
		g.outOfOrder = true
	}
	if commitTs > g.HighWatermark {
		g.HighWatermark = commitTs
	}
	g.messages = append(g.messages, spilledMessage{
		commitTs: commitTs,
		handle:   handle,
		dmlIndex: dmlIndex,
		restore:  messageData.Restore,
	})
	return nil
}

// ResolveInto appends all messages with CommitTs <= resolve into dst in commit-ts order and removes
// them from the group. Resolved messages are restored from the spill file only when downstream needs
// them, keeping the buffered group out of heap memory.
func (g *EventsGroup) ResolveInto(resolve uint64, dst []*codeccommon.DMLMessage) ([]*codeccommon.DMLMessage, error) {
	if len(g.messages) == 0 {
		if g.spillFile != nil {
			if err := g.spillFile.Cleanup(); err != nil {
				return dst, err
			}
			g.spillFile = nil
			clear(g.spillHandles)
		}
		return dst, nil
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
		return dst, nil
	}
	if g.spillFile == nil {
		return dst, errors.ErrSpillFileOp.FastGenByArgs("events group spill file is missing")
	}

	for _, message := range g.messages[:resolvedCount] {
		data, err := g.spillFile.Read(message.handle)
		if err != nil {
			return dst, err
		}
		restored, err := message.restore(data, message.dmlIndex)
		if err != nil {
			return dst, err
		}
		dst = append(dst, restored)
	}
	remainingCount := len(g.messages) - resolvedCount
	copy(g.messages, g.messages[resolvedCount:])
	clear(g.messages[remainingCount:])
	g.messages = g.messages[:remainingCount]
	if len(g.messages) == 0 {
		if err := g.spillFile.Cleanup(); err != nil {
			return dst, err
		}
		g.spillFile = nil
		clear(g.spillHandles)
	}
	if len(g.messages) != 0 {
		firstCommitTs := g.messages[0].commitTs
		log.Debug("not all events resolved",
			zap.Int32("partition", g.Partition), zap.Int64("tableID", g.tableID),
			zap.Int("resolved", resolvedCount), zap.Int("remained", len(g.messages)),
			zap.Uint64("resolveTs", resolve), zap.Uint64("firstCommitTs", firstCommitTs))
	}
	return dst, nil
}

// GetAllMessages gets all messages.
func (g *EventsGroup) GetAllMessages() ([]*codeccommon.DMLMessage, error) {
	return g.ResolveInto(math.MaxUint64, nil)
}

// Cleanup removes pending spill records when the consumer is stopping.
func (g *EventsGroup) Cleanup() error {
	if g.spillFile == nil {
		return nil
	}
	err := g.spillFile.Cleanup()
	if err != nil {
		return err
	}
	g.spillFile = nil
	clear(g.spillHandles)
	clear(g.messages)
	g.messages = g.messages[:0]
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
	var (
		once     sync.Once
		messages []*codeccommon.DMLMessage
		err      error
	)
	return codeccommon.NewDMLMessageData(key, value,
		func(data []byte, dmlIndex uint64) (*codeccommon.DMLMessage, error) {
			once.Do(func() {
				key, value, unmarshalErr := unmarshalDMLMessageData(data)
				if unmarshalErr != nil {
					err = unmarshalErr
					return
				}
				decoder, decoderErr := decoderFactory(key, value)
				if decoderErr != nil {
					err = errors.WrapError(errors.ErrSpillFileOp, decoderErr, "create DML spill decoder")
					return
				}
				messages, err = restoreDMLMessages(decoder, key, value)
			})
			if err != nil {
				return nil, err
			}
			if dmlIndex >= uint64(len(messages)) {
				return nil, errors.ErrSpillFileOp.FastGenByArgs("DML spill message index is out of range")
			}
			return messages[dmlIndex], nil
		})
}

func marshalDMLMessageData(key, value []byte) []byte {
	if len(key) == 0 && len(value) == 0 {
		return nil
	}
	data := make([]byte, 0, 2*8+len(key)+len(value))
	data = appendSpillBytes(data, key)
	return appendSpillBytes(data, value)
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

func appendSpillBytes(data, value []byte) []byte {
	data = appendSpillUint64(data, uint64(len(value)))
	return append(data, value...)
}

func appendSpillUint64(data []byte, value uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return append(data, buf[:]...)
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
