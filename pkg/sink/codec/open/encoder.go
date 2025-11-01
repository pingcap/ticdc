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

package open

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka/claimcheck"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	batchVersion1 uint64 = 1
)

var (
	lock             sync.RWMutex
	columnFlagsCache = make(map[int64]map[string]uint64, 32)
)

// batchEncoder for open protocol will batch multiple row changed events into a single message.
// One message can contain at most MaxBatchSize events, and the total size of the message cannot exceed MaxMessageBytes.
type batchEncoder struct {
	messages []*common.Message
	// buff the callback of the latest message
	callbackBuff []func()

	claimCheck *claimcheck.ClaimCheck

	config *common.Config
}

// NewBatchEncoder creates a new batchEncoder.
func NewBatchEncoder(ctx context.Context, config *common.Config) (common.EventEncoder, error) {
	claimCheck, err := claimcheck.New(ctx, config.LargeMessageHandle, config.ChangefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	lock.Lock()
	clear(columnFlagsCache)
	lock.Unlock()
	return &batchEncoder{
		config:     config,
		claimCheck: claimCheck,
	}, nil
}

func (d *batchEncoder) Clean() {
	if d.claimCheck != nil {
		d.claimCheck.CleanMetrics()
	}
}

func (d *batchEncoder) fetchColumnFlags(e *commonEvent.RowEvent) map[string]uint64 {
	lock.RLock()
	result, ok := columnFlagsCache[e.GetTableID()]
	lock.RUnlock()
	if !ok {
		result = initColumnFlags(e.TableInfo)
		lock.Lock()
		columnFlagsCache[e.GetTableID()] = result
		lock.Unlock()
	}
	return result
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *batchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	_ string,
	e *commonEvent.RowEvent,
) error {
	columnFlags := d.fetchColumnFlags(e)
	key, value, length, err := encodeRowChangedEvent(e, columnFlags, d.config, false, "")
	if err != nil {
		return errors.Trace(err)
	}

	if length > d.config.MaxMessageBytes {
		// message len is larger than max-message-bytes
		if d.config.LargeMessageHandle.Disabled() {
			log.Warn("Single message is too large for open-protocol",
				zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", e.TableInfo.TableName),
				zap.Any("key", key))
			return errors.ErrMessageTooLarge.GenWithStackByArgs()
		}

		if d.config.LargeMessageHandle.EnableClaimCheck() {
			// send the large message to the external storage first, then
			// create a new message contains the reference of the large message.
			claimCheckFileName := claimcheck.NewFileName()
			keyOutput, valueOutput := enhancedKeyValue(key, value)
			err = d.claimCheck.WriteMessage(ctx, keyOutput, valueOutput, claimCheckFileName)
			if err != nil {
				return errors.Trace(err)
			}

			key, value, length, err = encodeRowChangedEvent(e, columnFlags, d.config, true, d.claimCheck.FileNameWithPrefix(claimCheckFileName))
			if err != nil {
				return errors.Trace(err)
			}

			if length > d.config.MaxMessageBytes {
				log.Warn("Single message is too large for open-protocol, "+
					"when create the claim-check location message",
					zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
					zap.Int("length", length),
					zap.Any("key", key))
				return errors.ErrMessageTooLarge.GenWithStackByArgs()
			}
		}

		if d.config.LargeMessageHandle.HandleKeyOnly() {
			// it must that `LargeMessageHandle == LargeMessageHandleOnlyHandleKeyColumns` here.
			key, value, length, err = encodeRowChangedEvent(e, columnFlags, d.config, true, "")
			if err != nil {
				return errors.Trace(err)
			}

			if length > d.config.MaxMessageBytes {
				log.Warn("Single message is too large for open-protocol even only encode handle key columns",
					zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
					zap.Int("length", length),
					zap.Any("table", e.TableInfo.TableName),
					zap.Any("key", key))
				return errors.ErrMessageTooLarge.GenWithStackByArgs()
			}
		}
	}

	d.pushMessage(key, value, e.Callback)
	return nil
}

// Build implements the RowEventEncoder interface
func (d *batchEncoder) Build() (messages []*common.Message) {
	if len(d.messages) == 0 {
		return nil
	}
	d.finalizeCallback()
	result := d.messages
	d.messages = nil
	return result
}

func (d *batchEncoder) pushMessage(key, value []byte, callback func()) {
	length := len(key) + len(value) + 16

	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))

	if len(d.messages) == 0 || d.messages[len(d.messages)-1].Length()+length > d.config.MaxMessageBytes || d.messages[len(d.messages)-1].GetRowsCount() >= d.config.MaxBatchSize {
		d.finalizeCallback()
		// create a new message
		versionHead := make([]byte, 8)
		binary.BigEndian.PutUint64(versionHead, batchVersion1)

		message := common.NewMsg(versionHead, valueLenByte[:])
		message.Key = append(message.Key, keyLenByte[:]...)
		message.Key = append(message.Key, key...)
		message.Value = append(message.Value, value...)
		message.IncRowsCount()
		d.callbackBuff = append(d.callbackBuff, callback)
		d.messages = append(d.messages, message)
		return
	}

	// append to the latest message
	latestMessage := d.messages[len(d.messages)-1]
	latestMessage.Key = append(latestMessage.Key, keyLenByte[:]...)
	latestMessage.Key = append(latestMessage.Key, key...)
	latestMessage.Value = append(latestMessage.Value, valueLenByte[:]...)
	latestMessage.Value = append(latestMessage.Value, value...)
	d.callbackBuff = append(d.callbackBuff, callback)
	latestMessage.IncRowsCount()
}

func (d *batchEncoder) finalizeCallback() {
	if len(d.callbackBuff) == 0 || len(d.messages) == 0 {
		return
	}

	lastMsg := d.messages[len(d.messages)-1]
	callbacks := d.callbackBuff
	lastMsg.Callback = func() {
		for _, cb := range callbacks {
			cb()
		}
	}
	d.callbackBuff = make([]func(), 0)
}

func enhancedKeyValue(key, value []byte) ([]byte, []byte) {
	var (
		keyLenByte   [8]byte
		valueLenByte [8]byte
		versionHead  [8]byte
	)
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], uint64(len(value)))
	binary.BigEndian.PutUint64(versionHead[:], batchVersion1)

	keyOutput := versionHead[:]
	keyOutput = append(keyOutput, keyLenByte[:]...)
	keyOutput = append(keyOutput, key...)
	valueOutput := valueLenByte[:]
	valueOutput = append(valueOutput, value...)
	return keyOutput, valueOutput
}

func (d *batchEncoder) EncodeDDLEvent(e *commonEvent.DDLEvent) (*common.Message, error) {
	lock.Lock()
	delete(columnFlagsCache, e.TableInfo.TableName.TableID)
	defer lock.Unlock()

	key, value, err := encodeDDLEvent(e, d.config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return common.NewMsg(key, value), nil
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *batchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	keyBuf := &bytes.Buffer{}
	keyWriter := util.BorrowJSONWriter(keyBuf)

	keyWriter.WriteObject(func() {
		keyWriter.WriteUint64Field("ts", ts)
		keyWriter.WriteIntField("t", int(common.MessageTypeResolved))
	})

	util.ReturnJSONWriter(keyWriter)

	key := keyBuf.Bytes()

	var keyLenByte [8]byte
	var valueLenByte [8]byte
	var versionByte [8]byte
	binary.BigEndian.PutUint64(keyLenByte[:], uint64(len(key)))
	binary.BigEndian.PutUint64(valueLenByte[:], 0)
	binary.BigEndian.PutUint64(versionByte[:], batchVersion1)

	keyOutput := new(bytes.Buffer)

	keyOutput.Write(versionByte[:])
	keyOutput.Write(keyLenByte[:])
	keyOutput.Write(key)

	valueOutput := new(bytes.Buffer)
	valueOutput.Write(valueLenByte[:])

	key = keyOutput.Bytes()
	value := valueOutput.Bytes()
	return common.NewMsg(key, value), nil
}
