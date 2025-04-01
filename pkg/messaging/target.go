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

package messaging

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	. "github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/conn"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type MessageTarget interface {
	Epoch() uint64
}

const (
	reconnectInterval = 2 * time.Second
	msgTypeEvent      = "event"
	msgTypeCommand    = "command"
)

// remoteMessageTarget implements the SendMessageChannel interface.
// TODO: Reduce the goroutine number it spawns.
// Currently it spawns 2 goroutines for each remote target, and 2 goroutines for each local target,
// and 1 goroutine to handle grpc stream error.
type remoteMessageTarget struct {
	messageCenterID    node.ID
	messageCenterEpoch uint64

	targetEpoch atomic.Value
	targetId    node.ID
	targetAddr  string
	security    *security.Credential

	// senderMu is used to protect the eventSender and commandSender.
	// It is used to ensure that there is only one eventStream and commandStream for the target.
	senderMu      sync.Mutex
	eventSender   *sendStreamWrapper
	commandSender *sendStreamWrapper

	// For receiving events and commands
	client struct {
		sync.RWMutex
		c                 *grpc.ClientConn
		eventRecvStream   grpcReceiver
		commandRecvStream grpcReceiver
	}

	// We push the events and commands to remote send streams.
	// The send streams are created when the target is added to the message center.
	// These channels are used to cache the messages before sending.
	sendEventCh chan *proto.Message
	sendCmdCh   chan *proto.Message

	// We pull the events and commands from remote receive streams,
	// and push to the message center.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	wg *sync.WaitGroup
	// ctx is used to create the grpc stream.
	ctx context.Context
	// cancel is used to stop the grpc stream, and the goroutine spawned by remoteMessageTarget.
	cancel context.CancelFunc
	// errCh is used to gather the error from the goroutine spawned by remoteMessageTarget.
	errCh chan AppError

	sendEventCounter           prometheus.Counter
	dropEventCounter           prometheus.Counter
	recvEventCounter           prometheus.Counter
	congestedEventErrorCounter prometheus.Counter

	sendCmdCounter           prometheus.Counter
	dropCmdCounter           prometheus.Counter
	recvCmdCounter           prometheus.Counter
	congestedCmdErrorCounter prometheus.Counter

	receivedFailedErrorCounter     prometheus.Counter
	connectionNotfoundErrorCounter prometheus.Counter
	connectionFailedErrorCounter   prometheus.Counter
}

func (s *remoteMessageTarget) isReadyToSend() bool {
	return s.eventSender.ready.Load() && s.commandSender.ready.Load()
}

func (s *remoteMessageTarget) Epoch() uint64 {
	return s.targetEpoch.Load().(uint64)
}

func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if !s.eventSender.ready.Load() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has not been initialized, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has been closed, target: %s, addr: %s", s.targetId, s.targetAddr)}
	case s.sendEventCh <- s.newMessage(msg...):
		s.sendEventCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedEventErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: fmt.Sprintf("Send event message is congested, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
}

func (s *remoteMessageTarget) sendCommand(msg ...*TargetMessage) error {
	if !s.commandSender.ready.Load() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has not been initialized, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has been closed, target: %s, addr: %s", s.targetId, s.targetAddr)}
	case s.sendCmdCh <- s.newMessage(msg...):
		s.sendCmdCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedCmdErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: fmt.Sprintf("Send command message is congested, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
}

func newRemoteMessageTarget(
	ctx context.Context,
	localID, targetId node.ID,
	localEpoch, targetEpoch uint64,
	addr string,
	recvEventCh, recvCmdCh chan *TargetMessage,
	cfg *config.MessageCenterConfig,
	security *security.Credential,
) *remoteMessageTarget {
	log.Info("Create remote target", zap.Stringer("local", localID), zap.Stringer("remote", targetId), zap.Any("addr", addr), zap.Any("localEpoch", localEpoch), zap.Any("targetEpoch", targetEpoch))
	ctx, cancel := context.WithCancel(ctx)
	rt := &remoteMessageTarget{
		messageCenterID:    localID,
		messageCenterEpoch: localEpoch,
		targetAddr:         addr,
		targetId:           targetId,
		security:           security,
		eventSender:        &sendStreamWrapper{ready: atomic.Bool{}},
		commandSender:      &sendStreamWrapper{ready: atomic.Bool{}},
		ctx:                ctx,
		cancel:             cancel,
		sendEventCh:        make(chan *proto.Message, cfg.CacheChannelSize),
		sendCmdCh:          make(chan *proto.Message, cfg.CacheChannelSize),
		recvEventCh:        recvEventCh,
		recvCmdCh:          recvCmdCh,
		errCh:              make(chan AppError, 8),
		wg:                 &sync.WaitGroup{},

		sendEventCounter:           metrics.MessagingSendMsgCounter.WithLabelValues(string(addr), "event"),
		dropEventCounter:           metrics.MessagingDropMsgCounter.WithLabelValues(string(addr), "event"),
		recvEventCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues(string(addr), "event"),
		congestedEventErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "event", "message_congested"),

		sendCmdCounter:           metrics.MessagingSendMsgCounter.WithLabelValues(string(addr), "command"),
		dropCmdCounter:           metrics.MessagingDropMsgCounter.WithLabelValues(string(addr), "command"),
		recvCmdCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues(string(addr), "command"),
		congestedCmdErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "command", "message_congested"),

		receivedFailedErrorCounter:     metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "message_received_failed"),
		connectionNotfoundErrorCounter: metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "connection_not_found"),
		connectionFailedErrorCounter:   metrics.MessagingErrorCounter.WithLabelValues(string(addr), "message", "connection_failed"),
	}
	rt.targetEpoch.Store(targetEpoch)
	rt.runHandleErr(ctx)
	return rt
}

// close stops the grpc stream and the goroutine spawned by remoteMessageTarget.
func (s *remoteMessageTarget) close() {
	log.Info("Closing remote target", zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Any("addr", s.targetAddr))
	s.closeConn()
	s.cancel()
	s.wg.Wait()
	log.Info("Close remote target done", zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId))
}

func (s *remoteMessageTarget) runHandleErr(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Info("remoteMessageTarget exit",
					zap.Any("messageCenterID", s.messageCenterID),
					zap.Any("remote", s.targetId),
					zap.Any("error", ctx.Err()))
				return
			case err := <-s.errCh:
				switch err.Type {
				case ErrorTypeMessageReceiveFailed:
					log.Warn("received message from remote failed, will reset receive stream",
						zap.Any("messageCenterID", s.messageCenterID),
						zap.Any("remote", s.targetId),
						zap.Error(err))
					time.Sleep(reconnectInterval)
					s.resetReceiveStream()
				case ErrorTypeConnectionFailed:
					log.Warn("connection failed, will reset connection",
						zap.Any("messageCenterID", s.messageCenterID),
						zap.Any("remote", s.targetId),
						zap.Error(err))
					time.Sleep(reconnectInterval)
					s.resetConnect()
				default:
					log.Error("Error in remoteMessageTarget, error:", zap.Error(err))
				}
			}
		}
	}()
}

func (s *remoteMessageTarget) collectErr(err AppError) {
	switch err.Type {
	case ErrorTypeMessageReceiveFailed:
		s.receivedFailedErrorCounter.Inc()
	case ErrorTypeConnectionFailed:
		s.connectionFailedErrorCounter.Inc()
	}
	select {
	case s.errCh <- err:
	default:
	}
}

func (s *remoteMessageTarget) connect() {
	if _, ok := s.getConn(); ok {
		return
	}

	conn, err := conn.Connect(string(s.targetAddr), s.security)
	if err != nil {
		log.Info("Cannot create grpc client",
			zap.Any("messageCenterID", s.messageCenterID),
			zap.Any("remote", s.targetId),
			zap.Error(err))

		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error()),
		})
		return
	}

	client := proto.NewMessageCenterClient(conn)
	handshake := &proto.Message{
		From:  string(s.messageCenterID),
		To:    string(s.targetId),
		Epoch: uint64(s.messageCenterEpoch),
		Type:  int32(TypeMessageHandShake),
	}

	eventStream, err := client.SendEvents(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish event grpc stream",
			zap.Any("messageCenterID", s.messageCenterID), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error()),
		})
		return
	}

	commandStream, err := client.SendCommands(s.ctx, handshake)
	if err != nil {
		log.Info("Cannot establish command grpc stream",
			zap.Any("messageCenterID", s.messageCenterID), zap.Stringer("remote", s.targetId), zap.Error(err))
		s.collectErr(AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot open event grpc stream, error: %s", err.Error()),
		})
		return
	}

	s.setConn(conn)
	s.client.eventRecvStream = eventStream
	s.client.commandRecvStream = commandStream
	s.runReceiveMessages(eventStream, s.recvEventCh)
	s.runReceiveMessages(commandStream, s.recvCmdCh)
	log.Info("Connected to remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId),
		zap.Any("remoteAddr", s.targetAddr))
}

func (s *remoteMessageTarget) resetReceiveStream() {
	log.Info("reset receive stream",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId))

	s.client.Lock()
	s.client.eventRecvStream = nil
	s.client.commandRecvStream = nil
	s.client.Unlock()

	// reset the receive stream
	if conn, ok := s.getConn(); ok {
		client := proto.NewMessageCenterClient(conn)
		handshake := &proto.Message{
			From:  string(s.messageCenterID),
			To:    string(s.targetId),
			Epoch: uint64(s.messageCenterEpoch),
			Type:  int32(TypeMessageHandShake),
		}

		s.client.Lock()
		eventStream, err := client.SendEvents(s.ctx, handshake)
		if err != nil {
			log.Error("failed to send events",
				zap.Any("messageCenterID", s.messageCenterID),
				zap.Any("remote", s.targetId),
				zap.Error(err))
		} else {
			s.client.eventRecvStream = eventStream
			s.runReceiveMessages(eventStream, s.recvEventCh)
		}

		if commandStream, err := client.SendCommands(s.ctx, handshake); err != nil {
			log.Error("failed to send commands handshake",
				zap.Any("messageCenterID", s.messageCenterID),
				zap.Any("remote", s.targetId),
				zap.Error(err))
		} else {
			s.client.commandRecvStream = commandStream
			s.runReceiveMessages(commandStream, s.recvCmdCh)
		}
		s.client.Unlock()
	}
}

func (s *remoteMessageTarget) resetConnect() {
	log.Info("reconnect to remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId))
	// Close the old streams
	s.closeConn()
	s.client.eventRecvStream = nil
	s.client.commandRecvStream = nil
	// Clear the error channel
LOOP:
	for {
		select {
		case <-s.errCh:
		default:
			break LOOP
		}
	}
	// Reconnect
	s.connect()
}

func (s *remoteMessageTarget) runEventSendStream(eventStream grpcSender) error {
	s.senderMu.Lock()
	if s.eventSender.stream != nil {
		s.senderMu.Unlock()
		return nil
	}
	s.eventSender.stream = eventStream
	s.eventSender.ready.Store(true)
	s.senderMu.Unlock()

	// send a ack message to the remote target
	ack := &proto.Message{
		From:  string(s.messageCenterID),
		To:    string(s.targetId),
		Epoch: uint64(s.messageCenterEpoch),
		Type:  int32(TypeMessageHandShake),
	}
	if err := eventStream.Send(ack); err != nil {
		return err
	}

	return s.runSendMessages(s.ctx, s.eventSender.stream, s.sendEventCh)
}

func (s *remoteMessageTarget) runCommandSendStream(commandStream grpcSender) error {
	s.senderMu.Lock()
	if s.commandSender.stream != nil {
		s.senderMu.Unlock()
		return nil
	}
	s.commandSender.stream = commandStream
	s.commandSender.ready.Store(true)
	s.senderMu.Unlock()

	err := s.runSendMessages(s.ctx, s.commandSender.stream, s.sendCmdCh)
	log.Info("Command send stream closed",
		zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId), zap.Error(err))
	s.commandSender.ready.Store(false)
	return err
}

func (s *remoteMessageTarget) runSendMessages(sendCtx context.Context, stream grpcSender, sendChan chan *proto.Message) error {
	for {
		select {
		case <-sendCtx.Done():
			return sendCtx.Err()
		case message := <-sendChan:
			if err := stream.Send(message); err != nil {
				log.Error("Error when sending message to remote",
					zap.Error(err),
					zap.Any("messageCenterID", s.messageCenterID),
					zap.Any("remote", s.targetId))
				err = AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
				return err
			}
		}
	}
}

func (s *remoteMessageTarget) runReceiveMessages(stream grpcReceiver, receiveCh chan *TargetMessage) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			message, err := stream.Recv()
			if err != nil {
				err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
				// return the error to close the stream, the client side is responsible to reconnect.
				s.collectErr(err)
				return
			}
			mt := IOType(message.Type)
			if mt == TypeMessageHandShake {
				log.Info("Received handshake message", zap.Any("messageCenterID", s.messageCenterID), zap.Any("remote", s.targetId))
				continue
			}
			targetMsg := &TargetMessage{
				From:     node.ID(message.From),
				To:       node.ID(message.To),
				Topic:    message.Topic,
				Epoch:    message.Epoch,
				Sequence: message.Seqnum,
				Type:     mt,
			}
			for _, payload := range message.Payload {
				msg, err := decodeIOType(mt, payload)
				if err != nil {
					// TODO: handle this error properly.
					err := AppError{Type: ErrorTypeInvalidMessage, Reason: errors.Trace(err).Error()}
					log.Panic("Failed to decode message", zap.Error(err))
				}
				targetMsg.Message = append(targetMsg.Message, msg)
			}
			receiveCh <- targetMsg
		}
	}()
}

func (s *remoteMessageTarget) newMessage(msg ...*TargetMessage) *proto.Message {
	msgBytes := make([][]byte, 0, len(msg))
	for _, tm := range msg {
		for _, im := range tm.Message {
			// TODO: use a buffer pool to reduce the memory allocation.
			buf, err := im.Marshal()
			if err != nil {
				log.Panic("marshal message failed ",
					zap.Any("msg", im),
					zap.Error(err))
			}
			msgBytes = append(msgBytes, buf)
		}
	}
	protoMsg := &proto.Message{
		From:    string(s.messageCenterID),
		To:      string(s.targetId),
		Epoch:   uint64(s.messageCenterEpoch),
		Topic:   string(msg[0].Topic),
		Type:    int32(msg[0].Type),
		Payload: msgBytes,
	}
	return protoMsg
}

func (s *remoteMessageTarget) getConn() (*grpc.ClientConn, bool) {
	s.client.RLock()
	defer s.client.RUnlock()
	return s.client.c, s.client.c != nil
}

func (s *remoteMessageTarget) setConn(conn *grpc.ClientConn) {
	s.client.Lock()
	defer s.client.Unlock()
	s.client.c = conn
}

func (s *remoteMessageTarget) closeConn() {
	if conn, ok := s.getConn(); ok {
		conn.Close()
		s.setConn(nil)
	}
}

// localMessageTarget implements the SendMessageChannel interface.
// It is used to send messages to the local server.
// It simply pushes the messages to the messageCenter's channel directly.
type localMessageTarget struct {
	localId  node.ID
	epoch    uint64
	sequence atomic.Uint64

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	sendEventCounter   prometheus.Counter
	dropMessageCounter prometheus.Counter
	sendCmdCounter     prometheus.Counter
}

func (s *localMessageTarget) Epoch() uint64 {
	return s.epoch
}

func (s *localMessageTarget) sendEvent(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvEventCh, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeEvent)
	} else {
		s.sendEventCounter.Inc()
	}
	return err
}

func (s *localMessageTarget) sendCommand(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvCmdCh, msg)
	if err != nil {
		s.recordCongestedMessageError(msgTypeCommand)
	} else {
		s.sendCmdCounter.Inc()
	}
	return err
}

func newLocalMessageTarget(id node.ID,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage,
) *localMessageTarget {
	return &localMessageTarget{
		localId:            id,
		recvEventCh:        gatherRecvEventChan,
		recvCmdCh:          gatherRecvCmdChan,
		sendEventCounter:   metrics.MessagingSendMsgCounter.WithLabelValues("local", "event"),
		dropMessageCounter: metrics.MessagingDropMsgCounter.WithLabelValues("local", "message"),
		sendCmdCounter:     metrics.MessagingSendMsgCounter.WithLabelValues("local", "command"),
	}
}

func (s *localMessageTarget) recordCongestedMessageError(typeE string) {
	metrics.MessagingErrorCounter.WithLabelValues("local", typeE, "message_congested").Inc()
}

func (s *localMessageTarget) sendMsgToChan(ch chan *TargetMessage, msg ...*TargetMessage) error {
	for i, m := range msg {
		m.To = s.localId
		m.From = s.localId
		m.Epoch = s.epoch
		m.Sequence = s.sequence.Add(1)
		select {
		case ch <- m:
		default:
			remains := len(msg) - i
			s.dropMessageCounter.Add(float64(remains))
			return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
		}
	}
	return nil
}

type sendStreamWrapper struct {
	stream grpcSender
	ready  atomic.Bool
}
