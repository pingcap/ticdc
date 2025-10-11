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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	. "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/utils/conn"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	reconnectInterval = 2 * time.Second
	streamTypeEvent   = "event"
	streamTypeCommand = "command"

	eventRecvCh   = "eventRecvCh"
	commandRecvCh = "commandRecvCh"

	eventSendCh   = "eventSendCh"
	commandSendCh = "commandSendCh"
)

type streamSession struct {
	stream grpcStream
	cancel context.CancelFunc
}

// remoteMessageTarget represents a connection to a remote message center node.
// It handles bidirectional message streaming for both events and commands.
type remoteMessageTarget struct {
	messageCenterID node.ID
	localAddr       string
	targetId        node.ID
	targetAddr      string
	security        *security.Credential

	streams sync.Map // string(streamType) -> *streamSession

	// GRPC client connection to the remote target
	conn struct {
		sync.RWMutex
		c *grpc.ClientConn
	}

	// Channels for sending different types of messages
	sendEventCh chan *proto.Message
	sendCmdCh   chan *proto.Message

	// Channels for receiving different types of messages to pass to message center
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	errCh chan error

	ctx    context.Context
	cancel context.CancelFunc

	// Metrics for monitoring
	sendEventCounter           prometheus.Counter
	dropEventCounter           prometheus.Counter
	recvEventCounter           prometheus.Counter
	congestedEventErrorCounter prometheus.Counter

	sendCmdCounter           prometheus.Counter
	dropCmdCounter           prometheus.Counter
	recvCmdCounter           prometheus.Counter
	congestedCmdErrorCounter prometheus.Counter

	errorCounter prometheus.Counter

	// Whether this node is the initiator of the connection
	// If true, it will initiate the connection to the remote target
	// If false, it will wait for the remote target to initiate the connection
	isInitiator bool
}

// Check if this target is ready to send messages
func (s *remoteMessageTarget) isReadyToSend() bool {
	ready := true
	s.streams.Range(func(key, value interface{}) bool {
		if value == nil {
			ready = false
		}
		return true
	})
	return ready
}

// Send an event message to the remote target
func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if !s.isReadyToSend() {
		s.errorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: genSendErrorMsg("Stream not ready", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	}

	// Create message with EVENT type
	protoMsg := s.newMessage(msg...)

	select {
	case <-s.ctx.Done():
		s.errorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: genSendErrorMsg("Stream has been closed", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	case s.sendEventCh <- protoMsg:
		s.sendEventCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedEventErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: genSendErrorMsg("Send event message is congested", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	}
}

// Send a command message to the remote target
func (s *remoteMessageTarget) sendCommand(msg ...*TargetMessage) error {
	if !s.isReadyToSend() {
		s.errorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: genSendErrorMsg("Stream not ready", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	}

	// Create message with COMMAND type
	protoMsg := s.newMessage(msg...)

	select {
	case <-s.ctx.Done():
		s.errorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: genSendErrorMsg("Stream has been closed", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	case s.sendCmdCh <- protoMsg:
		s.sendCmdCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedCmdErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: genSendErrorMsg("Send command message is congested", string(s.messageCenterID), s.localAddr, string(s.targetId), s.targetAddr)}
	}
}

func genSendErrorMsg(reason string, localID, localAddr, targetID, targetAddr string) string {
	return fmt.Sprintf("%s, local: %s, localAddr: %s, target: %s, targetAddr: %s", reason, localID, localAddr, targetID, targetAddr)
}

// Create a new remote message target
func newRemoteMessageTarget(
	ctx context.Context,
	localID, targetId node.ID,
	localAddr, targetAddr string,
	recvEventCh, recvCmdCh chan *TargetMessage,
	cfg *config.MessageCenterConfig,
	security *security.Credential,
) *remoteMessageTarget {
	ctx, cancel := context.WithCancel(ctx)

	// Determine if this node should initiate the connection based on node address
	// If the local address is less than the target address, this node should initiate the connection.
	shouldInitiate := localAddr < targetAddr
	log.Info("Create remote target",
		zap.Stringer("localID", localID),
		zap.String("localAddr", localAddr),
		zap.Stringer("remoteID", targetId),
		zap.String("remoteAddr", targetAddr),
		zap.Bool("shouldInitiate", shouldInitiate))

	rt := &remoteMessageTarget{
		messageCenterID: localID,
		localAddr:       localAddr,
		targetAddr:      targetAddr,
		targetId:        targetId,
		security:        security,
		ctx:             ctx,
		cancel:          cancel,
		sendEventCh:     make(chan *proto.Message, cfg.CacheChannelSize),
		sendCmdCh:       make(chan *proto.Message, cfg.CacheChannelSize),
		recvEventCh:     recvEventCh,
		recvCmdCh:       recvCmdCh,
		isInitiator:     shouldInitiate,
		errCh:           make(chan error, 32),

		// Initialize metrics
		sendEventCounter:           metrics.MessagingSendMsgCounter.WithLabelValues("event"),
		dropEventCounter:           metrics.MessagingDropMsgCounter.WithLabelValues("event"),
		recvEventCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues("event"),
		congestedEventErrorCounter: metrics.MessagingErrorCounter.WithLabelValues("event", "message_congested"),

		sendCmdCounter:           metrics.MessagingSendMsgCounter.WithLabelValues("command"),
		dropCmdCounter:           metrics.MessagingDropMsgCounter.WithLabelValues("command"),
		recvCmdCounter:           metrics.MessagingReceiveMsgCounter.WithLabelValues("command"),
		congestedCmdErrorCounter: metrics.MessagingErrorCounter.WithLabelValues("command", "message_congested"),

		errorCounter: metrics.MessagingErrorCounter.WithLabelValues("message", "error"),
	}

	// initialize streams placeholder
	rt.streams.Store(streamTypeEvent, nil)
	rt.streams.Store(streamTypeCommand, nil)

	err := rt.connect()
	if err != nil {
		log.Error("Failed to connect to remote target", zap.Error(err))
		rt.collectErr(err)
	}

	return rt
}

// Close the target and clean up resources
func (s *remoteMessageTarget) close() {
	log.Info("Closing remote target",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))

	s.closeConn()
	s.cancel()

	log.Info("Close remote target done",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))
}

// Collect and report errors
func (s *remoteMessageTarget) collectErr(err error) {
	s.errorCounter.Inc()
	select {
	case s.errCh <- err:
	default:
		log.Error("Failed to collect error, channel is full", zap.Error(err))
	}
}

// Connect to the remote target
func (s *remoteMessageTarget) connect() error {
	// Only the node with the smaller ID should initiate the connection
	if !s.isInitiator {
		log.Info("Not initiating connection as remote has smaller ID",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr))
		return nil
	}

	log.Info("Initiating connection to remote target",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))

	conn, err := conn.Connect(string(s.targetAddr), s.security)
	if err != nil {
		log.Info("Cannot create grpc client",
			zap.Any("localID", s.messageCenterID),
			zap.Any("localAddr", s.localAddr),
			zap.Any("remoteID", s.targetId),
			zap.Error(err))

		return AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error()),
		}
	}

	client := proto.NewMessageServiceClient(conn)

	var outerErr error

	s.streams.Range(func(key, value interface{}) bool {
		streamType := key.(string)
		stream, ok := value.(grpcStream)
		if ok && stream != nil {
			log.Panic("Stream already exists",
				zap.Any("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Any("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr),
				zap.String("streamType", streamType))
		}

		streamCtx, streamCancel := context.WithCancel(s.ctx)
		gs, err := client.StreamMessages(streamCtx)
		if err != nil {
			log.Info("Cannot establish bidirectional grpc stream",
				zap.Any("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Any("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr),
				zap.Error(err))

			err = AppError{
				Type:   ErrorTypeConnectionFailed,
				Reason: fmt.Sprintf("Cannot open bidirectional grpc stream, error: %s", errors.Trace(err).Error()),
			}
			outerErr = err
			streamCancel()
			return false
		}
		session := &streamSession{
			stream: gs,
			cancel: streamCancel,
		}

		handshake := &HandshakeMessage{
			Version:    1,
			Timestamp:  time.Now().Unix(),
			StreamType: streamType,
		}

		hsBytes, err := handshake.Marshal()
		if err != nil {
			log.Error("Failed to marshal handshake message", zap.Error(err))
			err = AppError{Type: ErrorTypeMessageSendFailed, Reason: errors.Trace(err).Error()}
			outerErr = err
			return false
		}

		// Send handshake message to identify this node
		msg := &proto.Message{
			From:    string(s.messageCenterID),
			To:      string(s.targetId),
			Type:    int32(TypeMessageHandShake),
			Payload: [][]byte{hsBytes},
		}

		if err := gs.Send(msg); err != nil {
			log.Info("Failed to send handshake",
				zap.Any("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Any("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr),
				zap.Error(err))
			err = AppError{
				Type:   ErrorTypeMessageSendFailed,
				Reason: fmt.Sprintf("Failed to send handshake, error: %s", errors.Trace(err).Error()),
			}
			outerErr = err
			return false
		}

		s.streams.Store(streamType, session)
		return true
	})

	if outerErr != nil {
		log.Error("Failed to connect to remote target", zap.Error(outerErr))
		return outerErr
	}

	s.setConn(conn)

	// Start goroutines for sending messages
	eg, egCtx := errgroup.WithContext(s.ctx)
	s.streams.Range(func(key, value interface{}) bool {
		streamType := key.(string)
		s.run(eg, egCtx, streamType)
		return true
	})

	log.Info("Connected to remote target",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))

	return nil
}

// Reset the connection to the remote target
func (s *remoteMessageTarget) resetConnect() {
	// Only reconnect if this node should initiate connections
	if !s.isInitiator {
		return
	}
	log.Info("start to reset connection to remote target",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))

	// Close the old connection
	s.closeConn()

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
	err := s.connect()
	if err != nil {
		log.Error("Failed to connect to remote target", zap.Error(err))
		s.collectErr(err)
	}

	log.Info("reset connection to remote target done",
		zap.Any("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Any("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))
}

// Handle an incoming stream connection from a remote node, it will block until remote cancel the stream.
func (s *remoteMessageTarget) handleIncomingStream(stream proto.MessageService_StreamMessagesServer, handshake *HandshakeMessage) error {
	// Only accept incoming connections if this node should not initiate
	if s.isInitiator {
		log.Warn("Received unexpected connection from node with higher ID",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr))
		return fmt.Errorf("connection policy violation: local node should initiate connection")
	}

	// Cancel the old stream session if it exists.
	if old, ok := s.streams.Load(handshake.StreamType); ok && old != nil {
		log.Info("Canceling old stream session",
			zap.Stringer("localID", s.messageCenterID),
			zap.Stringer("remoteID", s.targetId),
			zap.String("streamType", handshake.StreamType))
		old.(*streamSession).cancel()
	}

	// Create a new context for this stream session that can be cancelled independently.
	streamCtx, streamCancel := context.WithCancel(s.ctx)
	session := &streamSession{
		stream: stream,
		cancel: streamCancel,
	}
	s.streams.Store(handshake.StreamType, session)

	eg, egCtx := errgroup.WithContext(streamCtx)
	// Start goroutines for sending and receiving messages
	s.run(eg, egCtx, handshake.StreamType)
	// Block until the there is an error or the context is done
	return eg.Wait()
}

// run spawn two goroutines to handle message sending and receiving
func (s *remoteMessageTarget) run(eg *errgroup.Group, ctx context.Context, streamType string) {
	eg.Go(func() error {
		return s.runReceiveMessages(ctx, streamType)
	})
	eg.Go(func() error {
		return s.runSendMessages(ctx, streamType)
	})

	log.Info("Start running remote target to process messages",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr),
		zap.String("streamType", streamType))
}

// Run goroutine to handle message sending
func (s *remoteMessageTarget) runSendMessages(ctx context.Context, streamType string) (err error) {
	defer func() {
		if err != nil {
			s.collectErr(err)
		}
		log.Info("exit runSendMessages",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr),
			zap.String("streamType", streamType),
			zap.Error(err))
	}()

	// wait stream ready
	for {
		if s.isReadyToSend() {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			log.Warn("remote target stream is not ready, wait and check again",
				zap.Stringer("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Stringer("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr))
			continue
		}
	}

	// Get the stream (it might have changed due to reconnection)
	session, _ := s.streams.Load(streamType)
	if session == nil {
		log.Info("Stream is nil, it might have been closed by new connection, exit",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr))
		return nil
	}

	gs := session.(*streamSession).stream

	sendCh := s.sendEventCh
	if streamType == streamTypeCommand {
		sendCh = s.sendCmdCh
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-sendCh:
			failpoint.Inject("InjectDropRemoteMessage", func() {
				log.Info("Inject Drop Remote Message", zap.Stringer("localID", s.messageCenterID), zap.String("localAddr", s.localAddr), zap.Stringer("remoteID", s.targetId), zap.String("remoteAddr", s.targetAddr), zap.String("streamType", streamType), zap.Any("message", msg))
				failpoint.Continue()
			})

			if err := gs.Send(msg); err != nil {
				log.Error("Error sending message",
					zap.Error(err),
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr),
					zap.String("streamType", streamType),
					zap.Stringer("message", msg))
				err = AppError{Type: ErrorTypeMessageSendFailed, Reason: errors.Trace(err).Error()}
				return err
			}
		}
	}
}

// Run goroutine to handle message receiving
func (s *remoteMessageTarget) runReceiveMessages(ctx context.Context, streamType string) (err error) {
	defer func() {
		if err != nil {
			s.collectErr(err)
		}
		log.Info("exit runReceiveMessages",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr),
			zap.String("streamType", streamType),
			zap.Error(err))
	}()

	// wait stream ready
	for {
		if s.isReadyToSend() {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			log.Warn("remote target stream is not ready, wait and check again",
				zap.Stringer("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Stringer("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr))
			continue
		}
	}

	// Get the stream (it might have changed due to reconnection)
	session, _ := s.streams.Load(streamType)
	if session == nil {
		log.Info("Stream is nil, it might have been closed by new connection, exit",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr))
		return nil
	}

	gs := session.(*streamSession).stream

	recvCh := s.recvEventCh
	if streamType == streamTypeCommand {
		recvCh = s.recvCmdCh
	}

	// Process the received message
	return s.handleIncomingMessage(ctx, gs, recvCh)
}

// Process a received message
func (s *remoteMessageTarget) handleIncomingMessage(ctx context.Context, stream grpcStream, ch chan *TargetMessage) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		message, err := stream.Recv()
		if err != nil {
			log.Error("Error receiving message",
				zap.Error(err),
				zap.Stringer("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Stringer("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr))
			err = AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
			return err
		}

		mt := IOType(message.Type)

		targetMsg := &TargetMessage{
			From:  node.ID(message.From),
			To:    node.ID(message.To),
			Topic: message.Topic,
			Type:  mt,
		}

		for _, payload := range message.Payload {
			msg, err := decodeIOType(mt, payload)
			if err != nil {
				log.Error("Failed to decode message",
					zap.Error(err),
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr))
				continue
			}
			targetMsg.Message = append(targetMsg.Message, msg)
		}

		ch <- targetMsg
	}
}

// Create a new protocol message from target messages
func (s *remoteMessageTarget) newMessage(msg ...*TargetMessage) *proto.Message {
	msgBytes := make([][]byte, 0, len(msg))
	for _, tm := range msg {
		for _, im := range tm.Message {
			// Use buffer for marshaling
			buf, err := im.Marshal()
			if err != nil {
				log.Panic("marshal message failed ",
					zap.Any("msg", im),
					zap.Error(err),
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr))
			}
			msgBytes = append(msgBytes, buf)
		}
	}
	protoMsg := &proto.Message{
		From:    string(s.messageCenterID),
		To:      string(s.targetId),
		Topic:   string(msg[0].Topic),
		Type:    int32(msg[0].Type),
		Payload: msgBytes,
	}
	return protoMsg
}

// Get the connection to the remote target
func (s *remoteMessageTarget) getConn() (*grpc.ClientConn, bool) {
	s.conn.RLock()
	defer s.conn.RUnlock()
	return s.conn.c, s.conn.c != nil
}

// Set the connection to the remote target
func (s *remoteMessageTarget) setConn(conn *grpc.ClientConn) {
	s.conn.Lock()
	defer s.conn.Unlock()
	s.conn.c = conn
}

// Close the connection to the remote target
func (s *remoteMessageTarget) closeConn() {
	if conn, ok := s.getConn(); ok {
		conn.Close()
		s.setConn(nil)
	}

	s.streams.Range(func(key, value interface{}) bool {
		if value != nil {
			value.(*streamSession).cancel()
		}
		s.streams.Store(key, nil)
		return true
	})
}

func (s *remoteMessageTarget) getErr() error {
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}
