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

const (
	reconnectInterval = 2 * time.Second
	streamTypeEvent   = "event"
	streamTypeCommand = "command"

	eventRecvCh   = "eventRecvCh"
	commandRecvCh = "commandRecvCh"

	eventSendCh   = "eventSendCh"
	commandSendCh = "commandSendCh"
)

// remoteMessageTarget represents a connection to a remote message center node.
// It handles bidirectional message streaming for both events and commands.
type remoteMessageTarget struct {
	messageCenterID node.ID
	targetId        node.ID
	targetAddr      string
	security        *security.Credential

	streams struct {
		sync.RWMutex
		m map[string]grpcStream
	}

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

	errCh chan AppError

	wg     *sync.WaitGroup
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

	receivedFailedErrorCounter     prometheus.Counter
	connectionNotfoundErrorCounter prometheus.Counter
	connectionFailedErrorCounter   prometheus.Counter

	// Whether this node should initiate the connection
	shouldInitiate bool
}

// Check if this target is ready to send messages
func (s *remoteMessageTarget) isReadyToSend() bool {
	s.streams.RLock()
	defer s.streams.RUnlock()
	for _, stream := range s.streams.m {
		if stream == nil {
			return false
		}
	}
	return true
}

// Send an event message to the remote target
func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if !s.isReadyToSend() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream not ready, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}

	// Create message with EVENT type
	protoMsg := s.newMessage(msg...)

	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has been closed, target: %s, addr: %s", s.targetId, s.targetAddr)}
	case s.sendEventCh <- protoMsg:
		s.sendEventCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedEventErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: fmt.Sprintf("Send event message is congested, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
}

// Send a command message to the remote target
func (s *remoteMessageTarget) sendCommand(msg ...*TargetMessage) error {
	if !s.isReadyToSend() {
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream not ready, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}

	// Create message with COMMAND type
	protoMsg := s.newMessage(msg...)

	select {
	case <-s.ctx.Done():
		s.connectionNotfoundErrorCounter.Inc()
		return AppError{Type: ErrorTypeConnectionNotFound, Reason: fmt.Sprintf("Stream has been closed, target: %s, addr: %s", s.targetId, s.targetAddr)}
	case s.sendCmdCh <- protoMsg:
		s.sendCmdCounter.Add(float64(len(msg)))
		return nil
	default:
		s.congestedCmdErrorCounter.Inc()
		return AppError{Type: ErrorTypeMessageCongested, Reason: fmt.Sprintf("Send command message is congested, target: %s, addr: %s", s.targetId, s.targetAddr)}
	}
}

// Create a new remote message target
func newRemoteMessageTarget(
	ctx context.Context,
	localID, targetId node.ID,
	addr string,
	recvEventCh, recvCmdCh chan *TargetMessage,
	cfg *config.MessageCenterConfig,
	security *security.Credential,
) *remoteMessageTarget {
	log.Info("Create remote target",
		zap.Stringer("local", localID),
		zap.Stringer("remote", targetId),
		zap.Any("addr", addr))

	ctx, cancel := context.WithCancel(ctx)

	// Determine if this node should initiate the connection based on node ID
	shouldInitiate := localID < targetId

	rt := &remoteMessageTarget{
		messageCenterID: localID,
		targetAddr:      addr,
		targetId:        targetId,
		security:        security,
		ctx:             ctx,
		cancel:          cancel,
		sendEventCh:     make(chan *proto.Message, cfg.CacheChannelSize),
		sendCmdCh:       make(chan *proto.Message, cfg.CacheChannelSize),
		recvEventCh:     recvEventCh,
		recvCmdCh:       recvCmdCh,
		wg:              &sync.WaitGroup{},
		shouldInitiate:  shouldInitiate,
		errCh:           make(chan AppError, 8),

		// Initialize metrics
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

	// initialize streams placeholder
	rt.streams.m = make(map[string]grpcStream)
	rt.streams.m[streamTypeEvent] = nil
	rt.streams.m[streamTypeCommand] = nil

	return rt
}

// Close the target and clean up resources
func (s *remoteMessageTarget) close() {
	log.Info("Closing remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId),
		zap.Any("addr", s.targetAddr))

	s.closeConn()
	s.cancel()
	s.wg.Wait()

	log.Info("Close remote target done",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId))
}

// Collect and report errors
func (s *remoteMessageTarget) collectErr(err AppError) {
	switch err.Type {
	case ErrorTypeMessageReceiveFailed:
		s.receivedFailedErrorCounter.Inc()
	case ErrorTypeConnectionFailed:
		s.connectionFailedErrorCounter.Inc()
	}
	s.errCh <- err
}

// Connect to the remote target
func (s *remoteMessageTarget) connect() error {
	// Only the node with the smaller ID should initiate the connection
	if !s.shouldInitiate {
		log.Info("Not initiating connection as remote has smaller ID",
			zap.Stringer("local", s.messageCenterID),
			zap.Stringer("remote", s.targetId))
		return nil
	}

	if _, ok := s.getConn(); ok {
		return nil
	}

	log.Info("Initiating connection to remote target",
		zap.Stringer("local", s.messageCenterID),
		zap.Stringer("remote", s.targetId))

	conn, err := conn.Connect(string(s.targetAddr), s.security)
	if err != nil {
		log.Info("Cannot create grpc client",
			zap.Any("messageCenterID", s.messageCenterID),
			zap.Any("remote", s.targetId),
			zap.Error(err))

		return AppError{
			Type:   ErrorTypeConnectionFailed,
			Reason: fmt.Sprintf("Cannot create grpc client on address %s, error: %s", s.targetAddr, err.Error()),
		}
	}

	client := proto.NewMessageServiceClient(conn)

	s.streams.Lock()
	for streamType, stream := range s.streams.m {
		if stream != nil {
			log.Panic("Stream already exists",
				zap.Any("messageCenterID", s.messageCenterID),
				zap.Stringer("remote", s.targetId),
				zap.String("streamType", streamType))
		}

		stream, err := client.StreamMessages(s.ctx)
		if err != nil {
			log.Info("Cannot establish bidirectional grpc stream",
				zap.Any("messageCenterID", s.messageCenterID),
				zap.Stringer("remote", s.targetId),
				zap.Error(err))
			return AppError{
				Type:   ErrorTypeConnectionFailed,
				Reason: fmt.Sprintf("Cannot open bidirectional grpc stream, error: %s", err.Error()),
			}
		}

		handshake := &HandshakeMessage{
			Version:    1,
			Timestamp:  time.Now().Unix(),
			StreamType: streamType,
		}

		hsBytes, err := handshake.Marshal()
		if err != nil {
			log.Error("Failed to marshal handshake message", zap.Error(err))
			return AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()}
		}

		// Send handshake message to identify this node
		msg := &proto.Message{
			From:    string(s.messageCenterID),
			To:      string(s.targetId),
			Type:    int32(TypeMessageHandShake),
			Payload: [][]byte{hsBytes},
		}

		if err := stream.Send(msg); err != nil {
			log.Info("Failed to send handshake",
				zap.Any("messageCenterID", s.messageCenterID),
				zap.Stringer("remote", s.targetId),
				zap.Error(err))
			return AppError{
				Type:   ErrorTypeMessageSendFailed,
				Reason: fmt.Sprintf("Failed to send handshake, error: %s", err.Error()),
			}
		}
		s.streams.m[streamType] = stream
	}

	s.streams.Unlock()
	s.setConn(conn)

	// Start goroutines for sending messages
	for streamType := range s.streams.m {
		s.runSendMessages(streamType)
	}

	log.Info("Connected to remote target",
		zap.Stringer("messageCenterID", s.messageCenterID),
		zap.String("messageCenterAddr", s.targetAddr),
		zap.Stringer("remote", s.targetId),
		zap.String("remoteAddr", s.targetAddr))

	return nil
}

// Reset the connection to the remote target
func (s *remoteMessageTarget) resetConnect() {
	// Only reconnect if this node should initiate connections
	if !s.shouldInitiate {
		return
	}

	log.Info("reconnect to remote target",
		zap.Any("messageCenterID", s.messageCenterID),
		zap.Any("remote", s.targetId))

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
	s.connect()
}

// Handle an incoming stream connection from a remote node, it will block until remote cancel the stream.
func (s *remoteMessageTarget) handleIncomingStream(stream proto.MessageService_StreamMessagesServer, handshake *HandshakeMessage) error {
	// Only accept incoming connections if this node should not initiate
	if s.shouldInitiate {
		log.Warn("Received unexpected connection from node with higher ID",
			zap.Stringer("local", s.messageCenterID),
			zap.Stringer("remote", s.targetId))
		return fmt.Errorf("connection policy violation: local node should initiate connection")
	}

	s.streams.Lock()
	s.streams.m[handshake.StreamType] = stream
	s.streams.Unlock()

	// Start goroutines for sending and receiving messages
	return s.runReceiveMessages(handshake.StreamType)
}

// Run goroutine to handle message sending
func (s *remoteMessageTarget) runSendMessages(streamType string) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			if !s.isReadyToSend() {
				// If stream is not ready, wait and check again
				select {
				case <-s.ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
					log.Warn("remote target stream is not ready, wait and check again",
						zap.Stringer("local", s.messageCenterID),
						zap.String("localAddr", s.targetAddr),
						zap.Stringer("remote", s.targetId),
						zap.String("remoteAddr", s.targetAddr))
					continue
				}
			}

			// Get the stream (it might have changed due to reconnection)
			s.streams.Lock()
			stream := s.streams.m[streamType]
			s.streams.Unlock()

			if stream == nil {
				// Wait for stream to be established
				time.Sleep(100 * time.Millisecond)
				continue
			}

			sendCh := s.sendEventCh
			if streamType == streamTypeCommand {
				sendCh = s.sendCmdCh
			}

			select {
			case <-s.ctx.Done():
				return
			case msg := <-sendCh:
				if err := stream.Send(msg); err != nil {
					log.Error("Error sending message",
						zap.Error(err),
						zap.Stringer("local", s.messageCenterID),
						zap.Stringer("remote", s.targetId))
					s.collectErr(AppError{Type: ErrorTypeMessageSendFailed, Reason: err.Error()})
					return
				}
			}
		}
	}()
}

// Run goroutine to handle message receiving
func (s *remoteMessageTarget) runReceiveMessages(streamType string) error {
	for {
		if !s.isReadyToSend() {
			// If stream is not ready, wait and check again
			select {
			case <-s.ctx.Done():
				return fmt.Errorf("target context done")
			case <-time.After(500 * time.Millisecond):
				log.Warn("remote target stream is not ready, wait and check again",
					zap.Stringer("local", s.messageCenterID),
					zap.String("localAddr", s.targetAddr),
					zap.Stringer("remote", s.targetId),
					zap.String("remoteAddr", s.targetAddr))
				continue
			}
		}

		// Get the stream (it might have changed due to reconnection)
		s.streams.Lock()
		stream := s.streams.m[streamType]
		s.streams.Unlock()

		recvCh := s.recvEventCh
		if streamType == streamTypeCommand {
			recvCh = s.recvCmdCh
		}

		// Process the received message
		if err := s.handleReceivedMessage(stream, recvCh); err != nil {
			s.streams.Lock()
			defer s.streams.Unlock()
			s.streams.m[streamType] = nil
			return err
		}
	}
}

// Process a received message
func (s *remoteMessageTarget) handleReceivedMessage(stream grpcStream, ch chan *TargetMessage) error {
	for {
		select {
		case <-s.ctx.Done():
			return fmt.Errorf("target context done")
		default:
		}
		message, err := stream.Recv()
		if err != nil {
			log.Error("Error receiving message",
				zap.Error(err),
				zap.Stringer("localID", s.messageCenterID),
				zap.String("localAddr", s.targetAddr),
				zap.Stringer("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr))
			err := AppError{Type: ErrorTypeMessageReceiveFailed, Reason: errors.Trace(err).Error()}
			return fmt.Errorf("failed to receive message: %s", err)
		}

		mt := IOType(message.Type)

		targetMsg := &TargetMessage{
			From:  node.ID(message.From),
			To:    node.ID(message.To),
			Topic: message.Topic,
			Type:  mt,
		}

		log.Info("fizz: Received message",
			zap.Any("message", message))

		for _, payload := range message.Payload {
			msg, err := decodeIOType(mt, payload)
			if err != nil {
				log.Error("Failed to decode message",
					zap.Error(err),
					zap.Stringer("local", s.messageCenterID),
					zap.Stringer("remote", s.targetId))
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

	s.streams.Lock()
	for streamType := range s.streams.m {
		s.streams.m[streamType] = nil
	}
	s.streams.Unlock()
}

func (s *remoteMessageTarget) getErr() error {
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}
