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
	goerrors "errors"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	reconnectInterval = 2 * time.Second

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

	streams sync.Map // StreamType -> *streamSession

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

func (s *remoteMessageTarget) isReadyToSendByStream(streamType StreamType) bool {
	session, ok := s.streams.Load(streamType)
	return ok && session != nil
}

func (s *remoteMessageTarget) updateStreamGauge() {
	readyCount := 0
	s.streams.Range(func(_, value interface{}) bool {
		if value != nil {
			readyCount++
		}
		return true
	})
	metrics.MessagingStreamGauge.WithLabelValues(s.targetId.String()).Set(float64(readyCount))
}

func (s *remoteMessageTarget) clearMetrics() {
	metrics.MessagingStreamGauge.DeleteLabelValues(s.targetId.String())
}

func isExpectedStreamShutdown(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() == nil {
		return false
	}
	if goerrors.Is(err, context.Canceled) || goerrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func getMessagingErrorReason(err error) string {
	switch e := errors.Cause(err).(type) {
	case AppError:
		return e.Type.String()
	case *AppError:
		return e.Type.String()
	default:
		return ErrorTypeUnknown.String()
	}
}

// Send an event message to the remote target
func (s *remoteMessageTarget) sendEvent(msg ...*TargetMessage) error {
	if !s.isReadyToSendByStream(EventStreamType) {
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
	if !s.isReadyToSendByStream(CommandStreamType) {
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
	rt.streams.Store(EventStreamType, nil)
	rt.streams.Store(CommandStreamType, nil)
	rt.updateStreamGauge()

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
	s.clearMetrics()

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
		metrics.MessagingConnectCounter.WithLabelValues("fail").Inc()
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
		streamType := key.(StreamType)
		stream, ok := value.(grpcStream)
		if ok && stream != nil {
			log.Panic("Stream already exists",
				zap.Any("localID", s.messageCenterID),
				zap.String("localAddr", s.localAddr),
				zap.Any("remoteID", s.targetId),
				zap.String("remoteAddr", s.targetAddr),
				zap.String("streamType", string(streamType)))
		}

		streamCtx, streamCancel := context.WithCancel(s.ctx)
		gs, err := client.StreamMessages(streamCtx)
		if err != nil {
			metrics.MessagingConnectCounter.WithLabelValues("fail").Inc()
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
			StreamType: string(streamType),
		}

		hsBytes, err := handshake.Marshal()
		if err != nil {
			metrics.MessagingConnectCounter.WithLabelValues("fail").Inc()
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
			metrics.MessagingConnectCounter.WithLabelValues("fail").Inc()
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
		s.updateStreamGauge()
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
		streamType := key.(StreamType)
		s.run(eg, egCtx, streamType)
		return true
	})

	log.Info("Connected to remote target",
		zap.Stringer("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Stringer("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))
	metrics.MessagingConnectCounter.WithLabelValues("success").Inc()

	return nil
}

// Reset the connection to the remote target
func (s *remoteMessageTarget) resetConnect(reason string) {
	// Only reconnect if this node should initiate connections
	if !s.isInitiator || s.ctx.Err() != nil {
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
		metrics.MessagingResetCounter.WithLabelValues("fail", reason).Inc()
		log.Error("Failed to connect to remote target", zap.Error(err))
		s.collectErr(err)
	} else {
		metrics.MessagingResetCounter.WithLabelValues("success", reason).Inc()
	}

	log.Info("reset connection to remote target done",
		zap.Any("localID", s.messageCenterID),
		zap.String("localAddr", s.localAddr),
		zap.Any("remoteID", s.targetId),
		zap.String("remoteAddr", s.targetAddr))
}

// Handle an incoming stream connection from a remote node, it will block until remote cancel the stream.
func (s *remoteMessageTarget) handleIncomingStream(stream proto.MessageService_StreamMessagesServer, handshake *HandshakeMessage) error {
	streamType := StreamType(handshake.StreamType)
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
	if old, ok := s.streams.Load(streamType); ok && old != nil {
		log.Info("Canceling old stream session",
			zap.Stringer("localID", s.messageCenterID),
			zap.Stringer("remoteID", s.targetId),
			zap.String("streamType", string(streamType)))
		old.(*streamSession).cancel()
	}

	// Create a new context for this stream session that can be cancelled independently.
	streamCtx, streamCancel := context.WithCancel(s.ctx)
	session := &streamSession{
		stream: stream,
		cancel: streamCancel,
	}
	s.streams.Store(streamType, session)
	s.updateStreamGauge()

	eg, egCtx := errgroup.WithContext(streamCtx)
	// Start goroutines for sending and receiving messages
	s.run(eg, egCtx, streamType)
	// Block until the there is an error or the context is done
	return eg.Wait()
}

// run spawn two goroutines to handle message sending and receiving
func (s *remoteMessageTarget) run(eg *errgroup.Group, ctx context.Context, streamType StreamType) {
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
		zap.String("streamType", string(streamType)))
}

// Run goroutine to handle message sending
func (s *remoteMessageTarget) runSendMessages(ctx context.Context, streamType StreamType) (err error) {
	defer func() {
		if err != nil && !isExpectedStreamShutdown(ctx, err) {
			s.collectErr(err)
		}
		log.Info("exit runSendMessages",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr),
			zap.String("streamType", string(streamType)),
			zap.Error(err))
	}()

	// wait stream ready
	for {
		if s.isReadyToSendByStream(streamType) {
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
	if streamType == CommandStreamType {
		sendCh = s.sendCmdCh
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-sendCh:
			failpoint.Inject("InjectDropRemoteMessage", func() {
				log.Info("Inject Drop Remote Message", zap.Stringer("localID", s.messageCenterID), zap.String("localAddr", s.localAddr), zap.Stringer("remoteID", s.targetId), zap.String("remoteAddr", s.targetAddr), zap.String("streamType", string(streamType)), zap.Any("message", msg))
				failpoint.Continue()
			})

			if err := gs.Send(msg); err != nil {
				if isExpectedStreamShutdown(ctx, err) {
					log.Info("remote target send loop exits with canceled stream",
						zap.Stringer("localID", s.messageCenterID),
						zap.String("localAddr", s.localAddr),
						zap.Stringer("remoteID", s.targetId),
						zap.String("remoteAddr", s.targetAddr),
						zap.String("streamType", string(streamType)))
					return ctx.Err()
				}
				log.Error("Error sending message",
					zap.Error(err),
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr),
					zap.String("streamType", string(streamType)),
					zap.Stringer("message", msg))
				err = AppError{Type: ErrorTypeMessageSendFailed, Reason: errors.Trace(err).Error()}
				return err
			}
		}
	}
}

// Run goroutine to handle message receiving
func (s *remoteMessageTarget) runReceiveMessages(ctx context.Context, streamType StreamType) (err error) {
	defer func() {
		if err != nil && !isExpectedStreamShutdown(ctx, err) {
			s.collectErr(err)
		}
		log.Info("exit runReceiveMessages",
			zap.Stringer("localID", s.messageCenterID),
			zap.String("localAddr", s.localAddr),
			zap.Stringer("remoteID", s.targetId),
			zap.String("remoteAddr", s.targetAddr),
			zap.String("streamType", string(streamType)),
			zap.Error(err))
	}()

	// wait stream ready
	for {
		if s.isReadyToSendByStream(streamType) {
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
	recvCounter := s.recvEventCounter
	if streamType == CommandStreamType {
		recvCh = s.recvCmdCh
		recvCounter = s.recvCmdCounter
	}

	// Process the received message
	return s.handleIncomingMessage(ctx, gs, recvCh, recvCounter, streamType)
}

// Process a received message
func (s *remoteMessageTarget) handleIncomingMessage(
	ctx context.Context,
	stream grpcStream,
	ch chan *TargetMessage,
	recvCounter prometheus.Counter,
	streamType StreamType,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		message, err := stream.Recv()
		if err != nil {
			if isExpectedStreamShutdown(ctx, err) {
				log.Info("remote target receive loop exits with canceled stream",
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr))
				return ctx.Err()
			}
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
			From:     node.ID(message.From),
			To:       node.ID(message.To),
			Topic:    message.Topic,
			Type:     mt,
			CreateAt: message.CreateAt,
			Group:    message.Group,
		}

		for _, payload := range message.Payload {
			msg, err := decodeIOType(mt, payload)
			if err != nil {
				metrics.MessagingErrorCounter.WithLabelValues(string(streamType), "decode_error").Inc()
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

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- targetMsg:
			recvCounter.Inc()
		}
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
				log.Warn("marshal message failed ",
					zap.Any("msg", im),
					zap.Error(err),
					zap.Stringer("localID", s.messageCenterID),
					zap.String("localAddr", s.localAddr),
					zap.Stringer("remoteID", s.targetId),
					zap.String("remoteAddr", s.targetAddr))
				continue
			}
			msgBytes = append(msgBytes, buf)
		}
	}
	protoMsg := &proto.Message{
		From:     string(s.messageCenterID),
		To:       string(s.targetId),
		Topic:    string(msg[0].Topic),
		Type:     int32(msg[0].Type),
		Payload:  msgBytes,
		CreateAt: msg[0].CreateAt,
		Group:    msg[0].Group,
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
	s.updateStreamGauge()
}

func (s *remoteMessageTarget) getErr() error {
	select {
	case err := <-s.errCh:
		return err
	default:
		return nil
	}
}
