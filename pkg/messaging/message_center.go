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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	messageCenterCheckInterval = time.Second * 1
)

// MessageCenter is the interface to send and receive messages to/from other targets.
// Note: Methods of MessageCenter and MessageSender are thread-safe.
// OnNodeChanges is not thread-safe, and should be called in the main thread of a server.
type MessageCenter interface {
	MessageSender
	MessageReceiver
	// OnNodeChanges is called when the nodes in the cluster are changed. The message center should update the target list.
	OnNodeChanges(map[node.ID]*node.Info)
	Close()
}

// MessageSender is the interface for sending messages to the target.
// The method in the interface should be thread-safe and non-blocking.
// If the message cannot be sent, the method will return an `ErrorTypeMessageCongested` error.
type MessageSender interface {
	SendEvent(msg *TargetMessage) error
	SendCommand(cmd *TargetMessage) error
	IsReadyToSend(target node.ID) bool
}

// MessageReceiver is the interface to receive messages from other targets.
type MessageReceiver interface {
	RegisterHandler(topic string, handler MessageHandler)
	DeRegisterHandler(topic string)
}

// messageCenter is the core of the messaging system.
// It hosts a local grpc server to receive messages (events and commands) from other targets (server).
// It hosts streaming channels to each other targets to send messages.
// Events and commands are sent by different channels.
//
// If the target is a remote server(the other process), the messages will be sent to the target by grpc streaming channel.
// If the target is the local (the same process), the messages will be sent to the local by golang channel directly.
//
// TODO: Currently, for each target, we only use one channel to send events.
// We might use multiple channels later.
type messageCenter struct {
	// The server id of the message center
	id       node.ID
	addr     string
	cfg      *config.MessageCenterConfig
	security *security.Credential
	// The local target, which is the message center itself.
	localTarget *localMessageTarget
	// The remote targets, which are the other message centers in remote servers.
	remoteTargets struct {
		sync.RWMutex
		m map[node.ID]*remoteMessageTarget
	}

	remoteNodeInfos map[node.ID]*node.Info
	notifyCh        chan map[node.ID]*node.Info

	grpcServer *grpc.Server
	router     *router

	// Messages from all targets are put into these channels.
	receiveEventCh chan *TargetMessage
	receiveCmdCh   chan *TargetMessage
	g              *errgroup.Group
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewMessageCenter(
	ctx context.Context,
	id node.ID,
	cfg *config.MessageCenterConfig,
	security *security.Credential,
) *messageCenter {
	receiveEventCh := make(chan *TargetMessage, cfg.CacheChannelSize)
	receiveCmdCh := make(chan *TargetMessage, cfg.CacheChannelSize)

	mc := &messageCenter{
		id:             id,
		addr:           cfg.Addr,
		cfg:            cfg,
		security:       security,
		localTarget:    newLocalMessageTarget(id, receiveEventCh, receiveCmdCh),
		receiveEventCh: receiveEventCh,
		receiveCmdCh:   receiveCmdCh,
		router:         newRouter(),
		notifyCh:       make(chan map[node.ID]*node.Info, 128),
	}
	mc.remoteTargets.m = make(map[node.ID]*remoteMessageTarget)

	log.Info("create message center success.",
		zap.Stringer("id", id), zap.String("addr", cfg.Addr))
	return mc
}

func (mc *messageCenter) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	mc.g = g
	mc.ctx = ctx
	mc.cancel = cancel

	mc.g.Go(func() error {
		mc.router.runDispatch(ctx, mc.receiveEventCh)
		return nil
	})

	mc.g.Go(func() error {
		mc.router.runDispatch(ctx, mc.receiveCmdCh)
		return nil
	})

	mc.g.Go(func() error {
		mc.updateMetrics(ctx)
		return nil
	})

	mc.g.Go(func() error {
		mc.checkRemoteTarget(ctx)
		return nil
	})

	log.Info("Start running message center", zap.Stringer("id", mc.id), zap.String("addr", mc.addr))
}

// checkRemoteTarget checks the remote targets and reset the connection if there is an error.
func (mc *messageCenter) checkRemoteTarget(ctx context.Context) {
	ticker := time.NewTicker(messageCenterCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.remoteTargets.RLock()
			for _, target := range mc.remoteTargets.m {
				if err := target.getErr(); err != nil {
					log.Warn("remote target error, reset the connection",
						zap.Stringer("localID", mc.id),
						zap.String("localAddr", mc.addr),
						zap.Stringer("remoteID", target.targetId),
						zap.String("remoteAddr", target.targetAddr),
						zap.Error(err))
					target.resetConnect()
				}
			}
			mc.remoteTargets.RUnlock()
		case activeNode := <-mc.notifyCh:
			mc.handleNodeChanges(activeNode)
		}
	}
}

func (mc *messageCenter) RegisterHandler(topic string, handler MessageHandler) {
	mc.router.registerHandler(topic, handler)
}

func (mc *messageCenter) DeRegisterHandler(topic string) {
	mc.router.deRegisterHandler(topic)
}

func (mc *messageCenter) OnNodeChanges(activeNode map[node.ID]*node.Info) {
	mc.notifyCh <- activeNode
	log.Info("notify node changes", zap.Any("activeNode", activeNode))
}

func (mc *messageCenter) handleNodeChanges(activeNode map[node.ID]*node.Info) {
	infosMap := make(map[node.ID]*node.Info)
	for id, node := range activeNode {
		infosMap[id] = node
	}
	mc.remoteNodeInfos = infosMap

	currentTargets := make(map[node.ID]bool)
	currentTargets[mc.id] = true

	mc.remoteTargets.RLock()
	for id := range mc.remoteTargets.m {
		currentTargets[id] = true
	}
	mc.remoteTargets.RUnlock()

	// Add the new targets that are not in the currentTargets
	for id, node := range infosMap {
		if _, ok := currentTargets[id]; !ok {
			mc.addTarget(node.ID, node.AdvertiseAddr)
		}
	}

	// Remove the targets that are not in the activeNode
	for id := range currentTargets {
		if _, ok := infosMap[id]; !ok {
			mc.removeTarget(id)
		}
	}
}

// AddTarget is called when a new remote target is discovered,
// to add the target to the message center.
func (mc *messageCenter) addTarget(id node.ID, addr string) {
	// If the target is the message center itself, we don't need to add it.
	if id == mc.id {
		log.Warn("Add a local target, ignore it", zap.Stringer("id", id), zap.String("addr", addr))
		return
	}
	mc.touchRemoteTarget(id, addr)
	log.Info("Add remote target", zap.Stringer("localID", mc.id), zap.Stringer("remoteID", id), zap.String("remoteAddr", addr))
}

func (mc *messageCenter) removeTarget(id node.ID) {
	mc.remoteTargets.Lock()
	defer mc.remoteTargets.Unlock()
	if target, ok := mc.remoteTargets.m[id]; ok {
		log.Info("remove remote target from message center",
			zap.Stringer("localID", mc.id),
			zap.String("localAddr", mc.addr),
			zap.Stringer("remoteID", id),
			zap.String("remoteAddr", target.targetAddr))
		target.close()
		delete(mc.remoteTargets.m, id)
	}
}

func (mc *messageCenter) IsReadyToSend(targetID node.ID) bool {
	if targetID == mc.id {
		return true
	}
	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()
	target, ok := mc.remoteTargets.m[targetID]
	if !ok {
		return false
	}
	return target.isReadyToSend()
}

func (mc *messageCenter) SendEvent(msg *TargetMessage) error {
	if msg == nil {
		return nil
	}

	if msg.To == mc.id {
		return mc.localTarget.sendEvent(msg)
	}

	mc.remoteTargets.RLock()
	target, ok := mc.remoteTargets.m[msg.To]
	mc.remoteTargets.RUnlock()
	if !ok {
		// If target not found, there are two cases:
		// 1. The target is not discovered yet.
		// 2. The target is removed.
		// The caller should handle the error correctly.
		// For example, if the target is not discovered yet, the caller can retry later.
		// If the target is removed, the caller must remove the objects that was sending
		// message to this target to avoid blocking.
		return apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", msg.To)}
	}
	return target.sendEvent(msg)
}

func (mc *messageCenter) SendCommand(msg *TargetMessage) error {
	if msg == nil {
		return nil
	}

	if msg.To == mc.id {
		return mc.localTarget.sendCommand(msg)
	}

	mc.remoteTargets.RLock()
	target, ok := mc.remoteTargets.m[msg.To]
	mc.remoteTargets.RUnlock()
	if !ok {
		return errors.WithStack(apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %v not found", msg.To.String())})
	}
	return target.sendCommand(msg)
}

// Close stops the grpc server and stops all the connections to the remote targets.
func (mc *messageCenter) Close() {
	log.Info("message center is closing", zap.Stringer("id", mc.id), zap.String("addr", mc.addr))
	mc.remoteTargets.RLock()
	defer mc.remoteTargets.RUnlock()

	for _, target := range mc.remoteTargets.m {
		target.close()
	}

	mc.cancel()
	if mc.grpcServer != nil {
		mc.grpcServer.Stop()
	}

	mc.grpcServer = nil
	_ = mc.g.Wait()
	log.Info("message center is closed", zap.Stringer("id", mc.id))
}

// touchRemoteTarget returns the remote target by the id,
// if the target is not found, it will create a new one.
func (mc *messageCenter) touchRemoteTarget(id node.ID, targetAddr string) {
	mc.remoteTargets.RLock()
	target, ok := mc.remoteTargets.m[id]
	mc.remoteTargets.RUnlock()
	if !ok {
		// If the target is not found, create a new one.
		target = newRemoteMessageTarget(
			mc.ctx,
			mc.id, id,
			mc.addr, targetAddr,
			mc.receiveEventCh,
			mc.receiveCmdCh,
			mc.cfg,
			mc.security)
		mc.remoteTargets.Lock()
		mc.remoteTargets.m[id] = target
		mc.remoteTargets.Unlock()
		return
	}

	log.Info("Remote target changed, creating a new one",
		zap.Stringer("localID", mc.id),
		zap.String("localAddr", mc.addr),
		zap.Stringer("remoteID", id),
		zap.String("oldAddr", target.targetAddr),
		zap.String("newAddr", targetAddr))

	target.close()
	newTarget := newRemoteMessageTarget(
		mc.ctx,
		mc.id, id,
		mc.addr, targetAddr,
		mc.receiveEventCh,
		mc.receiveCmdCh,
		mc.cfg,
		mc.security)

	mc.remoteTargets.Lock()
	mc.remoteTargets.m[id] = newTarget
	mc.remoteTargets.Unlock()
}

func (mc *messageCenter) updateMetrics(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	commandChLen := metrics.MessagingReceiveChannelLength.WithLabelValues("command")
	eventChLen := metrics.MessagingReceiveChannelLength.WithLabelValues("event")
	for {
		select {
		case <-ticker.C:
			commandChLen.Set(float64(len(mc.receiveCmdCh)))
			eventChLen.Set(float64(len(mc.receiveEventCh)))
		case <-ctx.Done():
			return
		}
	}
}

// grpcServer implements the gRPC `service MessageCenter` defined in the proto file
// It handles the gRPC requests from the clients,
// and then calls the methods in MessageCenter struct to handle the requests.
type grpcServer struct {
	proto.UnimplementedMessageServiceServer
	messageCenter *messageCenter
}

func NewMessageCenterServer(mc MessageCenter) proto.MessageServiceServer {
	return &grpcServer{messageCenter: mc.(*messageCenter)}
}

func (s *grpcServer) StreamMessages(stream proto.MessageService_StreamMessagesServer) error {
	return s.handleConnect(stream)
}

func (s *grpcServer) id() node.ID {
	return s.messageCenter.id
}

// handleConnect registers the client as a target in the message center.
// So the message center can receive messages from the client.
func (s *grpcServer) handleConnect(stream proto.MessageService_StreamMessagesServer) error {
	// The first message is an handshake message.
	msg, err := stream.Recv()
	if err != nil {
		return err
	}

	if msg.Type != int32(TypeMessageHandShake) {
		log.Panic("Received unexpected message type, should be handshake",
			zap.Stringer("localID", s.messageCenter.id),
			zap.String("localAddr", s.messageCenter.addr),
			zap.String("remoteID", msg.From),
			zap.Int32("type", msg.Type))
	}

	handshake := &HandshakeMessage{}
	if err := handshake.Unmarshal(msg.Payload[0]); err != nil {
		log.Panic("failed to unmarshal handshake message", zap.Error(err))
	}

	to := node.ID(msg.To)
	if to != s.id() {
		err := apperror.AppError{Type: apperror.ErrorTypeTargetMismatch, Reason: fmt.Sprintf("The receiver %s not match with the message center id %s", to, s.id())}
		log.Error("Target mismatch", zap.Error(err))
		return err
	}

	targetId := node.ID(msg.From)
	var target *remoteMessageTarget
	var ok bool

	// Wait a while for the node to be discovered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err = retry.Do(ctx, func() error {
		s.messageCenter.remoteTargets.RLock()
		target, ok = s.messageCenter.remoteTargets.m[targetId]
		s.messageCenter.remoteTargets.RUnlock()

		if !ok {
			log.Info("Remote target not found",
				zap.Stringer("localID", s.messageCenter.id),
				zap.String("localAddr", s.messageCenter.addr),
				zap.Stringer("remoteID", targetId))
			err := &apperror.AppError{Type: apperror.ErrorTypeTargetNotFound, Reason: fmt.Sprintf("Target %s not found", targetId)}
			return err
		}
		return nil
	}, retry.WithIsRetryableErr(func(err error) bool {
		if err == context.DeadlineExceeded {
			return false
		}
		return true
	}), retry.WithMaxTries(10))
	if err != nil {
		log.Error("Failed to get remote target", zap.Error(err))
		return err
	}

	log.Info("Start to receive messages from remote target",
		zap.String("streamType", handshake.StreamType),
		zap.Stringer("localID", s.messageCenter.id),
		zap.String("localAddr", s.messageCenter.addr),
		zap.Stringer("remoteID", targetId),
		zap.String("remoteAddr", target.targetAddr))

	return target.handleIncomingStream(stream, handshake)
}

type HandshakeMessage struct {
	Version    byte
	Timestamp  int64
	StreamType string
}

func (h *HandshakeMessage) Marshal() ([]byte, error) {
	return json.Marshal(h)
}

func (h *HandshakeMessage) Unmarshal(data []byte) error {
	return json.Unmarshal(data, h)
}
