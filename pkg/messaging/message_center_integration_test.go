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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var mockEpoch = uint64(1)

func setupMessageCenters(t *testing.T) (*messageCenter, *messageCenter, *messageCenter, func()) {
	mc1, mc1Addr, mc1Stop := NewMessageCenterForTest(t)
	mc2, mc2Addr, mc2Stop := NewMessageCenterForTest(t)
	mc3, mc3Addr, mc3Stop := NewMessageCenterForTest(t)

	mc1.addTarget(mc2.id, mc2Addr)
	mc1.addTarget(mc3.id, mc3Addr)
	mc2.addTarget(mc1.id, mc1Addr)
	mc2.addTarget(mc3.id, mc3Addr)
	mc3.addTarget(mc1.id, mc1Addr)
	mc3.addTarget(mc2.id, mc2Addr)

	cleanup := func() {
		mc1Stop()
		mc2Stop()
		mc3Stop()
	}

	return mc1, mc2, mc3, cleanup
}

func registerHandler(mc *messageCenter, topic string) chan *TargetMessage {
	ch := make(chan *TargetMessage, 1)
	mc.RegisterHandler(topic, func(ctx context.Context, msg *TargetMessage) error {
		ch <- msg
		log.Info(fmt.Sprintf("%s received message", mc.id), zap.Any("msg", msg))
		return nil
	})
	return ch
}

func waitForTargetsReady(mc *messageCenter) {
	// wait for all targets to be ready
	time.Sleep(time.Second)
	for {
		allReady := true
		for _, target := range mc.remoteTargets.m {
			if !target.isReadyToSend() {
				log.Info("target is not ready, retry it later", zap.String("target", target.targetId.String()))
				allReady = false
				break
			}
		}
		if allReady {
			log.Info("All targets are ready")
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func sendAndReceiveMessage(t *testing.T, sender *messageCenter, receiver *messageCenter, topic string, event *commonEvent.BatchDMLEvent) {
	targetMsg := NewSingleTargetMessage(receiver.id, topic, event)
	ch := make(chan *TargetMessage, 1)
	receiver.RegisterHandler(topic, func(ctx context.Context, msg *TargetMessage) error {
		ch <- msg
		return nil
	})

	timeoutCh := time.After(30 * time.Second)
	for {
		err := sender.SendEvent(targetMsg)
		require.NoError(t, err)
		select {
		case receivedMsg := <-ch:
			validateReceivedMessage(t, targetMsg, receivedMsg, sender.id, event)
			return
		case <-timeoutCh:
			t.Fatal("Timeout when sending message")
		default:
			log.Info("waiting for message, retry to send it later")
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func validateReceivedMessage(t *testing.T, targetMsg *TargetMessage, receivedMsg *TargetMessage, senderID node.ID, event *commonEvent.BatchDMLEvent) {
	require.Equal(t, targetMsg.To, receivedMsg.To)
	require.Equal(t, senderID, receivedMsg.From)
	require.Equal(t, targetMsg.Type, receivedMsg.Type)
	receivedEvent := receivedMsg.Message[0].(*commonEvent.BatchDMLEvent)
	receivedEvent.AssembleRows(event.TableInfo)
	require.Equal(t, event.Rows.ToString(event.TableInfo.GetFieldSlice()), receivedEvent.Rows.ToString(event.TableInfo.GetFieldSlice()))
}

func TestMessageCenterBasic(t *testing.T) {
	mc1, mc2, mc3, cleanup := setupMessageCenters(t)
	defer cleanup()

	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Job("create table t1(id int primary key, a int, b int, c int)")
	dml1 := helper.DML2Event("test", "t1", "insert into t1 values (1, 1, 1, 1)")
	dml2 := helper.DML2Event("test", "t1", "insert into t1 values (2, 2, 2, 2)")
	dml3 := helper.DML2Event("test", "t1", "insert into t1 values (3, 3, 3, 3)")

	topic1 := "topic1"
	topic2 := "topic2"
	topic3 := "topic3"

	registerHandler(mc1, topic1)
	registerHandler(mc2, topic2)
	registerHandler(mc3, topic3)

	time.Sleep(time.Second)
	waitForTargetsReady(mc1)
	waitForTargetsReady(mc2)
	waitForTargetsReady(mc3)

	// Case 1: Send a message from mc1 to mc1 (local message)
	sendAndReceiveMessage(t, mc1, mc1, topic1, event.BatchDML(dml1))
	log.Info("Pass test 1: send and receive local message")

	// Case 2: Send a message from mc1 to mc2 (remote message)
	sendAndReceiveMessage(t, mc1, mc2, topic2, event.BatchDML(dml2))
	log.Info("Pass test 2: send and receive remote message")

	// Case 3: Send a message from mc2 to mc3 (remote message)
	sendAndReceiveMessage(t, mc2, mc3, topic3, event.BatchDML(dml3))
	log.Info("Pass test 3: send and receive remote message")
}
