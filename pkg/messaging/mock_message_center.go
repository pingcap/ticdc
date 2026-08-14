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

import "github.com/pingcap/ticdc/pkg/node"

var _ MessageCenter = &mockMessageCenter{}

// mockMessageCenter is a mock implementation of the MessageCenter interface
type mockMessageCenter struct {
	messageCh chan *TargetMessage
}

func NewMockMessageCenter() *mockMessageCenter {
	return &mockMessageCenter{
		messageCh: make(chan *TargetMessage, 100),
	}
}

func (m *mockMessageCenter) GetMessageChannel() chan *TargetMessage {
	return m.messageCh
}

func (m *mockMessageCenter) OnNodeChanges(nodeInfos map[node.ID]*node.Info) {
}

func (m *mockMessageCenter) SendEvent(event *TargetMessage) error {
	m.messageCh <- event
	return nil
}

func (m *mockMessageCenter) SendCommand(command *TargetMessage) error {
	m.messageCh <- command
	return nil
}

func (m *mockMessageCenter) RegisterHandler(topic string, handler MessageHandler) {
}

func (m *mockMessageCenter) DeRegisterHandler(topic string) {
}

func (m *mockMessageCenter) AddTarget(id node.ID, epoch uint64, addr string) {
}

func (m *mockMessageCenter) RemoveTarget(id node.ID) {
}

func (m *mockMessageCenter) Close() {
}

func (m *mockMessageCenter) IsReadyToSend(id node.ID) bool {
	return true
}
