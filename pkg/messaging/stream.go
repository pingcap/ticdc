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
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/messaging/proto"
)

// gRPC generates two different interfaces, MessageCenter_SendEventsServer
// and MessageCenter_SendCommandsServer.
// We use these two interfaces to unite them, to simplify the code.
type grpcStream interface {
	Send(*proto.Message) error
	Recv() (*proto.Message, error)
}

var streamGenerator atomic.Uint64

type streamWrapper struct {
	grpcStream
	id         uint64
	streamType string
}

// newStreamWrapper creates a new stream wrapper.
func newStreamWrapper(stream grpcStream, streamType string) *streamWrapper {
	return &streamWrapper{
		grpcStream: stream,
		id:         streamGenerator.Add(1),
		streamType: streamType,
	}
}

func (s *streamWrapper) Send(msg *proto.Message) error {
	return s.grpcStream.Send(msg)
}

func (s *streamWrapper) Recv() (*proto.Message, error) {
	return s.grpcStream.Recv()
}

func (s *streamWrapper) ID() uint64 {
	return s.id
}

func (s *streamWrapper) StreamType() string {
	return s.streamType
}

// Equals returns true if the two streams are the same.
func (s *streamWrapper) Equals(other *streamWrapper) bool {
	return s.id == other.id
}
