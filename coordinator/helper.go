// Copyright 2024 PingCAP, Inc.
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

package coordinator

import (
	"github.com/pingcap/ticdc/pkg/messaging"
)

type ChangeType int

const (
	// ChangeStateAndTs indicate changefeed state and checkpointTs need update
	ChangeStateAndTs ChangeType = iota
	// ChangeTs indicate changefeed checkpointTs needs update
	ChangeTs
	// ChangeState indicate changefeed state needs update
	ChangeState
)

const (
	// EventMessage is triggered when a grpc message received
	EventMessage = iota
	// EventPeriod is triggered periodically, coordinator handle some task in the loop, like resend messages
	EventPeriod
)

type Event struct {
	eventType int
	message   *messaging.TargetMessage
}
