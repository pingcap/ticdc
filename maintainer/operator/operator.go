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

package operator

import (
	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
)

// Operator is the interface for the maintainer schedule dispatchers
type Operator interface {
	// ID returns the dispatcher ID
	ID() common.DispatcherID
	// Start is called when the operator is added to the operator executing queue
	Start()
	// Check checks when the new status comes, returns true if the operator is finished
	Check(from node.ID, status *heartbeatpb.TableSpanStatus)
	// IsFinished returns true if the operator is finished
	IsFinished() bool
	// PostFinished is called after the operator is finished and before remove from the task tracker
	PostFinished()
	// Schedule schedules this operator returns the message to be sent to the dispatcher
	Schedule() *messaging.TargetMessage
	// OnNodeRemove is called when node offline
	OnNodeRemove(node.ID)
	// OnTaskRemoved is called when the task is removed by ddl
	OnTaskRemoved()
	// String returns the string representation of the operator
	String() string
}
