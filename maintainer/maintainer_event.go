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

package maintainer

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
)

const (
	// EventInit initialize the changefeed maintainer
	EventInit = iota
	// EventMessage is triggered when a grpc message received
	EventMessage
	// EventPeriod is triggered periodically, maintainer handle some task in the loop, like resend messages
	EventPeriod
)

// Event identify the Event that maintainer will handle in event-driven loop
type Event struct {
	changefeedID common.ChangeFeedID
	eventType    int
	message      *messaging.TargetMessage
}
