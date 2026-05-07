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
	"strings"

	"github.com/pingcap/ticdc/pkg/config"
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

const changefeedErrorMetricMsgLimit = 256

type Event struct {
	eventType int
	message   *messaging.TargetMessage
}

type changefeedErrorMetricLabels struct {
	keyspace   string
	changefeed string
	state      string
	code       string
	message    string
}

func (l changefeedErrorMetricLabels) labelValues() []string {
	return []string{l.keyspace, l.changefeed, l.state, l.code, l.message}
}

func normalizeChangefeedErrorMetricMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	if len(message) <= changefeedErrorMetricMsgLimit {
		return message
	}
	return message[:changefeedErrorMetricMsgLimit-3] + "..."
}

func getChangefeedErrorMetricLabels(info *config.ChangeFeedInfo) (changefeedErrorMetricLabels, bool) {
	if info == nil {
		return changefeedErrorMetricLabels{}, false
	}
	if info.State != config.StateFailed && info.State != config.StateWarning {
		return changefeedErrorMetricLabels{}, false
	}

	runningErr := info.Error
	if runningErr == nil {
		runningErr = info.Warning
	}
	if runningErr == nil {
		return changefeedErrorMetricLabels{}, false
	}

	return changefeedErrorMetricLabels{
		keyspace:   info.ChangefeedID.Keyspace(),
		changefeed: info.ChangefeedID.Name(),
		state:      string(info.State),
		code:       runningErr.Code,
		message:    normalizeChangefeedErrorMetricMessage(runningErr.Message),
	}, true
}
