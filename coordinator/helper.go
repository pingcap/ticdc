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
<<<<<<< HEAD
=======
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
>>>>>>> 7b68b7051 (metrics: show error time in changefeed error details panel (#5086))
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
<<<<<<< HEAD
=======

type changefeedErrorMetricLabels struct {
	keyspace   string
	changefeed string
	state      string
	errorTime  string
	code       string
	message    string
}

func isUnchangedRuntimeState(info *config.ChangeFeedInfo, state config.FeedState, err *config.RunningError) bool {
	if info == nil {
		return true
	}
	if info.State != state {
		return false
	}
	return sameRunningErrorSignature(info.Error, err)
}

func sameRunningErrorSignature(lhs *config.RunningError, rhs *config.RunningError) bool {
	if lhs == nil || rhs == nil {
		return lhs == rhs
	}
	return lhs.Addr == rhs.Addr &&
		lhs.Code == rhs.Code &&
		lhs.Message == rhs.Message
}

func (l changefeedErrorMetricLabels) labelValues() []string {
	return []string{l.keyspace, l.changefeed, l.state, l.errorTime, l.code, l.message}
}

func normalizeChangefeedErrorMetricMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	if len(message) <= changefeedErrorMetricMsgLimit {
		return message
	}
	return message[:changefeedErrorMetricMsgLimit-3] + "..."
}

func normalizeChangefeedErrorMetricTime(errorTime time.Time) string {
	// Keep the label stable across nodes with different local time zones while remaining
	// directly readable in Grafana's table view.
	if errorTime.IsZero() {
		return ""
	}
	return errorTime.UTC().Format(time.RFC3339)
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
		errorTime:  normalizeChangefeedErrorMetricTime(runningErr.Time),
		code:       runningErr.Code,
		message:    normalizeChangefeedErrorMetricMessage(runningErr.Message),
	}, true
}
>>>>>>> 7b68b7051 (metrics: show error time in changefeed error details panel (#5086))
