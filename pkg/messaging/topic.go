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

// A topic identifies a message target.
const (
	// EventServiceTopic is the topic of the event service.
	EventServiceTopic = "EventServiceTopic"
	// EventStoreTopic is the topic of the event store
	EventStoreTopic = "event-store"
	// LogCoordinatorTopic is the topic of the log coordinator
	LogCoordinatorTopic = "log-coordinator"
	// EventCollectorTopic is the topic of the event collector.
	EventCollectorTopic = "event-collector"
	// CoordinatorTopic is the topic of the coordinator.
	CoordinatorTopic = "coordinator"
	// MaintainerManagerTopic is the topic of the maintainer manager.
	MaintainerManagerTopic = "maintainer-manager"
	// MaintainerTopic is the topic of the maintainer.
	MaintainerTopic = "maintainer"
	// HeartbeatCollectorTopic is the topic of the heartbeat collector.
	HeartbeatCollectorTopic = "heartbeat-collector"
	// DispatcherManagerTopic is the topic of the dispatcher manager.
	DispatcherManagerManagerTopic = "dispatcher-manager-manager"
)
