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

package context

import (
	"sync"
)

var (
	instance *AppContext
	once     sync.Once
)

const (
	MessageCenter           = "MessageCenter"
	EventCollector          = "EventCollector"
	HeartbeatCollector      = "HeartbeatCollector"
	SubscriptionClient      = "SubscriptionClient"
	SchemaStore             = "SchemaStore"
	EventStore              = "EventStore"
	EventService            = "EventService"
	DispatcherDynamicStream = "DispatcherDynamicStream"
	MaintainerManager       = "MaintainerManager"
)

// Put all the global instances here.
type AppContext struct {
	id         string
	serviceMap sync.Map
}

func GetGlobalContext() *AppContext {
	once.Do(func() {
		instance = &AppContext{
			// Initialize fields here
		}
	})
	return instance
}

func SetID(id string) { GetGlobalContext().id = id }
func GetID() string   { return GetGlobalContext().id }

func SetService[T any](name string, t T) { GetGlobalContext().serviceMap.Store(name, t) }
func GetService[T any](name string) T {
	v, _ := GetGlobalContext().serviceMap.Load(name)
	return v.(T)
}
