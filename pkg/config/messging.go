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

package config

const (
	// size of channel to cache the messages to be sent and received
	defaultCacheSize = 1024 * 16 // 16K messages
)

type MessageCenterConfig struct {
	// The size of the channel for pending messages to be sent and received.
	CacheChannelSize int
}

func NewDefaultMessageCenterConfig() *MessageCenterConfig {
	return &MessageCenterConfig{
		CacheChannelSize: defaultCacheSize,
	}
}
