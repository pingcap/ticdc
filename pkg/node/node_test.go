// Copyright 2026 PingCAP, Inc.
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

package node

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMessagingProtocolVersion(t *testing.T) {
	current := NewInfo("127.0.0.1:8300", "")
	require.Equal(t, CurrentMessagingProtocolVersion, current.MessagingProtocolVersion)

	legacy := CaptureInfoToNodeInfo(&config.CaptureInfo{
		ID: config.CaptureID("legacy-node"),
	})
	require.Zero(t, legacy.MessagingProtocolVersion)

	capable := CaptureInfoToNodeInfo(&config.CaptureInfo{
		ID:                       config.CaptureID("capable-node"),
		MessagingProtocolVersion: CurrentMessagingProtocolVersion,
	})
	require.Equal(t, CurrentMessagingProtocolVersion, capable.MessagingProtocolVersion)
}
