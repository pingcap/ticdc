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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixedSizeEventsRejectShortPayloads(t *testing.T) {
	tests := []struct {
		name      string
		eventType int
		version   int
		unmarshal func([]byte) error
	}{
		{
			name: "resolved", eventType: TypeResolvedEvent, version: ResolvedEventVersion1,
			unmarshal: func(data []byte) error { return new(ResolvedEvent).Unmarshal(data) },
		},
		{
			name: "handshake", eventType: TypeHandshakeEvent, version: HandshakeEventVersion,
			unmarshal: func(data []byte) error { return new(HandshakeEvent).Unmarshal(data) },
		},
		{
			name: "sync point", eventType: TypeSyncPointEvent, version: SyncPointEventVersion1,
			unmarshal: func(data []byte) error { return new(SyncPointEvent).Unmarshal(data) },
		},
		{
			name: "not reusable", eventType: TypeNotReusableEvent, version: NotReusableEventVersion,
			unmarshal: func(data []byte) error { return new(NotReusableEvent).Unmarshal(data) },
		},
		{
			name: "ready", eventType: TypeReadyEvent, version: ReadyEventVersion1,
			unmarshal: func(data []byte) error { return new(ReadyEvent).Unmarshal(data) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := MarshalEventWithHeader(test.eventType, test.version, nil)
			require.NoError(t, err)
			require.Error(t, test.unmarshal(data))
		})
	}
}
