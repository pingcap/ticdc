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

import "time"

// sendThrottler helps to throttle message sending.
type sendThrottler struct {
	lastSendMessageTime time.Time
}

// newSendThrottler creates a new sendThrottler.
func newSendThrottler() sendThrottler {
	return sendThrottler{
		// Initialize to allow sending the first message immediately.
		lastSendMessageTime: time.Now().Add(-minSendMessageInterval),
	}
}

// shouldSend checks if a message should be sent. If so, it updates the last send time.
func (t *sendThrottler) shouldSend() bool {
	if time.Since(t.lastSendMessageTime) < minSendMessageInterval {
		return false
	}
	t.lastSendMessageTime = time.Now()
	return true
}

// reset allows sending the next message immediately.
func (t *sendThrottler) reset() {
	t.lastSendMessageTime = time.Now().Add(-minSendMessageInterval)
}
