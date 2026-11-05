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

package eventcollector

import "github.com/pingcap/ticdc/pkg/node"

func setSessionState(
	session *dispatcherSession,
	currentEventServiceID node.ID,
	localReadyPending bool,
	pendingRemoteTarget node.ID,
) {
	session.connState.Lock()
	defer session.connState.Unlock()
	session.connState.currentEventServiceID = currentEventServiceID
	session.connState.localReadyPending = localReadyPending
	session.connState.pendingRemoteEventServiceID = pendingRemoteTarget
}

func setSessionRemoteCandidates(session *dispatcherSession, nodes []string) {
	session.connState.Lock()
	defer session.connState.Unlock()
	session.connState.remoteCandidates = nodes
}

func setSessionReadyCallback(session *dispatcherSession, readyCallback func()) {
	session.readyCallback = readyCallback
}

func markSessionRegistering(session *dispatcherSession, serverID node.ID) {
	session.beginRegister(serverID)
}

func markSessionReceiving(session *dispatcherSession, serverID node.ID) {
	session.connState.Lock()
	defer session.connState.Unlock()
	session.connState.currentEventServiceID = serverID
	session.connState.pendingRemoteEventServiceID = ""
	if serverID == session.localServerID {
		session.connState.localReadyPending = false
	}
}

func sessionState(session *dispatcherSession) (node.ID, bool, node.ID) {
	session.connState.RLock()
	defer session.connState.RUnlock()
	return session.connState.currentEventServiceID, session.connState.localReadyPending, session.connState.pendingRemoteEventServiceID
}
