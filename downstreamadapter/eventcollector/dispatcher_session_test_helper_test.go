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
	session.connState.pendingRegisterTarget = pendingRemoteTarget
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
	session.connState.pendingRegisterTarget = ""
	if serverID == session.localServerID {
		session.connState.localReadyPending = false
	}
}

func sessionState(session *dispatcherSession) (node.ID, bool, node.ID) {
	session.connState.RLock()
	defer session.connState.RUnlock()
	return session.connState.currentEventServiceID, session.connState.localReadyPending, session.connState.pendingRegisterTarget
}
