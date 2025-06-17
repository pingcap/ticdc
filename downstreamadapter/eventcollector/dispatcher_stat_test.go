package eventcollector

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
)

func TestGetResetTs(t *testing.T) {
	// Test case 1: lastEventCommitTs is greater than startTs
	// Expected: resetTs should be lastEventCommitTs - 1
	stat := &dispatcherStat{
		target: &mockDispatcher{},
	}
	stat.lastEventCommitTs.Store(100)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs := stat.getResetTs()
	if resetTs != 99 {
		t.Errorf("Expected resetTs to be 99, got %d", resetTs)
	}

	// Test case 2: lastEventCommitTs is equal to startTs
	// Expected: resetTs should be startTs
	stat.lastEventCommitTs.Store(50)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs = stat.getResetTs()
	if resetTs != 50 {
		t.Errorf("Expected resetTs to be 50, got %d", resetTs)
	}

	// Test case 3: lastEventCommitTs is less than startTs
	// Expected: resetTs should be startTs
	stat.lastEventCommitTs.Store(30)
	stat.target.(*mockDispatcher).startTs = 50
	resetTs = stat.getResetTs()
	if resetTs != 50 {
		t.Errorf("Expected resetTs to be 50, got %d", resetTs)
	}
}

// mockDispatcher implements the dispatcher.EventDispatcher interface for testing
type mockDispatcher struct {
	dispatcher.EventDispatcher
	startTs uint64
}

func (m *mockDispatcher) GetStartTs() uint64 {
	return m.startTs
}
