package eventstore

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func TestEventStoreCloseWaitsForWriteWorkers(t *testing.T) {
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	appcontext.SetService(appcontext.MessageCenter, messaging.NewMockMessageCenter())

	subClient := NewMockSubscriptionClient()
	store := New(t.TempDir(), subClient).(*eventStore)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- store.Run(context.Background())
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		store.runMu.Lock()
		cancelSet := store.runCancel != nil
		store.runMu.Unlock()
		if cancelSet {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("event store Run did not start in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	const eventCount = 128
	var callbackCount atomic.Int64
	for i := 0; i < eventCount; i++ {
		kvKey := []byte("k")
		kvVal := []byte("v")
		store.chs[0].Push(eventWithCallback{
			subID:   1,
			tableID: 1,
			kvs: []common.RawKVEntry{
				{
					OpType:   common.OpTypePut,
					StartTs:  1,
					CRTs:     uint64(100 + i),
					KeyLen:   uint32(len(kvKey)),
					ValueLen: uint32(len(kvVal)),
					Key:      kvKey,
					Value:    kvVal,
				},
			},
			currentResolvedTs: 0,
			callback: func() {
				callbackCount.Add(1)
			},
		})
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, store.Close(closeCtx))
	require.Equal(t, int64(eventCount), callbackCount.Load())

	err := <-runErrCh
	require.ErrorIs(t, err, context.Canceled)
}

