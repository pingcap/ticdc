package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

// TestForwardBarrierEvent verifies the forwarding rules for barrier events against a span replication's
// checkpoint and block state. This matters because incorrect forwarding can either deadlock the barrier
// (never advancing) or violate ordering guarantees (advancing past an unflushed barrier).
// The key boundary is checkpointTs == commitTs, which must not be treated as "passed" except when
// ordering guarantees it (replication is blocked on a syncpoint at the same ts while the event is a DDL barrier).
func TestForwardBarrierEvent(t *testing.T) {
	makeReplication := func(t *testing.T, checkpointTs uint64, blockState *heartbeatpb.State) *replica.SpanReplication {
		t.Helper()

		span := &heartbeatpb.TableSpan{
			KeyspaceID: common.DefaultKeyspaceID,
			TableID:    1,
		}
		startKey, endKey, err := common.GetKeyspaceTableRange(span.KeyspaceID, span.TableID)
		require.NoError(t, err)
		span.StartKey = common.ToComparableKey(startKey)
		span.EndKey = common.ToComparableKey(endKey)

		cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme)
		r := replica.NewSpanReplication(
			cfID,
			common.NewDispatcherID(),
			1,
			span,
			checkpointTs,
			common.DefaultMode,
			false,
		)
		if blockState != nil {
			r.UpdateBlockState(*blockState)
		}
		return r
	}

	cases := []struct {
		name        string
		checkpoint  uint64
		blockState  *heartbeatpb.State
		event       *BarrierEvent
		shouldAllow bool
	}{
		{
			name:        "checkpoint beyond barrier",
			checkpoint:  101,
			blockState:  nil,
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: false},
			shouldAllow: true,
		},
		{
			name:        "checkpoint equals barrier without block state",
			checkpoint:  100,
			blockState:  nil,
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: false},
			shouldAllow: false,
		},
		{
			name:        "checkpoint before barrier without block state",
			checkpoint:  99,
			blockState:  nil,
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: false},
			shouldAllow: false,
		},
		{
			name:       "block state beyond barrier",
			checkpoint: 99,
			blockState: &heartbeatpb.State{
				BlockTs:     101,
				IsSyncPoint: false,
			},
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: false},
			shouldAllow: true,
		},
		{
			name:       "syncpoint at same ts forwards ddl barrier",
			checkpoint: 100,
			blockState: &heartbeatpb.State{
				BlockTs:     100,
				IsSyncPoint: true,
			},
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: false},
			shouldAllow: true,
		},
		{
			name:       "ddl at same ts does not forward syncpoint barrier",
			checkpoint: 100,
			blockState: &heartbeatpb.State{
				BlockTs:     100,
				IsSyncPoint: false,
			},
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: true},
			shouldAllow: false,
		},
		{
			name:       "syncpoint at same ts does not forward syncpoint barrier",
			checkpoint: 100,
			blockState: &heartbeatpb.State{
				BlockTs:     100,
				IsSyncPoint: true,
			},
			event:       &BarrierEvent{commitTs: 100, isSyncPoint: true},
			shouldAllow: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := makeReplication(t, tc.checkpoint, tc.blockState)
			require.Equal(t, tc.shouldAllow, forwardBarrierEvent(r, tc.event))
		})
	}
}
