package nodeliveness

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

// State is the coordinator-derived view of a node's liveness.
//
// UNKNOWN is derived when a node's heartbeat exceeds the configured TTL after it has
// ever been observed via node liveness messages.
type State int

const (
	StateAlive State = iota
	StateDraining
	StateStopping
	StateUnknown
)

type record struct {
	lastSeen          time.Time
	nodeEpoch         uint64
	liveness          heartbeatpb.NodeLiveness
	everSeenHeartbeat bool
}

// View maintains an in-memory view of node-reported liveness.
//
// It is used by schedulers to filter destination nodes and by the drain controller
// to drive drain state transitions.
type View struct {
	mu   sync.RWMutex
	ttl  time.Duration
	data map[node.ID]*record
}

func NewView(ttl time.Duration) *View {
	return &View{
		ttl:  ttl,
		data: make(map[node.ID]*record),
	}
}

func (v *View) HandleNodeHeartbeat(id node.ID, hb *heartbeatpb.NodeHeartbeat, now time.Time) {
	if hb == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()

	r, ok := v.data[id]
	if !ok {
		r = &record{}
		v.data[id] = r
	}
	r.lastSeen = now
	r.nodeEpoch = hb.NodeEpoch
	r.liveness = hb.Liveness
	r.everSeenHeartbeat = true
}

func (v *View) HandleSetNodeLivenessResponse(id node.ID, resp *heartbeatpb.SetNodeLivenessResponse, now time.Time) {
	if resp == nil {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()

	r, ok := v.data[id]
	if !ok {
		r = &record{}
		v.data[id] = r
	}
	r.lastSeen = now
	r.nodeEpoch = resp.NodeEpoch
	r.liveness = resp.Applied
	r.everSeenHeartbeat = true
}

// GetState returns a derived liveness state for node id.
//
// Note: Nodes that have never sent node liveness messages are treated as ALIVE for
// compatibility during rollout.
func (v *View) GetState(id node.ID, now time.Time) State {
	var (
		lastSeen time.Time
		liveness heartbeatpb.NodeLiveness
		everSeen bool
	)
	v.mu.RLock()
	r := v.data[id]
	if r != nil {
		lastSeen = r.lastSeen
		liveness = r.liveness
		everSeen = r.everSeenHeartbeat
	}
	v.mu.RUnlock()

	if r == nil || !everSeen {
		return StateAlive
	}
	if v.ttl > 0 && now.Sub(lastSeen) > v.ttl {
		return StateUnknown
	}
	switch liveness {
	case heartbeatpb.NodeLiveness_DRAINING:
		return StateDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return StateStopping
	default:
		return StateAlive
	}
}

// GetNodeEpoch returns the last observed node epoch and whether it is known.
func (v *View) GetNodeEpoch(id node.ID) (uint64, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	r := v.data[id]
	if r == nil || !r.everSeenHeartbeat {
		return 0, false
	}
	return r.nodeEpoch, true
}

// FilterSchedulableDestNodes filters nodes that are eligible to be used as scheduling destinations.
//
// Only ALIVE nodes are eligible. DRAINING/STOPPING/UNKNOWN nodes are excluded.
// Nodes that have never sent node liveness messages are treated as ALIVE.
func (v *View) FilterSchedulableDestNodes(nodes map[node.ID]*node.Info, now time.Time) map[node.ID]*node.Info {
	if len(nodes) == 0 {
		return map[node.ID]*node.Info{}
	}
	out := make(map[node.ID]*node.Info, len(nodes))
	for id, info := range nodes {
		if v.GetState(id, now) == StateAlive {
			out[id] = info
		}
	}
	return out
}

// FilterSchedulableDestNodeIDs filters node IDs that are eligible to be used as scheduling destinations.
func (v *View) FilterSchedulableDestNodeIDs(ids []node.ID, now time.Time) []node.ID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]node.ID, 0, len(ids))
	for _, id := range ids {
		if v.GetState(id, now) == StateAlive {
			out = append(out, id)
		}
	}
	return out
}

// GetNodesByState returns node IDs whose derived state equals state.
func (v *View) GetNodesByState(state State, now time.Time) []node.ID {
	v.mu.RLock()
	defer v.mu.RUnlock()

	out := make([]node.ID, 0)
	for id, r := range v.data {
		if r == nil || !r.everSeenHeartbeat {
			continue
		}
		s := StateAlive
		if v.ttl > 0 && now.Sub(r.lastSeen) > v.ttl {
			s = StateUnknown
		} else {
			switch r.liveness {
			case heartbeatpb.NodeLiveness_DRAINING:
				s = StateDraining
			case heartbeatpb.NodeLiveness_STOPPING:
				s = StateStopping
			default:
				s = StateAlive
			}
		}
		if s == state {
			out = append(out, id)
		}
	}
	return out
}
