package nodeliveness

import (
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

// State is the coordinator-derived node liveness state.
// It extends the node-reported liveness with an UNKNOWN state based on heartbeat TTL.
type State int32

const (
	StateAlive State = iota
	StateDraining
	StateStopping
	StateUnknown
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "Alive"
	case StateDraining:
		return "Draining"
	case StateStopping:
		return "Stopping"
	case StateUnknown:
		return "Unknown"
	default:
		return "Unknown"
	}
}

// record stores the latest liveness observation of a node.
type record struct {
	lastSeen  time.Time
	nodeEpoch uint64
	liveness  heartbeatpb.NodeLiveness
}

// View maintains a best-effort in-memory view of node liveness derived from node heartbeats.
//
// Compatibility note:
// A node that has never been observed (no heartbeat/response) is treated as Alive and is never marked Unknown.
type View struct {
	mu    sync.RWMutex
	nodes map[node.ID]record
	ttl   time.Duration
}

func NewView(ttl time.Duration) *View {
	return &View{
		nodes: make(map[node.ID]record),
		ttl:   ttl,
	}
}

func (v *View) ObserveHeartbeat(nodeID node.ID, hb *heartbeatpb.NodeHeartbeat, now time.Time) {
	if hb == nil {
		return
	}

	v.mu.Lock()
	v.nodes[nodeID] = record{
		lastSeen:  now,
		nodeEpoch: hb.NodeEpoch,
		liveness:  hb.Liveness,
	}
	v.mu.Unlock()
}

func (v *View) ObserveSetNodeLivenessResponse(nodeID node.ID, resp *heartbeatpb.SetNodeLivenessResponse, now time.Time) {
	if resp == nil {
		return
	}

	v.mu.Lock()
	v.nodes[nodeID] = record{
		lastSeen:  now,
		nodeEpoch: resp.NodeEpoch,
		liveness:  resp.Applied,
	}
	v.mu.Unlock()
}

func (v *View) GetNodeEpoch(nodeID node.ID) (uint64, bool) {
	v.mu.RLock()
	r, ok := v.nodes[nodeID]
	v.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return r.nodeEpoch, true
}

func (v *View) GetState(nodeID node.ID) State {
	v.mu.RLock()
	r, ok := v.nodes[nodeID]
	v.mu.RUnlock()

	now := time.Now()
	if !ok {
		// Never observed: keep compatibility during rollout.
		return StateAlive
	}
	if now.Sub(r.lastSeen) > v.ttl {
		return StateUnknown
	}
	switch r.liveness {
	case heartbeatpb.NodeLiveness_ALIVE:
		return StateAlive
	case heartbeatpb.NodeLiveness_DRAINING:
		return StateDraining
	case heartbeatpb.NodeLiveness_STOPPING:
		return StateStopping
	default:
		return StateAlive
	}
}

// IsSchedulableDest returns true only when the node is eligible as a scheduling destination.
func (v *View) IsSchedulableDest(nodeID node.ID) bool {
	return v.GetState(nodeID) == StateAlive
}

func (v *View) GetDrainingOrStoppingNodes(now time.Time) []node.ID {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.nodes) == 0 {
		return nil
	}

	res := make([]node.ID, 0, len(v.nodes))
	for id, r := range v.nodes {
		if now.Sub(r.lastSeen) > v.ttl {
			continue
		}
		if r.liveness == heartbeatpb.NodeLiveness_DRAINING || r.liveness == heartbeatpb.NodeLiveness_STOPPING {
			res = append(res, id)
		}
	}
	return res
}
