// Copyright 2022 PingCAP, Inc.
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

package causality

import (
	"math"
	"sort"
	"sync"

	"github.com/pingcap/log"
)

type slot struct {
	nodes map[uint64]*Node
	mu    sync.Mutex
}

type getSlotFunc func(hash uint64) uint64

// Slots implements slot-based conflict detection.
// It holds references to Node, which can be used to build
// a DAG of dependency.
type Slots struct {
	slots    []slot
	numSlots uint64
	getSlot  getSlotFunc
}

// NewSlots creates a new Slots.
func NewSlots(numSlots uint64) *Slots {
	if numSlots == 0 {
		log.Panic("causality slot count must be positive")
	}

	slots := make([]slot, numSlots)
	for i := uint64(0); i < numSlots; i++ {
		slots[i].nodes = make(map[uint64]*Node, 8)
	}
	return &Slots{
		slots:    slots,
		numSlots: numSlots,
		getSlot:  newGetSlotFunc(numSlots),
	}
}

// AllocNode allocates a new node and initializes it with the given hashes.
// TODO: reuse node if necessary. Currently it's impossible if async-notify is used.
// The reason is a node can step functions `assignTo`, `Remove`, `free`, then `assignTo`.
// again. In the last `assignTo`, it can never know whether the node has been reused
// or not.
func (s *Slots) AllocNode(hashes []uint64) *Node {
	return &Node{
		id:                  genNextNodeID(),
		sortedDedupKeysHash: sortHashes(hashes, s.getSlot),
		assignedTo:          unassigned,
	}
}

// Add adds an elem to the slots and calls DependOn for elem.
func (s *Slots) Add(elem *Node) {
	hashes := elem.sortedDedupKeysHash
	dependencyNodes := make(map[int64]*Node, len(hashes))

	var lastSlot uint64 = math.MaxUint64
	for _, hash := range hashes {
		// Lock slots in a stable order to avoid deadlock across concurrent txns.
		// The actual slot mapper is selected once in NewSlots:
		// power-of-two slot counts use bitmasking, other values fall back to modulo.
		slotIdx := s.getSlot(hash)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Lock()
			lastSlot = slotIdx
		}

		// If there is a node occpuied the same hash slot, we may have conflict with it.
		// Add the conflict node to the dependencyNodes.
		if prevNode, ok := s.slots[slotIdx].nodes[hash]; ok {
			prevID := prevNode.nodeID()
			// If there are multiple hashes conflicts with the same node, we only need to
			// depend on the node once.
			dependencyNodes[prevID] = prevNode
		}
		// Add this node to the slot, make sure new coming nodes with the same hash should
		// depend on this node.
		s.slots[slotIdx].nodes[hash] = elem
	}

	// Construct the dependency graph based on collected `dependencyNodes` and with corresponding
	// slots locked.
	elem.dependOn(dependencyNodes)

	// Lock those slots one by one and then unlock them one by one, so that
	// we can avoid 2 transactions get executed interleaved.
	lastSlot = math.MaxUint64
	for _, hash := range hashes {
		slotIdx := s.getSlot(hash)
		if lastSlot != slotIdx {
			s.slots[slotIdx].mu.Unlock()
			lastSlot = slotIdx
		}
	}
}

// Remove removes an element from the Slots.
func (s *Slots) Remove(elem *Node) {
	elem.remove()
	hashes := elem.sortedDedupKeysHash
	for _, hash := range hashes {
		slotIdx := s.getSlot(hash)
		s.slots[slotIdx].mu.Lock()
		// Remove the node from the slot.
		// If the node is not in the slot, it means the node has been replaced by new node with the same hash,
		// in this case we don't need to remove it from the slot.
		if tail, ok := s.slots[slotIdx].nodes[hash]; ok && tail.nodeID() == elem.nodeID() {
			delete(s.slots[slotIdx].nodes, hash)
		}
		s.slots[slotIdx].mu.Unlock()
	}
}

func newGetSlotFunc(numSlots uint64) getSlotFunc {
	if isPowerOfTwo(numSlots) {
		mask := numSlots - 1
		return func(hash uint64) uint64 {
			return getSlotByMask(hash, mask)
		}
	}

	return func(hash uint64) uint64 {
		return getSlotByModulo(hash, numSlots)
	}
}

func isPowerOfTwo(n uint64) bool {
	return n != 0 && n&(n-1) == 0
}

// getSlotByMask maps a hashed conflict key to one slot with a bitmask.
//
// This is only correct when the original slot count is a power of two, because
// only then does `hash & (numSlots-1)` equal `hash % numSlots`. We choose this
// version on the hot path for the fixed MySQL sink default because it is
// branchless and slightly cheaper than modulo.
func getSlotByMask(hash, numSlotsMask uint64) uint64 {
	return hash & numSlotsMask
}

// getSlotByModulo maps a hashed conflict key to one slot for arbitrary slot
// counts. It is the correctness fallback when the slot count is not a power of
// two.
func getSlotByModulo(hash, numSlots uint64) uint64 {
	return hash % numSlots
}

// Sort hashes by slot index to ensure all transactions lock slots in the same
// order and therefore cannot deadlock each other. The slot mapper is selected
// once when the Slots object is created, so sorting and later lock acquisition
// always use the same mapping rule.
func sortHashes(hashes []uint64, getSlot getSlotFunc) []uint64 {
	if len(hashes) == 0 {
		return nil
	}

	sort.Slice(hashes, func(i, j int) bool {
		return getSlot(hashes[i]) < getSlot(hashes[j])
	})

	return hashes
}
