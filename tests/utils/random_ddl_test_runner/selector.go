package main

import (
	"math/rand"
	"sync"
)

type ddlSelector struct {
	mu         sync.Mutex
	windowSize int
	window     []string
	counts     map[string]int
	kinds      []ddlKind
}

func newDDLSelector(kinds []ddlKind, windowSize int) *ddlSelector {
	return &ddlSelector{
		windowSize: windowSize,
		counts:     make(map[string]int),
		kinds:      kinds,
	}
}

func (s *ddlSelector) pick(rng *rand.Rand) ddlKind {
	s.mu.Lock()
	defer s.mu.Unlock()

	weights := make([]float64, 0, len(s.kinds))
	var sum float64
	for _, k := range s.kinds {
		count := s.counts[k.name]
		w := k.baseWeight / float64(1+count)
		if w < 0.001 {
			w = 0.001
		}
		weights = append(weights, w)
		sum += w
	}
	x := rng.Float64() * sum
	var acc float64
	for i, w := range weights {
		acc += w
		if x <= acc {
			return s.kinds[i]
		}
	}
	return s.kinds[len(s.kinds)-1]
}

func (s *ddlSelector) record(kindName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.window = append(s.window, kindName)
	s.counts[kindName]++
	if len(s.window) <= s.windowSize {
		return
	}
	evicted := s.window[0]
	s.window = s.window[1:]
	s.counts[evicted]--
	if s.counts[evicted] <= 0 {
		delete(s.counts, evicted)
	}
}
