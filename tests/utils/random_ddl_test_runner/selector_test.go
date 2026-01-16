package main

import (
	"math/rand"
	"testing"
)

func TestDDLSelector_CoverageDebtLikeBehavior(t *testing.T) {
	kinds := []ddlKind{
		{name: "a", baseWeight: 1},
		{name: "b", baseWeight: 1},
	}
	s := newDDLSelector(kinds, 100)
	for i := 0; i < 50; i++ {
		s.record("a")
	}
	rng := rand.New(rand.NewSource(1))
	var ca, cb int
	for i := 0; i < 1000; i++ {
		k := s.pick(rng)
		if k.name == "a" {
			ca++
		} else if k.name == "b" {
			cb++
		}
	}
	if cb <= ca {
		t.Fatalf("expected b to be selected more often than a, got a=%d b=%d", ca, cb)
	}
}
