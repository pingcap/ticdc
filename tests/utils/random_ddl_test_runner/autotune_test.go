package main

import (
	"testing"
	"time"
)

func TestAutoTuneStep_DegradeOnHardStall(t *testing.T) {
	res := autoTuneStep(10*time.Minute, 1.0, 64, 4, 128, 8, 2*time.Minute, 5*time.Minute)
	if !res.fail {
		t.Fatalf("expected fail on hard stall")
	}
}

func TestAutoTuneStep_DegradeDDLFirst(t *testing.T) {
	res := autoTuneStep(3*time.Minute, 1.0, 64, 4, 128, 8, 2*time.Minute, 5*time.Minute)
	if res.fail {
		t.Fatalf("unexpected fail")
	}
	if res.nextDDL != 3 {
		t.Fatalf("expected ddl decrease first, got %d", res.nextDDL)
	}
	if res.nextDML != 64 {
		t.Fatalf("expected dml unchanged, got %d", res.nextDML)
	}
}

func TestAutoTuneStep_DegradeDMLWhenDDLMin(t *testing.T) {
	res := autoTuneStep(3*time.Minute, 1.0, 16, 1, 128, 8, 2*time.Minute, 5*time.Minute)
	if res.fail {
		t.Fatalf("unexpected fail")
	}
	if res.nextDDL != 1 {
		t.Fatalf("expected ddl unchanged at min, got %d", res.nextDDL)
	}
	if res.nextDML >= 16 {
		t.Fatalf("expected dml decreased, got %d", res.nextDML)
	}
}

func TestAutoTuneStep_IncreaseWhenHealthy(t *testing.T) {
	res := autoTuneStep(10*time.Second, 0.9, 16, 1, 32, 4, 2*time.Minute, 5*time.Minute)
	if res.fail {
		t.Fatalf("unexpected fail")
	}
	if res.nextDML <= 16 {
		t.Fatalf("expected dml increased, got %d", res.nextDML)
	}
	if res.nextDDL != 2 {
		t.Fatalf("expected ddl increased, got %d", res.nextDDL)
	}
}
