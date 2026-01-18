package eventcollector

import (
	"sync"
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

const (
	adaptiveScanWindowInitial = 5 * time.Second
	adaptiveScanWindowMin     = 1 * time.Second
	adaptiveScanWindowMax     = 30 * time.Minute

	adaptiveScanWindowAdjustAfter = 10 * time.Second

	adaptiveScanWindowShrinkThreshold = 0.80
	adaptiveScanWindowGrowThreshold   = 0.50
)

type adaptiveScanWindow struct {
	mu sync.Mutex

	interval time.Duration

	lastObserveTime time.Time
	now             func() time.Time

	overShrinkFor time.Duration
	underGrowFor  time.Duration
}

func newAdaptiveScanWindow() *adaptiveScanWindow {
	now := time.Now
	return &adaptiveScanWindow{
		interval:        adaptiveScanWindowInitial,
		now:             now,
		lastObserveTime: now(),
	}
}

func (w *adaptiveScanWindow) observe(memoryUsageRatio float64, maxInterval time.Duration) (interval time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now
	if w.now != nil {
		now = w.now
	}
	currentTime := now()
	delta := currentTime.Sub(w.lastObserveTime)
	if delta < 0 {
		delta = 0
	}
	w.lastObserveTime = currentTime

	if maxInterval > adaptiveScanWindowMax {
		maxInterval = adaptiveScanWindowMax
	}
	if maxInterval < adaptiveScanWindowMin {
		maxInterval = adaptiveScanWindowMin
	}

	if w.interval <= 0 {
		w.interval = adaptiveScanWindowInitial
	}
	if w.interval < adaptiveScanWindowMin {
		w.interval = adaptiveScanWindowMin
	}
	if w.interval > maxInterval {
		w.interval = maxInterval
	}

	if memoryUsageRatio > adaptiveScanWindowShrinkThreshold {
		w.overShrinkFor += delta
	} else {
		w.overShrinkFor = 0
	}

	if memoryUsageRatio >= 0 && memoryUsageRatio < adaptiveScanWindowGrowThreshold {
		w.underGrowFor += delta
	} else {
		w.underGrowFor = 0
	}

	if w.overShrinkFor >= adaptiveScanWindowAdjustAfter {
		w.overShrinkFor = 0
		w.underGrowFor = 0
		w.interval = max(w.interval/2, adaptiveScanWindowMin)
	}

	if w.underGrowFor >= adaptiveScanWindowAdjustAfter {
		w.underGrowFor = 0
		w.overShrinkFor = 0
		w.interval = min(w.interval*2, maxInterval)
	}

	return w.interval
}

func calcScanMaxTs(scanLimitBaseTs uint64, interval time.Duration) uint64 {
	if scanLimitBaseTs == 0 {
		return 0
	}
	return oracle.GoTimeToTS(oracle.GetTimeFromTS(scanLimitBaseTs).Add(interval))
}
