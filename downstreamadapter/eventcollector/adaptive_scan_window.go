package eventcollector

import (
	"sync"
	"time"

	"github.com/tikv/client-go/v2/oracle"
)

const (
	adaptiveScanWindowTickInterval = time.Second

	adaptiveScanWindowInitial = 5 * time.Second
	adaptiveScanWindowMin     = 1 * time.Second
	adaptiveScanWindowMax     = 30 * time.Minute

	adaptiveScanWindowAdjustAfter = 30 * time.Second

	adaptiveScanWindowShrinkThreshold = 1.10
	adaptiveScanWindowResetThreshold  = 1.50
	adaptiveScanWindowGrowThreshold   = 0.50
)

type adaptiveScanWindow struct {
	mu sync.Mutex

	interval time.Duration

	overShrinkFor time.Duration
	underGrowFor  time.Duration
}

func newAdaptiveScanWindow() *adaptiveScanWindow {
	return &adaptiveScanWindow{
		interval: adaptiveScanWindowInitial,
	}
}

func (w *adaptiveScanWindow) observe(memoryUsageRatio float64, maxInterval time.Duration) (interval time.Duration, needReset bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if maxInterval <= 0 {
		maxInterval = adaptiveScanWindowMin
	}
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

	if memoryUsageRatio > adaptiveScanWindowResetThreshold {
		w.overShrinkFor = 0
		w.underGrowFor = 0
		w.interval = adaptiveScanWindowMin
		return w.interval, true
	}

	if memoryUsageRatio > adaptiveScanWindowShrinkThreshold {
		w.overShrinkFor += adaptiveScanWindowTickInterval
	} else {
		w.overShrinkFor = 0
	}

	if memoryUsageRatio >= 0 && memoryUsageRatio < adaptiveScanWindowGrowThreshold {
		w.underGrowFor += adaptiveScanWindowTickInterval
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

	if w.interval > maxInterval {
		w.interval = maxInterval
	}
	if w.interval < adaptiveScanWindowMin {
		w.interval = adaptiveScanWindowMin
	}
	return w.interval, false
}

func calcScanMaxTs(scanLimitBaseTs uint64, interval time.Duration) uint64 {
	if scanLimitBaseTs == 0 {
		return 0
	}
	return oracle.GoTimeToTS(oracle.GetTimeFromTS(scanLimitBaseTs).Add(interval))
}
