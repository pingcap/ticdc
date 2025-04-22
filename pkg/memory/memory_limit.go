package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// When the current memory limit is larger than ssthreshFactor * MaxMemoryLimit, the memory limiter will
	// start to increase in congestion avoidance phase.
	ssthreshFactor = 0.6
	// congestionThreshold is the number of memory limit being called Decrease in a row that will trigger congestion avoidance.
	congestionThreshold = 3
	// congestionDecreaseFactor is the factor to decrease the memory limit when congestion avoidance is triggered.
	congestionDecreaseFactor = 0.5
	// normalDecreaseFactor is the factor to decrease the memory limit when congestion avoidance is not triggered.
	normalDecreaseFactor = 0.8
)

// MemoryLimitConfig defines parameters for the memory flow control system,
// inspired by TCP congestion control principles.
type MemoryLimitConfig struct {
	// CurrentMemoryLimit is the current allowed memory usage rate
	CurrentMemoryLimit int
	// MinMemoryLimit represents the minimum memory usage rate, similar to TCP's minimum congestion window
	MinMemoryLimit int
	// MaxMemoryLimit represents the maximum memory usage rate, similar to TCP's receive window
	MaxMemoryLimit int
	// LinearIncreaseStep is the step to increase the memory limit when the current memory limit is larger than ssthreshFactor * MaxMemoryLimit
	LinearIncreaseStep int
	// MemoryLimitIncreaseRate defines the growth factor for slow start and congestion avoidance
	MemoryLimitIncreaseRate float64
	// IncreaseInterval sets how frequently the memory limit can increase
	IncreaseInterval time.Duration
	// PenaltyFactor should be set carefully, by the component that uses the memory limiter
	// because different components have different memory enlargement factors.
	PenaltyFactor float64
}

func NewMemoryLimitConfig(currentMemoryLimit int, minMemoryLimit int, maxMemoryLimit int, linearIncreaseStep int, memoryLimitIncreaseRate float64, increaseInterval time.Duration, penaltyFactor float64) *MemoryLimitConfig {
	return &MemoryLimitConfig{
		CurrentMemoryLimit:      currentMemoryLimit,
		MinMemoryLimit:          minMemoryLimit,
		MaxMemoryLimit:          maxMemoryLimit,
		LinearIncreaseStep:      linearIncreaseStep,
		MemoryLimitIncreaseRate: memoryLimitIncreaseRate,
		IncreaseInterval:        increaseInterval,
		PenaltyFactor:           penaltyFactor,
	}
}

// MemoryLimiter implements a TCP-inspired flow control system for memory usage.
// It regulates memory consumption using concepts like slow start, congestion avoidance,
// and fast recovery from TCP congestion control.
type MemoryLimiter struct {
	mu             sync.RWMutex
	role           string
	rateLimit      *rate.Limiter
	config         *MemoryLimitConfig
	started        atomic.Bool
	lastChangeTime time.Time

	congested         bool
	congestionCounter int
}

// NewMemoryLimiter creates a new memory limiter instance with the provided configuration.
// This initializes the system at the configured starting memory limit, similar to
// how TCP initializes its congestion window.
func NewMemoryLimiter(role string, config *MemoryLimitConfig) *MemoryLimiter {
	m := &MemoryLimiter{
		role:      role,
		config:    config,
		rateLimit: rate.NewLimiter(rate.Limit(config.CurrentMemoryLimit), config.CurrentMemoryLimit),
	}
	return m
}

// GetCurrentMemoryLimit returns the current memory consumption rate limit.
// This is analogous to checking TCP's current congestion window size.
func (m *MemoryLimiter) GetCurrentMemoryLimit() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.CurrentMemoryLimit
}

// penalty returns a factor to adjust memory consumption based on the system's state.
// It uses the configured PenaltyFactor and reduces it gradually over time when the
// memory limit has reached its maximum and has been stable for some time.
// This is similar to TCP's congestion response, but with gradual recovery.
// - Returns the configured penalty factor when memory is still growing or recently changed
// - Reduces the penalty factor the longer the system stays at maximum memory
// - Never returns a value less than 1 (which would promote memory usage)
func (m *MemoryLimiter) penalty() float64 {
	penalty := m.config.PenaltyFactor

	if m.config.CurrentMemoryLimit >= m.config.MaxMemoryLimit &&
		time.Since(m.lastChangeTime) >= 15*time.Second {
		switch {
		case time.Since(m.lastChangeTime) > 15*time.Second:
			penalty = penalty / 1.5
		case time.Since(m.lastChangeTime) > 30*time.Second:
			penalty = penalty / 2
		case time.Since(m.lastChangeTime) > 60*time.Second:
			penalty = penalty / 3
		case time.Since(m.lastChangeTime) > 90*time.Second:
			penalty = 1
		}
	}

	if penalty <= 1 {
		return 1
	}

	return float64(penalty)
}

// WaitN blocks until n tokens are available, adjusted by the current penalty factor.
// This implements the rate limiting similar to how TCP paces packet transmission
// based on congestion window size and network conditions.
func (m *MemoryLimiter) WaitN(n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	n = int(float64(n) * m.penalty())
	// TODO: use a real context
	m.rateLimit.WaitN(context.Background(), n)
}

// Start begins the memory limit adjustment process.
// This launches the background process that periodically increases the memory limit,
// similar to TCP's congestion avoidance phase.
func (m *MemoryLimiter) Start() {
	if m.started.CompareAndSwap(false, true) {
		log.Info("Start memory limiter", zap.String("role", m.role))
		go m.run()
	}
}

// Decrease immediately reduces the memory limit by decreaseFactor, but never below MinMemoryLimit.
// This is similar to how TCP cuts its congestion window in half upon packet loss.
func (m *MemoryLimiter) Decrease() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// If the memory limit is already at the minimum, do nothing.
	if m.config.CurrentMemoryLimit <= m.config.MinMemoryLimit {
		m.congested = false
		m.congestionCounter = 0
		return
	}

	m.congestionCounter++
	if m.congestionCounter >= congestionThreshold {
		m.congested = true
	}

	if m.congested {
		m.config.CurrentMemoryLimit = int(float64(m.config.CurrentMemoryLimit) * congestionDecreaseFactor)

		// reset the congestion counter and congested flag
		m.congestionCounter = 0
		m.congested = false
	} else {
		m.config.CurrentMemoryLimit = int(float64(m.config.CurrentMemoryLimit) * normalDecreaseFactor)
	}

	if m.config.CurrentMemoryLimit < m.config.MinMemoryLimit {
		m.config.CurrentMemoryLimit = m.config.MinMemoryLimit
	}

	m.rateLimit.SetLimit(rate.Limit(m.config.CurrentMemoryLimit))
	m.lastChangeTime = time.Now()
	log.Info("Decrease memory limit", zap.String("role", m.role), zap.Int("currentMemoryLimit", m.config.CurrentMemoryLimit))
}

// run is the background process that periodically increases the memory limit.
// This implements the TCP-like additive increase mechanism for congestion avoidance.
func (m *MemoryLimiter) run() {
	tick := time.NewTicker(m.config.IncreaseInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			m.increaseMemoryLimit()
		}
	}
}

// increaseMemoryLimit implements the additive increase part of TCP's AIMD algorithm.
// It gradually increases the memory limit by the configured rate, without exceeding
// the maximum limit. This is similar to TCP's congestion avoidance phase where
// the congestion window increases linearly.
func (m *MemoryLimiter) increaseMemoryLimit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.CurrentMemoryLimit >= m.config.MaxMemoryLimit {
		return
	}

	currentMemoryFactor := float64(m.config.CurrentMemoryLimit) / float64(m.config.MaxMemoryLimit)

	if currentMemoryFactor < ssthreshFactor {
		m.config.CurrentMemoryLimit = int(float64(m.config.CurrentMemoryLimit) * m.config.MemoryLimitIncreaseRate)
	} else {
		m.config.CurrentMemoryLimit = m.config.CurrentMemoryLimit + m.config.LinearIncreaseStep
	}

	if m.config.CurrentMemoryLimit > m.config.MaxMemoryLimit {
		m.config.CurrentMemoryLimit = m.config.MaxMemoryLimit
	}
	m.rateLimit.SetLimit(rate.Limit(m.config.CurrentMemoryLimit))
	m.lastChangeTime = time.Now()
	log.Info("Increase memory limit", zap.String("role", m.role), zap.Int("currentMemoryLimit", m.config.CurrentMemoryLimit))
}
