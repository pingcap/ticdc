package common

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type MemoryLimitConfig struct {
	CurrentMemoryLimit      int
	MinMemoryLimit          int
	MaxMemoryLimit          int
	MemoryLimitIncreaseRate float64
	IncreaseInterval        time.Duration
}

type MemoryLimiter struct {
	mu        sync.RWMutex
	rateLimit *rate.Limiter
	config    *MemoryLimitConfig
}

func NewMemoryLimiter(config *MemoryLimitConfig) *MemoryLimiter {
	m := &MemoryLimiter{
		config:    config,
		rateLimit: rate.NewLimiter(rate.Limit(config.CurrentMemoryLimit), config.CurrentMemoryLimit),
	}
	go m.run()
	return m
}

func (m *MemoryLimiter) GetCurrentMemoryLimit() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.config.CurrentMemoryLimit
}

func (m *MemoryLimiter) WaitN(n int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// TODO: use a real context
	m.rateLimit.WaitN(context.Background(), n)
}

func (m *MemoryLimiter) Decrease() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.CurrentMemoryLimit <= m.config.MinMemoryLimit {
		return
	}
	m.config.CurrentMemoryLimit = int(float64(m.config.CurrentMemoryLimit) * 0.5)
	if m.config.CurrentMemoryLimit < m.config.MinMemoryLimit {
		m.config.CurrentMemoryLimit = m.config.MinMemoryLimit
	}
	m.rateLimit.SetLimit(rate.Limit(m.config.CurrentMemoryLimit))
	log.Info("Decrease memory limit", zap.Int("currentMemoryLimit", m.config.CurrentMemoryLimit))
}

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

func (m *MemoryLimiter) increaseMemoryLimit() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.CurrentMemoryLimit >= m.config.MaxMemoryLimit {
		return
	}
	m.config.CurrentMemoryLimit = int(float64(m.config.CurrentMemoryLimit) * m.config.MemoryLimitIncreaseRate)
	if m.config.CurrentMemoryLimit > m.config.MaxMemoryLimit {
		m.config.CurrentMemoryLimit = m.config.MaxMemoryLimit
	}
	m.rateLimit.SetLimit(rate.Limit(m.config.CurrentMemoryLimit))
	log.Info("Increase memory limit", zap.Int("currentMemoryLimit", m.config.CurrentMemoryLimit))
}
