// Copyright 2025 PingCAP, Inc.
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

package messaging

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/metrics"
	"go.uber.org/zap"
)

type MessageHandler func(ctx context.Context, msg *TargetMessage) error

type router struct {
	mu       sync.RWMutex
	handlers map[string]MessageHandler
}

func newRouter() *router {
	return &router{
		handlers: make(map[string]MessageHandler),
	}
}

func (r *router) registerHandler(topic string, handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[topic] = handler
}

func (r *router) deRegisterHandler(topic string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, topic)
}

func (r *router) runDispatch(ctx context.Context, out <-chan *TargetMessage) {
	lastSlowLogTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			log.Info("router: close, since context done")
			return
		case msg := <-out:
			r.mu.RLock()
			handler, ok := r.handlers[msg.Topic]
			r.mu.RUnlock()
			if !ok {
				log.Debug("no handler for message, drop it", zap.Any("msg", msg))
				continue
			}
			start := time.Now()
			err := handler(ctx, msg)
			now := time.Now()
			if now.Sub(start) > 100*time.Millisecond {
				// Rate limit logging: only log once every 10 seconds
				if now.Sub(lastSlowLogTime) >= 10*time.Second {
					lastSlowLogTime = now
					log.Warn("slow message handling detected",
						zap.String("topic", msg.Topic),
						zap.String("type", msg.Type.String()),
						zap.Duration("duration", now.Sub(start)),
						zap.String("from", msg.From.String()))
				}

				// Always increment metrics counter for slow message handling
				metrics.MessagingSlowHandleCounter.WithLabelValues(msg.Type.String()).Inc()
			}
			if err != nil {
				log.Error("Handle message failed", zap.Error(err), zap.Any("msg", msg))
			}
		}
	}
}
