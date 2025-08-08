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

	"github.com/pingcap/log"
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
			err := handler(ctx, msg)
			if err != nil {
				log.Error("Handle message failed", zap.Error(err), zap.Any("msg", msg))
			}
		}
	}
}
