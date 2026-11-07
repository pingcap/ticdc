// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"net/http"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

func newProfileServer(enabled bool) *http.Server {
	if !enabled {
		return nil
	}
	return &http.Server{
		Addr:              profileAddress,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func (c *consumer) serveProfile() error {
	if err := c.profileServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("consumer profiling server failed", zap.Error(err))
		return err
	}
	return nil
}

func (c *consumer) stopProfile(ctx context.Context) error {
	<-ctx.Done()
	if err := c.profileServer.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Warn("consumer profiling server close failed", zap.Error(err))
	}
	return nil
}
