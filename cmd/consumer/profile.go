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
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is served on 127.0.0.1 only and gated by --enable-profiling.
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// startProfileServer serves pprof on the loopback address until the process
// exits.
func startProfileServer() {
	server := &http.Server{
		Addr:              profileAddress,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warn("consumer profiling server failed", zap.Error(err))
		}
	}()
}
