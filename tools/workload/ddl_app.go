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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"sync"

	"github.com/pingcap/errors"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

func (app *WorkloadApp) handleDDLExecution(wg *sync.WaitGroup) error {
	cfg, err := LoadDDLConfig(app.Config.DDLConfigPath)
	if err != nil {
		return err
	}

	runner, err := NewDDLRunner(app, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	plog.Info("start ddl workload",
		zap.String("mode", cfg.Mode),
		zap.Int("ddlWorker", app.Config.DDLWorker),
		zap.String("ddlTimeout", app.Config.DDLTimeout.String()))
	runner.Start(wg)
	return nil
}
