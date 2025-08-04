// Copyright 2024 PingCAP, Inc.
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
	"log"
	"net/http"
	_ "net/http/pprof"

	_ "github.com/go-sql-driver/mysql"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
)

func main() {
	// start pprof server
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// create config object and parse command line arguments
	config := NewWorkloadConfig()
	if err := config.ParseFlags(); err != nil {
		plog.Panic("Failed to parse flags", zap.Error(err))
	}

	// create application object
	app := NewWorkloadApp(config)

	// initialize application
	if err := app.Initialize(); err != nil {
		plog.Panic("Failed to initialize application", zap.Error(err))
	}

	// start metrics reporting
	app.StartMetricsReporting()

	// execute workload
	if err := app.Execute(); err != nil {
		plog.Error("Error executing workload", zap.Error(err))
	}
}
