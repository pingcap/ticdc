package main

import (
	"log"
)

type runner struct {
	cfg    *config
	logger *log.Logger
}

func newRunner(cfg *config, logger *log.Logger) *runner {
	// runner is a thin orchestrator. Heavy logic lives in bootstrap.go/workload.go.
	return &runner{
		cfg:    cfg,
		logger: logger,
	}
}
