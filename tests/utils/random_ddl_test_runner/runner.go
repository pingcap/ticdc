package main

import (
	"log"
)

type runner struct {
	cfg    *config
	logger *log.Logger
}

func newRunner(cfg *config, logger *log.Logger) *runner {
	return &runner{
		cfg:    cfg,
		logger: logger,
	}
}
