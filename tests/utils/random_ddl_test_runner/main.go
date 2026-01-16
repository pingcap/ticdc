package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	var configPath string
	var phase string

	flag.StringVar(&configPath, "config", "", "path to runner config json")
	flag.StringVar(&phase, "phase", "", "bootstrap|workload")
	flag.Parse()

	if configPath == "" || phase == "" {
		_, _ = fmt.Fprintln(os.Stderr, "usage: random_ddl_test_runner --config <path> --phase <bootstrap|workload>")
		os.Exit(2)
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger, closeLogger, err := newRunnerLogger(cfg.Workdir)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	defer closeLogger()

	runner := newRunner(cfg, logger)

	switch phase {
	case "bootstrap":
		err = runner.bootstrap()
	case "workload":
		err = runner.workload()
	default:
		err = fmt.Errorf("unknown phase: %s", phase)
	}
	if err != nil {
		logger.Printf("runner failed: %v", err)
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
	logger.Printf("runner finished successfully")
}
