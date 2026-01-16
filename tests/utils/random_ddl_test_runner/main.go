package main

import (
	"flag"
	"fmt"
	"os"
)

// Command random_ddl_test_runner is a deterministic workload generator and verifier
// for TiCDC integration tests.
//
// It has two phases:
//   - bootstrap: initialize identical schemas and deterministic seed data on both upstream and downstream.
//   - workload: run concurrent random DML/DDL against upstream, while monitoring replication health.
//
// Typical end-to-end flow (driven by tests/integration_tests/*/run.sh):
//
// Components:
//
//	H: integration run.sh (harness)
//	R: random_ddl_test_runner (this binary)
//	U: upstream TiDB
//	C: TiCDC capture(s)
//	S: sink consumer for MQ/file sinks (optional)
//	D: downstream TiDB
//	DF: sync_diff_inspector
//
// Sequence (simplified):
//
//	H -> U,D: start_tidb_cluster
//	H -> R: bootstrap
//	R -> U,D: create db/table + insert deterministic rows
//	H -> C: start capture(s)
//	H -> C: create changefeed (sink uri)
//	H -> S: start consumer (optional)
//	H -> R: workload
//	R -> U: random DML/DDL + motif DDL sequence
//	R -> C: poll changefeed status (checkpoint/state) and auto-tune workload
//	R -> U: insert finish_mark (replication barrier)
//	R -> D: wait finish_mark visible (catch up)
//	R -> H: write diff_config.toml
//	H -> DF: final upstream vs downstream diff
//
// The runner writes artifacts into <workdir>, including:
//   - runner.log / ddl_trace.log: runner-side logs and executed DDL traces.
//   - runner_config.snapshot.json: the effective config used for this run.
//   - diff_config.toml: final sync_diff_inspector config for the end-to-end diff.
//   - diff_config_syncpoint_<ts>.toml: optional syncpoint snapshot diff configs (MySQL sink).
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
