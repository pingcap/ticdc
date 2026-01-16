package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type mysqlConnConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func (c mysqlConnConfig) dsn(database string) string {
	// Keep DSN ASCII-only and deterministic.
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&multiStatements=true&interpolateParams=true",
		c.User, c.Password, c.Host, c.Port, database)
}

type cdcAPIConfig struct {
	Addr     string `json:"addr"`
	User     string `json:"user"`
	Password string `json:"password"`
	Keyspace string `json:"keyspace"`
	// ChangefeedID is required for the workload phase.
	ChangefeedID string `json:"changefeed_id"`
}

type failoverConfig struct {
	Enabled          bool     `json:"enabled"`
	CaptureAddrs     []string `json:"capture_addrs"`
	CdcBinary        string   `json:"cdc_binary"`
	MinInterval      duration `json:"min_interval"`
	MaxInterval      duration `json:"max_interval"`
	GatedProbability float64  `json:"gated_probability"`
}

type dmlConfig struct {
	MaxWorkers          int      `json:"max_workers"`
	InitialWorkers      int      `json:"initial_workers"`
	HotspotRatio        float64  `json:"hotspot_ratio"`
	HotTableRatio       float64  `json:"hot_table_ratio"`
	BigTxnEnabled       bool     `json:"big_txn_enabled"`
	BigTxnInterval      duration `json:"big_txn_interval"`
	BigTxnRowsMin       int      `json:"big_txn_rows_min"`
	BigTxnRowsMax       int      `json:"big_txn_rows_max"`
	KeyConflictEnabled  bool     `json:"key_conflict_enabled"`
	KeyConflictKeyspace int      `json:"key_conflict_keyspace"`
}

type ddlConfig struct {
	MaxWorkers     int `json:"max_workers"`
	InitialWorkers int `json:"initial_workers"`
}

type verifyConfig struct {
	HealthInterval   duration `json:"health_interval"`
	NoAdvanceSoft    duration `json:"no_advance_soft"`
	NoAdvanceHard    duration `json:"no_advance_hard"`
	LogScanEnabled   bool     `json:"log_scan_enabled"`
	PanicPatterns    []string `json:"panic_patterns"`
	FailOnPanicMatch bool     `json:"fail_on_panic_match"`
	ConvergeWait     duration `json:"converge_wait"`
}

type mysqlSyncpointConfig struct {
	Enabled            bool     `json:"enabled"`
	DiffInterval       duration `json:"diff_interval"`
	MaxDiffChecks      int      `json:"max_diff_checks"`
	UpstreamStatusHost string   `json:"upstream_status_host"`
	UpstreamStatusPort int      `json:"upstream_status_port"`
}

type config struct {
	Workdir  string   `json:"workdir"`
	Profile  string   `json:"profile"`
	Seed     int64    `json:"seed"`
	Duration duration `json:"duration"`

	Upstream   mysqlConnConfig `json:"upstream"`
	Downstream mysqlConnConfig `json:"downstream"`

	CDC      cdcAPIConfig         `json:"cdc"`
	SinkType string               `json:"sink_type"`
	Failover failoverConfig       `json:"failover"`
	DML      dmlConfig            `json:"dml"`
	DDL      ddlConfig            `json:"ddl"`
	Verify   verifyConfig         `json:"verify"`
	MySQL    mysqlSyncpointConfig `json:"mysql"`

	Bootstrap struct {
		DBCount            int `json:"db_count"`
		TablesPerDB        int `json:"tables_per_db"`
		BaseRowsPerTable   int `json:"base_rows_per_table"`
		SplitRowsPerTable  int `json:"split_rows_per_table"`
		FrozenRowsPerTable int `json:"frozen_rows_per_table"`
	} `json:"bootstrap"`
}

func loadConfig(path string) (*config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *config) applyDefaultsAndValidate() error {
	// Defaults should keep the smoke profile fast while still exercising key paths.
	// "weekly" uses larger concurrency / data volumes to increase coverage.
	if c.Workdir == "" {
		return fmt.Errorf("workdir is required")
	}
	if c.Seed == 0 {
		// Allow explicit 0 but require deterministic default.
		c.Seed = 1
	}
	if c.Duration.Duration <= 0 {
		c.Duration.Duration = 3 * time.Minute
	}
	if c.Profile == "" {
		c.Profile = "smoke"
	}

	if c.Upstream.Host == "" {
		c.Upstream.Host = "127.0.0.1"
	}
	if c.Upstream.Port == 0 {
		c.Upstream.Port = 4000
	}
	if c.Upstream.User == "" {
		c.Upstream.User = "root"
	}

	if c.Downstream.Host == "" {
		c.Downstream.Host = "127.0.0.1"
	}
	if c.Downstream.Port == 0 {
		c.Downstream.Port = 3306
	}
	if c.Downstream.User == "" {
		c.Downstream.User = "root"
	}

	if c.Bootstrap.DBCount == 0 {
		c.Bootstrap.DBCount = 5
	}
	if c.Bootstrap.TablesPerDB == 0 {
		c.Bootstrap.TablesPerDB = 20
	}
	if c.Bootstrap.BaseRowsPerTable == 0 {
		if c.Profile == "weekly" {
			c.Bootstrap.BaseRowsPerTable = 1000
		} else {
			c.Bootstrap.BaseRowsPerTable = 100
		}
	}
	if c.Bootstrap.SplitRowsPerTable == 0 {
		if c.Profile == "weekly" {
			c.Bootstrap.SplitRowsPerTable = 5000
		} else {
			c.Bootstrap.SplitRowsPerTable = 500
		}
	}
	if c.Bootstrap.FrozenRowsPerTable == 0 {
		c.Bootstrap.FrozenRowsPerTable = 50
	}

	if c.DML.MaxWorkers == 0 {
		if c.Profile == "weekly" {
			c.DML.MaxWorkers = 128
		} else {
			c.DML.MaxWorkers = 32
		}
	}
	if c.DML.InitialWorkers == 0 {
		c.DML.InitialWorkers = c.DML.MaxWorkers / 2
		if c.DML.InitialWorkers < 1 {
			c.DML.InitialWorkers = 1
		}
	}
	if c.DML.HotspotRatio == 0 {
		c.DML.HotspotRatio = 0.8
	}
	if c.DML.HotTableRatio == 0 {
		c.DML.HotTableRatio = 0.1
	}
	if c.DML.BigTxnInterval.Duration == 0 {
		c.DML.BigTxnInterval.Duration = 20 * time.Second
	}
	// Keep these enabled by default to ensure the workload includes the key motifs from the design doc.
	c.DML.BigTxnEnabled = true
	c.DML.KeyConflictEnabled = true
	if c.DML.BigTxnRowsMin == 0 {
		c.DML.BigTxnRowsMin = 200
	}
	if c.DML.BigTxnRowsMax == 0 {
		c.DML.BigTxnRowsMax = 400
	}
	if c.DML.KeyConflictKeyspace == 0 {
		c.DML.KeyConflictKeyspace = 1024
	}

	if c.DDL.MaxWorkers == 0 {
		if c.Profile == "weekly" {
			c.DDL.MaxWorkers = 8
		} else {
			c.DDL.MaxWorkers = 2
		}
	}
	if c.DDL.InitialWorkers == 0 {
		c.DDL.InitialWorkers = c.DDL.MaxWorkers / 2
		if c.DDL.InitialWorkers < 1 {
			c.DDL.InitialWorkers = 1
		}
	}

	if c.Verify.HealthInterval.Duration == 0 {
		c.Verify.HealthInterval.Duration = 10 * time.Second
	}
	if c.Verify.NoAdvanceSoft.Duration == 0 {
		c.Verify.NoAdvanceSoft.Duration = 2 * time.Minute
	}
	if c.Verify.NoAdvanceHard.Duration == 0 {
		if c.Profile == "weekly" {
			c.Verify.NoAdvanceHard.Duration = 15 * time.Minute
		} else {
			c.Verify.NoAdvanceHard.Duration = 5 * time.Minute
		}
	}
	if c.Verify.ConvergeWait.Duration == 0 {
		c.Verify.ConvergeWait.Duration = 20 * time.Second
	}
	if len(c.Verify.PanicPatterns) == 0 {
		c.Verify.PanicPatterns = []string{"panic", "fatal", "DATA RACE"}
	}
	if !c.Verify.LogScanEnabled {
		c.Verify.LogScanEnabled = true
	}
	if !c.Verify.FailOnPanicMatch {
		c.Verify.FailOnPanicMatch = true
	}

	if c.Failover.MinInterval.Duration == 0 {
		c.Failover.MinInterval.Duration = 20 * time.Second
	}
	if c.Failover.MaxInterval.Duration == 0 {
		c.Failover.MaxInterval.Duration = 40 * time.Second
	}
	if c.Failover.GatedProbability == 0 {
		c.Failover.GatedProbability = 0.5
	}
	if c.Failover.CdcBinary == "" {
		c.Failover.CdcBinary = "cdc.test"
	}
	if c.CDC.Keyspace == "" {
		c.CDC.Keyspace = "default"
	}
	if c.CDC.User == "" {
		c.CDC.User = "ticdc"
	}
	if c.CDC.Password == "" {
		c.CDC.Password = "ticdc_secret"
	}
	if c.CDC.Addr == "" {
		c.CDC.Addr = "127.0.0.1:8300"
	}

	if c.MySQL.DiffInterval.Duration == 0 {
		c.MySQL.DiffInterval.Duration = 2 * time.Minute
	}
	if c.MySQL.MaxDiffChecks == 0 {
		c.MySQL.MaxDiffChecks = 1
	}
	if c.MySQL.UpstreamStatusHost == "" {
		c.MySQL.UpstreamStatusHost = "127.0.0.1"
	}
	if c.MySQL.UpstreamStatusPort == 0 {
		c.MySQL.UpstreamStatusPort = 10080
	}

	// Basic validation.
	// Keep the schema shape stable so that:
	//   - bootstrap can create a predictable workload surface,
	//   - the integration scripts can pre-create storage sink directories deterministically,
	//   - the model can assume fixed database/table sets.
	if c.Bootstrap.DBCount != 5 {
		return fmt.Errorf("db_count must be 5 to match the design doc, got %d", c.Bootstrap.DBCount)
	}
	if c.Bootstrap.TablesPerDB != 20 {
		return fmt.Errorf("tables_per_db must be 20 to match the design doc, got %d", c.Bootstrap.TablesPerDB)
	}

	return nil
}
