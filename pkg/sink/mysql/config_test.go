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

package mysql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/DATA-DOG/go-sqlmock"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestGenerateDSNByConfig(t *testing.T) {
	t.Parallel()
	testDefaultConfig := func() {
		db, err := MockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := New()
		dsnStr, err := generateDSNByConfig(dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"tidb_txn_mode=optimistic",
			"readTimeout=2m",
			"writeTimeout=2m",
			"allow_auto_random_explicit_insert=1",
			"transaction_isolation=%22READ-COMMITTED%22",
			"charset=utf8mb4",
			"tidb_placement_mode=%22ignore%22",
		}
		for _, param := range expectedCfg {
			require.Contains(t, dsnStr, param)
		}
		require.False(t, strings.Contains(dsnStr, "time_zone"))
	}

	testActiveActiveDefaultTiDBTxnMode := func() {
		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)

		checkTxnMode := func(cfg *Config, expectedTxnMode string) {
			db, mock, err := sqlmock.New()
			require.Nil(t, err)
			columns := []string{"Variable_name", "Value"}
			mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
			)
			mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("tidb_txn_mode", expectedTxnMode),
			)
			mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
				sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
			)
			mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
				WillReturnRows(
					sqlmock.NewRows(columns).
						AddRow("tidb_placement_mode", "IGNORE"),
				)
			mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
				WillReturnRows(
					sqlmock.NewRows(columns).
						AddRow("tidb_enable_external_ts_read", "OFF"),
				)
			mock.ExpectClose()

			dsnStr, err := generateDSNByConfig(dsn, cfg, db)
			require.Nil(t, err)
			require.Contains(t, dsnStr, "tidb_txn_mode="+expectedTxnMode)
			require.Nil(t, db.Close())
			require.Nil(t, mock.ExpectationsWereMet())
		}

		cfg := New()
		cfg.IsTiDB = true
		cfg.EnableActiveActive = true
		checkTxnMode(cfg, txnModePessimistic)

		cfg = New()
		cfg.IsTiDB = true
		cfg.EnableActiveActive = true
		cfg.tidbTxnModeSpecified = true
		checkTxnMode(cfg, txnModeOptimistic)
	}

	testTimezoneParam := func() {
		db, err := MockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := New()
		cfg.Timezone = `"UTC"`
		dsnStr, err := generateDSNByConfig(dsn, cfg, db)
		require.Nil(t, err)
		require.True(t, strings.Contains(dsnStr, "time_zone=%22UTC%22"))
	}

	testTimeoutConfig := func() {
		db, err := MockTestDB(false)
		require.Nil(t, err)
		defer db.Close()

		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		uri, err := url.Parse("mysql://127.0.0.1:3306/?read-timeout=4m&write-timeout=5m&timeout=3m")
		require.Nil(t, err)
		cfg := New()

		changefeedConfig := &config.ChangefeedConfig{
			TimeZone: "UTC",
			SinkConfig: &config.SinkConfig{
				TiDBSourceID: 1,
			},
		}
		err = cfg.Apply(uri, common.NewChangefeedID4Test("default", "changefeed-01"), changefeedConfig)
		require.Nil(t, err)
		dsnStr, err := generateDSNByConfig(dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"readTimeout=4m",
			"writeTimeout=5m",
			"timeout=3m",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}
	}

	testIsolationConfig := func() {
		db, mock, err := sqlmock.New()
		require.Nil(t, err)
		defer db.Close() // nolint:errcheck
		columns := []string{"Variable_name", "Value"}
		mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
		)
		mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
		)
		// simulate error
		dsn, err := dmysql.ParseDSN("root:123456@tcp(127.0.0.1:4000)/")
		require.Nil(t, err)
		cfg := New()
		var dsnStr string
		_, err = generateDSNByConfig(dsn, cfg, db)
		require.Error(t, err)

		// simulate no transaction_isolation
		mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
		)
		mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
		)
		mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_placement_mode", "IGNORE"),
			)
		mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_enable_external_ts_read", "OFF"),
			)
		dsnStr, err = generateDSNByConfig(dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg := []string{
			"tx_isolation=%22READ-COMMITTED%22",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}

		// simulate transaction_isolation
		mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
		)
		mock.ExpectQuery("show session variables like 'tidb_txn_mode';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("tidb_txn_mode", "pessimistic"),
		)
		mock.ExpectQuery("show session variables like 'transaction_isolation';").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("transaction_isolation", "REPEATED-READ"),
		)
		mock.ExpectQuery("show session variables like 'tidb_placement_mode';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_placement_mode", "IGNORE"),
			)
		mock.ExpectQuery("show session variables like 'tidb_enable_external_ts_read';").
			WillReturnRows(
				sqlmock.NewRows(columns).
					AddRow("tidb_enable_external_ts_read", "OFF"),
			)
		dsnStr, err = generateDSNByConfig(dsn, cfg, db)
		require.Nil(t, err)
		expectedCfg = []string{
			"transaction_isolation=%22READ-COMMITTED%22",
		}
		for _, param := range expectedCfg {
			require.True(t, strings.Contains(dsnStr, param))
		}
	}

	testDefaultConfig()
	testActiveActiveDefaultTiDBTxnMode()
	testTimezoneParam()
	testTimeoutConfig()
	testIsolationConfig()
}

func TestApplySinkURIParamsToConfig(t *testing.T) {
	t.Parallel()

	expected := New()
	expected.workerCountSpecified = true
	expected.WorkerCount = 64
	expected.MaxTxnRow = 20
	expected.MaxMultiUpdateRowCount = 80
	expected.MaxMultiUpdateRowSize = 512
	// expected.BatchReplaceEnabled = true
	// expected.BatchReplaceSize = 50
	expected.SafeMode = false
	expected.Timezone = `"UTC"`
	expected.TidbTxnMode = "pessimistic"
	expected.tidbTxnModeSpecified = true
	// expected.EnableOldValue = true
	uriStr := "mysql://127.0.0.1:3306/?time-zone=UTC&worker-count=64&max-txn-row=20" +
		"&max-multi-update-row=80&max-multi-update-row-size=512" +
		"&batch-replace-enable=true&batch-replace-size=50&safe-mode=false" +
		"&tidb-txn-mode=pessimistic"
	uri, err := url.Parse(uriStr)
	require.Nil(t, err)
	cfg := New()
	changefeedConfig := &config.ChangefeedConfig{
		TimeZone: "UTC",
		SinkConfig: &config.SinkConfig{
			TiDBSourceID: 1,
		},
	}
	err = cfg.Apply(uri, common.NewChangefeedID4Test("default", "changefeed-01"), changefeedConfig)
	require.Nil(t, err)

	expected.sinkURI = uri
	require.Equal(t, expected, cfg)
}

func TestDefaultWorkerCountByDownstream(t *testing.T) {
	t.Parallel()

	cfg := New()
	cfg.IsTiDB = true
	cfg.setWorkerCountByDownstream()
	require.Equal(t, DefaultTiDBWorkerCount, cfg.WorkerCount)

	cfg = New()
	cfg.IsTiDB = false
	cfg.setWorkerCountByDownstream()
	require.Equal(t, DefaultMySQLWorkerCount, cfg.WorkerCount)

	cfg = New()
	cfg.workerCountSpecified = true
	cfg.WorkerCount = 123
	cfg.IsTiDB = false
	cfg.setWorkerCountByDownstream()
	require.Equal(t, 123, cfg.WorkerCount)
}

func TestParseSinkURIOverride(t *testing.T) {
	t.Parallel()

	cases := []struct {
		uri     string
		checker func(*Config)
	}{{
		uri: "mysql://127.0.0.1:3306/",
		checker: func(sp *Config) {
			require.True(t, sp.MultiStmtEnable)
			require.True(t, sp.BatchDMLEnable)
			require.True(t, sp.CachePrepStmts)
			require.True(t, sp.EnableDDLTs)
			require.False(t, sp.HasVectorType)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?worker-count=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.WorkerCount, maxWorkerCount)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-txn-row=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxTxnRow, maxMaxTxnRow)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-multi-update-row=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxMultiUpdateRowCount, maxMaxMultiUpdateRowCount)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?max-multi-update-row-size=2147483648", // int32 max
		checker: func(sp *Config) {
			require.EqualValues(t, sp.MaxMultiUpdateRowSize, maxMaxMultiUpdateRowSize)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?tidb-txn-mode=badmode",
		checker: func(sp *Config) {
			require.EqualValues(t, sp.TidbTxnMode, defaultTiDBTxnMode)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?multi-stmt-enable=false",
		checker: func(sp *Config) {
			require.False(t, sp.MultiStmtEnable)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?batch-dml-enable=false",
		checker: func(sp *Config) {
			require.False(t, sp.BatchDMLEnable)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?cache-prep-stmts=false",
		checker: func(sp *Config) {
			require.False(t, sp.CachePrepStmts)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?enable-ddl-ts=false",
		checker: func(sp *Config) {
			require.False(t, sp.EnableDDLTs)
		},
	}, {
		uri: "mysql://127.0.0.1:3306/?has-vector-type=true",
		checker: func(sp *Config) {
			require.True(t, sp.HasVectorType)
		},
	}}
	var uri *url.URL
	var err error
	for _, cs := range cases {
		if cs.uri != "" {
			uri, err = url.Parse(cs.uri)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		cfg := New()
		changefeedConfig := &config.ChangefeedConfig{
			TimeZone: "UTC",
			SinkConfig: &config.SinkConfig{
				TiDBSourceID: 1,
			},
		}
		err = cfg.Apply(uri, common.NewChangefeedID4Test("default", "changefeed-01"), changefeedConfig)
		require.Nil(t, err)
		cs.checker(cfg)
	}
}

func TestParseSinkURIBadQueryString(t *testing.T) {
	t.Parallel()

	uris := []string{
		"",
		"postgre://127.0.0.1:3306",
		"mysql://127.0.0.1:3306/?worker-count=not-number",
		"mysql://127.0.0.1:3306/?worker-count=-1",
		"mysql://127.0.0.1:3306/?worker-count=0",
		"mysql://127.0.0.1:3306/?max-txn-row=not-number",
		"mysql://127.0.0.1:3306/?max-txn-row=-1",
		"mysql://127.0.0.1:3306/?max-txn-row=0",
		"mysql://127.0.0.1:3306/?ssl-ca=only-ca-exists",
		// "mysql://127.0.0.1:3306/?batch-replace-enable=not-bool",
		// "mysql://127.0.0.1:3306/?batch-replace-enable=true&batch-replace-size=not-number",
		"mysql://127.0.0.1:3306/?safe-mode=not-bool",
		"mysql://127.0.0.1:3306/?time-zone=badtz",
		"mysql://127.0.0.1:3306/?write-timeout=badduration",
		"mysql://127.0.0.1:3306/?read-timeout=badduration",
		"mysql://127.0.0.1:3306/?timeout=badduration",
	}
	var uri *url.URL
	var err error
	for _, uriStr := range uris {
		if uriStr != "" {
			uri, err = url.Parse(uriStr)
			require.Nil(t, err)
		} else {
			uri = nil
		}
		cfg := New()
		canalJsonProtocol := "canal-json"
		changefeedConfig := &config.ChangefeedConfig{
			// SinkURI: "tidb://root:@127.0.0.1:4000?dry-run=true",
			SinkURI: "kafka://127.0.0.1:9094/hyy?protocol=canal-json&kafka-version=2.4.0&max-message-bytes=67108864&replication-factor=1",
			Filter:  &config.FilterConfig{},
			SinkConfig: &config.SinkConfig{
				Protocol:     &canalJsonProtocol,
				TiDBSourceID: 1,
			},
		}
		err = cfg.Apply(uri, common.NewChangefeedID4Test("default", "changefeed-01"), changefeedConfig)
		require.Error(t, err)
	}
}

func TestCheckTiDBVariable(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.Nil(t, err)
	defer db.Close() //nolint:errcheck
	columns := []string{"Variable_name", "Value"}

	mock.ExpectQuery("show session variables like 'allow_auto_random_explicit_insert';").WillReturnRows(
		sqlmock.NewRows(columns).AddRow("allow_auto_random_explicit_insert", "0"),
	)
	val, err := checkTiDBVariable(db, "allow_auto_random_explicit_insert", "1")
	require.Nil(t, err)
	require.Equal(t, "1", val)

	mock.ExpectQuery("show session variables like 'no_exist_variable';").WillReturnError(sql.ErrNoRows)
	val, err = checkTiDBVariable(db, "no_exist_variable", "0")
	require.Nil(t, err)
	require.Equal(t, "", val)

	mock.ExpectQuery("show session variables like 'version';").WillReturnError(sql.ErrConnDone)
	_, err = checkTiDBVariable(db, "version", "5.7.25-TiDB-v4.0.0")
	require.NotNil(t, err)
	require.Regexp(t, ".*"+sql.ErrConnDone.Error(), err.Error())
}

func TestApplyTimezone(t *testing.T) {
	t.Parallel()

	localTimezone, err := util.GetTimezone("Local")
	require.Nil(t, err)

	for _, test := range []struct {
		name                 string
		noChangefeedTimezone bool
		changefeedTimezone   string
		serverTimezone       *time.Location
		expected             string
		expectedHasErr       bool
		expectedErr          string
	}{
		{
			name:                 "no changefeed timezone",
			noChangefeedTimezone: true,
			serverTimezone:       time.UTC,
			expected:             "",
			expectedHasErr:       false,
		},
		{
			name:                 "empty changefeed timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "",
			serverTimezone:       time.UTC,
			expected:             "",
			expectedHasErr:       false,
		},
		{
			name:                 "normal changefeed timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "UTC",
			serverTimezone:       time.UTC,
			expected:             "\"UTC\"",
			expectedHasErr:       false,
		},
		{
			name:                 "local timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "Local",
			serverTimezone:       localTimezone,
			expected:             "\"" + localTimezone.String() + "\"",
			expectedHasErr:       false,
		},
		{
			name:                 "sink-uri timezone different from server timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "UTC",
			serverTimezone:       time.FixedZone("Asia/Shanghai", 8*60*60),
			expectedHasErr:       true,
			expectedErr:          "Please make sure that the timezone of the TiCDC server",
		},
		{
			name:                 "offset different from server timezone",
			noChangefeedTimezone: false,
			changefeedTimezone:   "%2B08%3A00", // +08:00
			serverTimezone:       time.UTC,
			expectedHasErr:       true,
			expectedErr:          "Please make sure that the timezone of the TiCDC server",
		},
	} {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := New()
			sinkURI := "mysql://127.0.0.1:3306"
			if !tc.noChangefeedTimezone {
				sinkURI = sinkURI + "?time-zone=" + tc.changefeedTimezone
			}
			uri, err := url.Parse(sinkURI)
			require.Nil(t, err)
			changefeedConfig := &config.ChangefeedConfig{
				TimeZone: test.serverTimezone.String(),
				SinkConfig: &config.SinkConfig{
					TiDBSourceID: 1,
				},
			}
			err = cfg.Apply(uri, common.NewChangefeedID4Test("default", "changefeed-01"), changefeedConfig)
			if tc.expectedHasErr {
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.Nil(t, err)
				require.Equal(t, tc.expected, cfg.Timezone)
			}
		})
	}
}

func TestParseMySQLTimezone(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		input    string
		expected string
		offset   int
	}{
		{input: "UTC", expected: "UTC"},
		{input: "Etc/UTC", expected: "Etc/UTC"},
		{input: "+0:00", expected: "+00:00"},
		{input: "-00:00", expected: "+00:00"},
		{input: "-0:00", expected: "+00:00"},
		{input: "+8:00", expected: "+08:00", offset: 8 * 3600},
		{input: "-6:00", expected: "-06:00", offset: -6 * 3600},
		{input: "+05:30", expected: "+05:30", offset: 5*3600 + 30*60},
		{input: "-03:30", expected: "-03:30", offset: -(3*3600 + 30*60)},
		{input: "+00:01", expected: "+00:01", offset: 60},
		{input: "-00:01", expected: "-00:01", offset: -60},
		{input: "-13:59", expected: "-13:59", offset: -(13*3600 + 59*60)},
		{input: "+14:00", expected: "+14:00", offset: 14 * 3600},
	} {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			loc, err := parseMySQLTimezone(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, loc.String())
			_, offset := time.Unix(0, 0).In(loc).Zone()
			require.Equal(t, tc.offset, offset)
		})
	}
	for _, input := range []string{
		"+14:01", "-14:00", "+24:00", "+08:60", "+08:99", "-13:60",
		"+00:60", "-00:60", "+13:60", "-12:60", "+8:60",
		"+8:0", "+008:00", "+0800", "+08:00:00", "+08", "+", "--08:00",
		"UTC+8", "08:00", " +00:00", "+00:00 ", "+０８:00", "invalid",
		"-", "+0", "+0:", "+:00", "+0:0", "++08:00", "+-08:00",
		"+08:0a", "+08:00Z", "+08:00\n", "+08:00\x00", "\"+08:00\"",
	} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			_, err := parseMySQLTimezone(input)
			require.Error(t, err)
		})
	}
}

func TestParseMySQLTimezoneOffsetRange(t *testing.T) {
	t.Parallel()
	// Cover every valid minute plus the values accepted by time.Parse but not
	// MySQL (minute 60 and hours up to 24), with both hour-width spellings.
	for _, sign := range []string{"+", "-"} {
		for hour := 0; hour <= 24; hour++ {
			for minute := 0; minute <= 60; minute++ {
				offset := (hour*60 + minute) * 60
				if sign == "-" {
					offset = -offset
				}
				canonical := fmt.Sprintf("%s%02d:%02d", sign, hour, minute)
				if offset == 0 {
					canonical = "+00:00"
				}
				for _, name := range []string{
					fmt.Sprintf("%s%02d:%02d", sign, hour, minute),
					fmt.Sprintf("%s%d:%02d", sign, hour, minute),
				} {
					loc, err := parseMySQLTimezone(name)
					if minute == 60 || offset < -(13*60+59)*60 || offset > 14*60*60 {
						require.Error(t, err, name)
						require.Nil(t, loc, name)
						continue
					}
					require.NoError(t, err, name)
					require.Equal(t, canonical, loc.String(), name)
					_, actual := time.Unix(0, 0).In(loc).Zone()
					require.Equal(t, offset, actual, name)
					// A numeric offset must remain fixed, even if time.Parse can
					// reuse the host's Local location with the same offset.
					start, end := time.Unix(0, 0).In(loc).ZoneBounds()
					require.True(t, start.IsZero() && end.IsZero(), name)
				}
			}
		}
	}
}

func TestConstantTimezoneOffset(t *testing.T) {
	t.Parallel()
	for _, offset := range []int{0, 60, -60, 5*3600 + 30*60, -(13*3600 + 59*60), 14 * 3600} {
		require.True(t, hasConstantTimezoneOffset(time.FixedZone("fixed", offset), offset))
		require.False(t, hasConstantTimezoneOffset(time.FixedZone("fixed", offset), offset+60))
	}
	for _, name := range []string{"UTC", "Etc/UTC", "Etc/GMT-8", "Asia/Shanghai", "America/New_York"} {
		loc, err := time.LoadLocation(name)
		require.NoError(t, err)
		_, offset := time.Date(2026, 1, 1, 0, 0, 0, 0, loc).Zone()
		require.Equal(t, name != "Asia/Shanghai" && name != "America/New_York", hasConstantTimezoneOffset(loc, offset))
	}
	// A future-only rule must not be accepted just because it matches today.
	// Use a small TZif fixture to avoid relying on the host tzdata version.
	data := []byte("TZif\x00" + string(make([]byte, 15)))
	// One 32-bit transition at 2037-01-01, two local time types, four name bytes.
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 4)
	transition := uint32(time.Date(2037, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	data = append(data, byte(transition>>24), byte(transition>>16), byte(transition>>8), byte(transition), 1)
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0x0e, 0x10, 0, 2, 'A', 0, 'B', 0)
	loc, err := time.LoadLocationFromTZData("future", data)
	require.NoError(t, err)
	require.False(t, hasConstantTimezoneOffset(loc, 0))
}

func TestTimezoneConfigPrecedence(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		toml     string
		query    string
		server   string
		expected string
		wantErr  bool
	}{
		{name: "default"},
		{name: "TOML named zone", toml: `time-zone = "UTC"`, expected: `"UTC"`},
		{name: "TOML offset", toml: `time-zone = "+00:00"`, expected: `"+00:00"`},
		{name: "TOML empty", toml: `time-zone = ""`},
		{name: "unrelated URI preserves TOML", toml: `time-zone = "+00:00"`, query: "?worker-count=1", expected: `"+00:00"`},
		{name: "TOML positive offset", toml: `time-zone = "+8:00"`, server: "Etc/GMT-8", expected: `"+08:00"`},
		{name: "URI negative offset", query: "?time-zone=-6:00", server: "Etc/GMT+6", expected: `"-06:00"`},
		{name: "TOML negative zero", toml: `time-zone = "-0:00"`, expected: `"+00:00"`},
		{name: "URI offset", query: "?time-zone=%2B00:00", expected: `"+00:00"`},
		{name: "URI overrides TOML", toml: `time-zone = "Asia/Shanghai"`, query: "?time-zone=%2B0%3A00", expected: `"+00:00"`},
		{name: "URI named overrides TOML", toml: `time-zone = "+08:00"`, query: "?time-zone=UTC", expected: `"UTC"`},
		{name: "URI empty overrides TOML", toml: `time-zone = "UTC"`, query: "?time-zone="},
		{name: "URI bare key overrides TOML", toml: `time-zone = "UTC"`, query: "?time-zone"},
		{name: "valid URI overrides invalid TOML", toml: `time-zone = "invalid"`, query: "?time-zone=%2B00%3A00", expected: `"+00:00"`},
		{name: "empty URI overrides invalid TOML", toml: `time-zone = "invalid"`, query: "?time-zone="},
		{name: "invalid TOML", toml: `time-zone = "invalid"`, wantErr: true},
		{name: "inconsistent TOML", toml: `time-zone = "+08:00"`, wantErr: true},
		{name: "invalid URI overrides TOML", toml: `time-zone = "UTC"`, query: "?time-zone=invalid", wantErr: true},
		{name: "unescaped URI plus", query: "?time-zone=+00:00", wantErr: true},
		{name: "quoted URI offset", query: "?time-zone=%22%2B00:00%22", wantErr: true},
		{name: "double encoded URI offset", query: "?time-zone=%252B00%253A00", wantErr: true},
		{name: "TOML minute overflow", toml: `time-zone = "+00:60"`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			replica := config.GetDefaultReplicaConfig()
			_, err := toml.Decode("[sink.mysql-config]\n"+tc.toml, replica)
			require.NoError(t, err)
			cfg := &config.ChangefeedConfig{TimeZone: "UTC", SinkConfig: replica.Sink}
			if tc.server != "" {
				cfg.TimeZone = tc.server
			}
			uri, err := url.Parse("mysql://root@127.0.0.1:3306/" + tc.query)
			require.NoError(t, err)
			sinkConfig := New()
			err = sinkConfig.Apply(uri, common.NewChangefeedID4Test("default", "timezone"), cfg)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, sinkConfig.Timezone)
			// Verify the value actually reaching the driver's DSN, not just Apply.
			dsn, err := GenBasicDSN(sinkConfig)
			require.NoError(t, err)
			parsed, err := dmysql.ParseDSN(dsn.FormatDSN())
			require.NoError(t, err)
			if tc.expected == "" {
				require.NotContains(t, parsed.Params, "time_zone")
			} else {
				require.Equal(t, tc.expected, parsed.Params["time_zone"])
			}
		})
	}
}

func TestTimezoneOffsetConsistency(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		server   string
		sink     string
		expected string
		wantErr  bool
	}{
		{server: "UTC", sink: "+00:00", expected: `"+00:00"`},
		{server: "Etc/UTC", sink: "-0:00", expected: `"+00:00"`},
		{server: "Etc/GMT-8", sink: "+8:00", expected: `"+08:00"`},
		{server: "Etc/GMT+6", sink: "-6:00", expected: `"-06:00"`},
		{server: "UTC", sink: "+08:00", wantErr: true},
		{server: "Asia/Shanghai", sink: "+08:00", wantErr: true},
		{server: "America/New_York", sink: "-05:00", wantErr: true},
		{server: "invalid", sink: "+00:00", wantErr: true},
		{server: "Asia/Shanghai", sink: "Asia/Shanghai", expected: `"Asia/Shanghai"`},
		{server: "America/New_York", sink: "America/New_York", expected: `"America/New_York"`},
		{server: "America/New_York", sink: "-04:00", wantErr: true},
		{server: "Etc/GMT-14", sink: "+14:00", expected: `"+14:00"`},
	} {
		t.Run(tc.server+"/"+tc.sink, func(t *testing.T) {
			t.Parallel()
			var timezone string
			err := getTimezone(tc.server, url.Values{"time-zone": {tc.sink}}, &timezone)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, timezone)
		})
	}
}

func TestTimezoneOffsetDSN(t *testing.T) {
	t.Parallel()
	for _, timezone := range []string{`"+00:00"`, `"+08:00"`, `"-06:00"`, `"+05:45"`, `"-03:30"`, `"-13:59"`, `"+14:00"`} {
		t.Run(timezone, func(t *testing.T) {
			t.Parallel()
			uri, err := url.Parse("mysql://root@127.0.0.1:3306/")
			require.NoError(t, err)
			cfg := New()
			cfg.sinkURI = uri
			cfg.Timezone = timezone
			dsn, err := GenBasicDSN(cfg)
			require.NoError(t, err)
			require.Equal(t, timezone, dsn.Params["time_zone"])
			parsed, err := dmysql.ParseDSN(dsn.FormatDSN())
			require.NoError(t, err)
			require.Equal(t, timezone, parsed.Params["time_zone"])

			db, err := MockTestDB(false)
			require.NoError(t, err)
			defer db.Close()
			fullDSN, err := generateDSNByConfig(dsn, cfg, db)
			require.NoError(t, err)
			parsed, err = dmysql.ParseDSN(fullDSN)
			require.NoError(t, err)
			require.Equal(t, timezone, parsed.Params["time_zone"])
		})
	}
}

func TestParseMySQLTimezoneNamedZones(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"UTC", "Etc/UTC", "Asia/Shanghai", "America/New_York"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			loc, err := parseMySQLTimezone(name)
			require.NoError(t, err)
			expected, err := time.LoadLocation(name)
			require.NoError(t, err)
			require.Equal(t, expected.String(), loc.String())
			// Named zones must retain historical and DST rules, not become a
			// fixed offset based on the current date.
			for _, year := range []int{1900, 1991, 2026, 2040} {
				for _, month := range []time.Month{time.January, time.July} {
					instant := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
					wantName, wantOffset := instant.In(expected).Zone()
					gotName, gotOffset := instant.In(loc).Zone()
					require.Equal(t, wantName, gotName)
					require.Equal(t, wantOffset, gotOffset)
				}
			}
		})
	}
}
