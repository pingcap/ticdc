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

package integration

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/checker"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/stretchr/testify/require"
)

const (
	envKeyCDCOutput = "ACTIVE_ACTIVE_FAILPOINT_CDC_OUTPUT"

	envKeyCheckerOutput = "ACTIVE_ACTIVE_FAILPOINT_CHECKER_OUTPUT"

	envKeyReportDir = "ACTIVE_ACTIVE_FAILPOINT_REPORT_DIR"
)

func readRecordJSONL(path string) ([]checker.Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var records []checker.Record
	scanner := bufio.NewScanner(file)
	// JSONL lines may contain large row sets; increase scanner limit to avoid
	// "bufio.Scanner: token too long".
	scanner.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record checker.Record
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal jsonl record in %s line %d: %w", path, lineNo, err)
		}
		records = append(records, record)
	}

	err = scanner.Err()
	if err != nil {
		return nil, fmt.Errorf("failed to scan jsonl file %s: %w", path, err)
	}
	return records, nil
}

func getCdcRecords(t *testing.T, cdcOutputPath string) []checker.Record {
	records, err := readRecordJSONL(cdcOutputPath)
	require.NoError(t, err)
	return records
}

func getCheckerRecords(t *testing.T, checkerOutputPath string) []checker.Record {
	records, err := readRecordJSONL(checkerOutputPath)
	require.NoError(t, err)
	return records
}

func readReports(t *testing.T, reportDir string) []recorder.Report {
	entries, err := os.ReadDir(reportDir)
	require.NoError(t, err)

	var reportFiles []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".json") {
			reportFiles = append(reportFiles, filepath.Join(reportDir, entry.Name()))
		}
	}
	sort.Strings(reportFiles)

	var reports []recorder.Report
	for _, path := range reportFiles {
		content, err := os.ReadFile(path)
		require.NoError(t, err)

		var report recorder.Report
		err = json.Unmarshal(content, &report)
		require.NoError(t, err)
		reports = append(reports, report)
	}
	return reports
}

type dataLossKey struct {
	PK            string
	LocalCommitTS uint64
}

type dataInconsistentKey struct {
	PK            string
	LocalCommitTS uint64
}

type dataRedundantKey struct {
	PK       string
	OriginTS uint64
}

type lwwViolationKey struct {
	PK       string
	OriginTS uint64
}

func pkMapToKey(pk map[string]any) string {
	keys := make([]string, 0, len(pk))
	for key := range pk {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, normalizePKValue(pk[key])))
	}
	return strings.Join(parts, ",")
}

func normalizePKValue(v any) string {
	switch value := v.(type) {
	case string:
		if u, err := strconv.ParseUint(value, 10, 64); err == nil {
			return strconv.FormatUint(u, 10)
		}
		if i, err := strconv.ParseInt(value, 10, 64); err == nil {
			return strconv.FormatInt(i, 10)
		}
		return value
	case json.Number:
		return value.String()
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(value), 'f', -1, 32)
	case int:
		return strconv.FormatInt(int64(value), 10)
	case int8:
		return strconv.FormatInt(int64(value), 10)
	case int16:
		return strconv.FormatInt(int64(value), 10)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case uint:
		return strconv.FormatUint(uint64(value), 10)
	case uint8:
		return strconv.FormatUint(uint64(value), 10)
	case uint16:
		return strconv.FormatUint(uint64(value), 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func validate(t *testing.T,
	cdcRecords, checkerRecords []checker.Record,
	reports []recorder.Report,
) {
	dataLossItems := make(map[dataLossKey]struct{})
	dataInconsistentItems := make(map[dataInconsistentKey]struct{})
	dataRedundantItems := make(map[dataRedundantKey]struct{})
	lwwViolationItems := make(map[lwwViolationKey]struct{})
	for _, report := range reports {
		for _, clusterReport := range report.ClusterReports {
			for _, tableFailureItems := range clusterReport.TableFailureItems {
				for _, dataLossItem := range tableFailureItems.DataLossItems {
					key := dataLossKey{
						PK:            pkMapToKey(dataLossItem.PK),
						LocalCommitTS: dataLossItem.LocalCommitTS,
					}
					dataLossItems[key] = struct{}{}
				}
				for _, dataInconsistentItem := range tableFailureItems.DataInconsistentItems {
					key := dataInconsistentKey{
						PK:            pkMapToKey(dataInconsistentItem.PK),
						LocalCommitTS: dataInconsistentItem.LocalCommitTS,
					}
					dataInconsistentItems[key] = struct{}{}
				}
				for _, dataRedundantItem := range tableFailureItems.DataRedundantItems {
					key := dataRedundantKey{
						PK:       pkMapToKey(dataRedundantItem.PK),
						OriginTS: dataRedundantItem.OriginTS,
					}
					dataRedundantItems[key] = struct{}{}
				}
				for _, lwwViolationItem := range tableFailureItems.LWWViolationItems {
					key := lwwViolationKey{
						PK:       pkMapToKey(lwwViolationItem.PK),
						OriginTS: lwwViolationItem.OriginTS,
					}
					lwwViolationItems[key] = struct{}{}
				}
			}
		}
	}
	skippedRecords := make(map[dataLossKey]struct{})
	for _, record := range checkerRecords {
		for _, row := range record.Rows {
			skippedRecords[dataLossKey{PK: pkMapToKey(row.PrimaryKeys), LocalCommitTS: row.CommitTs}] = struct{}{}
		}
	}
	for _, record := range cdcRecords {
		for _, row := range record.Rows {
			switch record.Failpoint {
			case "cloudStorageSinkDropMessage":
				if row.OriginTs > 0 {
					key := dataLossKey{PK: pkMapToKey(row.PrimaryKeys), LocalCommitTS: row.OriginTs}
					if _, ok := skippedRecords[key]; !ok {
						_, ok := dataLossItems[key]
						require.True(t, ok)
						delete(dataLossItems, key)
					}
				} else {
					// the replicated record maybe skipped by LWW
					key := dataRedundantKey{PK: pkMapToKey(row.PrimaryKeys), OriginTS: row.CommitTs}
					_, ok := dataRedundantItems[key]
					if !ok {
						t.Log("replicated record maybe skipped by LWW", key)
					}
					delete(dataRedundantItems, key)
				}
			case "cloudStorageSinkMutateValue":
				keyLoss := dataLossKey{PK: pkMapToKey(row.PrimaryKeys), LocalCommitTS: row.CommitTs}
				if _, skipped := skippedRecords[keyLoss]; !skipped {
					key := dataInconsistentKey{PK: pkMapToKey(row.PrimaryKeys), LocalCommitTS: row.CommitTs}
					_, ok := dataInconsistentItems[key]
					require.True(t, ok)
					delete(dataInconsistentItems, key)
				}
			case "cloudStorageSinkMutateValueTidbOriginTs":
				keyLoss := dataLossKey{PK: pkMapToKey(row.PrimaryKeys), LocalCommitTS: row.OriginTs}
				if _, ok := skippedRecords[keyLoss]; !ok {
					_, ok := dataLossItems[keyLoss]
					require.True(t, ok)
					delete(dataLossItems, keyLoss)
				}

				keyRedundant := dataRedundantKey{PK: pkMapToKey(row.PrimaryKeys), OriginTS: row.OriginTs + 1}
				_, ok := dataRedundantItems[keyRedundant]
				require.True(t, ok)
				delete(dataRedundantItems, keyRedundant)
			}
		}
	}
	require.Empty(t, dataLossItems)
	require.Empty(t, dataInconsistentItems)
	require.Empty(t, dataRedundantItems)
	require.Empty(t, lwwViolationItems)
	require.Fail(t, "success")
}

func TestValidation(t *testing.T) {
	var (
		cdcOutputPath     string
		checkerOutputPath string
		reportDir         string
	)

	cdcOutputPath = os.Getenv(envKeyCDCOutput)
	if cdcOutputPath == "" {
		t.Log("skipped because ACTIVE_ACTIVE_FAILPOINT_CDC_OUTPUT is not set")
		return
	}
	checkerOutputPath = os.Getenv(envKeyCheckerOutput)
	if checkerOutputPath == "" {
		t.Log("skipped because ACTIVE_ACTIVE_FAILPOINT_CHECKER_OUTPUT is not set")
		return
	}
	reportDir = os.Getenv(envKeyReportDir)
	if reportDir == "" {
		t.Log("skipped because ACTIVE_ACTIVE_FAILPOINT_REPORT_DIR is not set")
		return
	}

	cdcRecords := getCdcRecords(t, cdcOutputPath)
	checkerRecords := getCheckerRecords(t, checkerOutputPath)
	reports := readReports(t, reportDir)

	validate(t, cdcRecords, checkerRecords, reports)
}
