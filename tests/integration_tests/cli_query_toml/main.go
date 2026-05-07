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
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> <file> [args...]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands: validate_toml, check_kebab_keys, compare_json_toml, check_overrides, check_filter_rules, check_indentation, check_array_format\n")
		os.Exit(1)
	}

	cmd := os.Args[1]
	var err error

	switch cmd {
	case "validate_toml":
		err = validateTOML(os.Args[2])
	case "check_kebab_keys":
		err = checkKebabKeys(os.Args[2])
	case "compare_json_toml":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "compare_json_toml requires <json_file> <toml_file>\n")
			os.Exit(1)
		}
		err = compareJSONTOML(os.Args[2], os.Args[3])
	case "check_overrides":
		err = checkOverrides(os.Args[2])
	case "check_filter_rules":
		if len(os.Args) < 4 {
			fmt.Fprintf(os.Stderr, "check_filter_rules requires <toml_file> <expected_count>\n")
			os.Exit(1)
		}
		var expected int
		fmt.Sscanf(os.Args[3], "%d", &expected)
		err = checkFilterRules(os.Args[2], expected)
	case "check_realistic":
		err = checkRealistic(os.Args[2])
	case "check_indentation":
		subcheck := ""
		if len(os.Args) >= 4 {
			subcheck = os.Args[3]
		}
		err = checkIndentation(os.Args[2], subcheck)
	case "check_array_format":
		err = checkArrayFormat(os.Args[2])
	case "check_default_array_format":
		err = checkDefaultArrayFormat(os.Args[2])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func readTOML(path string) (map[string]interface{}, error) {
	var data map[string]interface{}
	if _, err := toml.DecodeFile(path, &data); err != nil {
		return nil, fmt.Errorf("failed to parse TOML file %s: %v", path, err)
	}
	return data, nil
}

func readJSON(path string) (map[string]interface{}, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file %s: %v", path, err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON file %s: %v", path, err)
	}
	return data, nil
}

func validateTOML(path string) error {
	data, err := readTOML(path)
	if err != nil {
		return err
	}
	if _, ok := data["id"]; !ok {
		return fmt.Errorf("missing 'id' field in TOML")
	}
	if _, ok := data["sink-uri"]; !ok {
		return fmt.Errorf("missing 'sink-uri' field in TOML")
	}
	if _, ok := data["config"]; !ok {
		return fmt.Errorf("missing 'config' section in TOML")
	}
	return nil
}

func checkKebabKeys(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}
	text := string(content)

	snakeCaseKeys := []string{"sink_uri", "start_ts", "case_sensitive", "worker_num",
		"checkpoint_tso", "checkpoint_time", "upstream_id", "creator_version"}
	for _, key := range snakeCaseKeys {
		if strings.Contains(text, key+" ") || strings.Contains(text, key+"=") {
			return fmt.Errorf("found snake_case key '%s' in TOML output", key)
		}
	}

	kebabKeys := []string{"sink-uri", "start-ts", "checkpoint-tso"}
	for _, key := range kebabKeys {
		if !strings.Contains(text, key) {
			return fmt.Errorf("missing expected kebab-case key '%s' in TOML output", key)
		}
	}

	return nil
}

func compareJSONTOML(jsonPath, tomlPath string) error {
	jData, err := readJSON(jsonPath)
	if err != nil {
		return err
	}
	tData, err := readTOML(tomlPath)
	if err != nil {
		return err
	}

	if str(jData["id"]) != str(tData["id"]) {
		return fmt.Errorf("id mismatch: json=%v toml=%v", jData["id"], tData["id"])
	}
	if str(jData["sink_uri"]) != str(tData["sink-uri"]) {
		return fmt.Errorf("sink-uri mismatch: json=%v toml=%v", jData["sink_uri"], tData["sink-uri"])
	}
	if str(jData["state"]) != str(tData["state"]) {
		return fmt.Errorf("state mismatch: json=%v toml=%v", jData["state"], tData["state"])
	}

	jConfig, jOk := jData["config"].(map[string]interface{})
	tConfig, tOk := tData["config"].(map[string]interface{})
	if !jOk || !tOk {
		return fmt.Errorf("config section mismatch: json_ok=%v toml_ok=%v", jOk, tOk)
	}
	if jConfig["case_sensitive"] != tConfig["case-sensitive"] {
		return fmt.Errorf("config.case-sensitive mismatch: json=%v toml=%v",
			jConfig["case_sensitive"], tConfig["case-sensitive"])
	}

	jMounter, jmOk := jConfig["mounter"].(map[string]interface{})
	tMounter, tmOk := tConfig["mounter"].(map[string]interface{})
	if !jmOk || !tmOk {
		return fmt.Errorf("config.mounter section mismatch: json_ok=%v toml_ok=%v", jmOk, tmOk)
	}
	if num(jMounter["worker_num"]) != num(tMounter["worker-num"]) {
		return fmt.Errorf("config.mounter.worker-num mismatch: json=%v toml=%v",
			jMounter["worker_num"], tMounter["worker-num"])
	}

	jFilter, jfOk := jConfig["filter"].(map[string]interface{})
	tFilter, tfOk := tConfig["filter"].(map[string]interface{})
	if !jfOk || !tfOk {
		return fmt.Errorf("config.filter section mismatch: json_ok=%v toml_ok=%v", jfOk, tfOk)
	}
	jRules := toStringSlice(jFilter["rules"])
	tRules := toStringSlice(tFilter["rules"])
	if len(jRules) != len(tRules) {
		return fmt.Errorf("filter.rules length mismatch: json=%d toml=%d", len(jRules), len(tRules))
	}
	for i := range jRules {
		if jRules[i] != tRules[i] {
			return fmt.Errorf("filter.rules[%d] mismatch: json=%s toml=%s", i, jRules[i], tRules[i])
		}
	}

	return nil
}

func checkOverrides(path string) error {
	data, err := readTOML(path)
	if err != nil {
		return err
	}
	config, ok := data["config"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing or invalid 'config' section")
	}
	if config["case-sensitive"] != true {
		return fmt.Errorf("expected case-sensitive=true, got %v", config["case-sensitive"])
	}
	mounter, ok := config["mounter"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing config.mounter section")
	}
	if num(mounter["worker-num"]) != 8 {
		return fmt.Errorf("expected mounter.worker-num=8, got %v", mounter["worker-num"])
	}
	filter, ok := config["filter"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing config.filter section")
	}
	rules := toStringSlice(filter["rules"])
	found := false
	for _, r := range rules {
		if r == "test_db.*" {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("filter.rules missing 'test_db.*', got %v", rules)
	}
	return nil
}

func checkFilterRules(path string, expectedCount int) error {
	data, err := readTOML(path)
	if err != nil {
		return err
	}
	config, ok := data["config"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing or invalid 'config' section")
	}
	filter, ok := config["filter"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing config.filter section")
	}
	rules := toStringSlice(filter["rules"])
	if len(rules) != expectedCount {
		return fmt.Errorf("expected %d filter rules, got %d: %v", expectedCount, len(rules), rules)
	}
	return nil
}

func str(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func num(v interface{}) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, _ := n.Int64()
		return i
	default:
		return 0
	}
}

func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		result = append(result, fmt.Sprintf("%v", item))
	}
	return result
}

func checkRealistic(path string) error {
	data, err := readTOML(path)
	if err != nil {
		return err
	}
	config, ok := data["config"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing or invalid 'config' section")
	}
	if config["case-sensitive"] != true {
		return fmt.Errorf("expected case-sensitive=true, got %v", config["case-sensitive"])
	}
	filter, ok := config["filter"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing config.filter section")
	}
	rules := toStringSlice(filter["rules"])
	found := false
	for _, r := range rules {
		if r == "test_db_not_exist.*" {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("filter.rules missing 'test_db_not_exist.*', got %v", rules)
	}
	scheduler, ok := config["scheduler"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("missing config.scheduler section")
	}
	if scheduler["enable-table-across-nodes"] != true {
		return fmt.Errorf("expected scheduler.enable-table-across-nodes=true, got %v",
			scheduler["enable-table-across-nodes"])
	}
	if num(scheduler["region-threshold"]) != 1000 {
		return fmt.Errorf("expected scheduler.region-threshold=1000, got %v",
			scheduler["region-threshold"])
	}
	return nil
}

func checkIndentation(path string, subcheck string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}
	lines := strings.Split(string(content), "\n")

	currentDepth := 0
	inConfig := false

	for _, line := range lines {
		if line == "" {
			continue
		}
		trimmed := strings.TrimSpace(line)

		if trimmed == "[config]" {
			inConfig = true
			currentDepth = 1
			continue
		}
		if strings.HasPrefix(trimmed, "[config.") {
			inConfig = true
			currentDepth = strings.Count(trimmed, ".") + 1
			expectedIndent := (currentDepth - 1) * 2
			actualIndent := len(line) - len(strings.TrimLeft(line, " "))
			if (subcheck == "" || subcheck == "nested_headers") && actualIndent != expectedIndent {
				return fmt.Errorf("section %s: expected %d-space indent, got %d", trimmed, expectedIndent, actualIndent)
			}
			continue
		}
		if strings.HasPrefix(trimmed, "[") {
			inConfig = false
			continue
		}
		if inConfig {
			expectedIndent := currentDepth * 2
			actualIndent := len(line) - len(strings.TrimLeft(line, " "))
			if subcheck == "config_fields" && currentDepth == 1 && actualIndent < expectedIndent {
				return fmt.Errorf("config field: expected >= %d-space indent, got %d: %q",
					expectedIndent, actualIndent, line)
			}
			if subcheck == "nested_fields" && currentDepth > 1 && actualIndent < expectedIndent {
				return fmt.Errorf("nested field under depth %d: expected >= %d-space indent, got %d: %q",
					currentDepth, expectedIndent, actualIndent, line)
			}
			if subcheck == "" && actualIndent < expectedIndent {
				return fmt.Errorf("field under depth %d: expected >= %d-space indent, got %d: %q",
					currentDepth, expectedIndent, actualIndent, line)
			}
		}
	}
	return nil
}

func checkDefaultArrayFormat(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}
	text := string(content)
	expected := "    rules = [\n      '*.*'\n    ]"
	if !strings.Contains(text, expected) {
		return fmt.Errorf("default filter rules not in expected multi-line format")
	}
	return nil
}

func checkArrayFormat(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}
	text := string(content)

	if !strings.Contains(text, "    rules = [\n") {
		return fmt.Errorf("rules array not in multi-line format with 4-space indent")
	}
	if !strings.Contains(text, "      'db_alpha.*'") {
		return fmt.Errorf("array elements not 6-space indented")
	}
	if !strings.Contains(text, "    ]") {
		return fmt.Errorf("closing bracket not 4-space indented")
	}
	return nil
}
