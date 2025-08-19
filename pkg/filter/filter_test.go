// Copyright 2025 PingCAP, Inc.
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

package filter

import (
	"testing"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestFilterValue(t *testing.T) {
	ft, err := NewFilter(&config.FilterConfig{}, "", false, false)
	require.Nil(t, err)
	ok := ft.ShouldIgnoreSchema(TiDBWorkloadSchema)
	require.True(t, ok)
}

// Helper functions to create test configurations
func createTestFilterConfig() *eventpb.FilterConfig {
	return &eventpb.FilterConfig{
		CaseSensitive:  false,
		ForceReplicate: false,
		FilterConfig: &eventpb.InnerFilterConfig{
			Rules:            []string{"*.*"},
			IgnoreTxnStartTs: []uint64{},
			EventFilters:     []*eventpb.EventFilterRule{},
		},
	}
}

func createTestFilterConfigWithRules(rules []string) *eventpb.FilterConfig {
	cfg := createTestFilterConfig()
	cfg.FilterConfig.Rules = rules
	return cfg
}

func createTestFilterConfigWithEventFilters(eventFilters []*eventpb.EventFilterRule) *eventpb.FilterConfig {
	cfg := createTestFilterConfig()
	cfg.FilterConfig.EventFilters = eventFilters
	return cfg
}

func createTestEventFilterRule() *eventpb.EventFilterRule {
	return &eventpb.EventFilterRule{
		Matcher:                  []string{"test.*"},
		IgnoreEvent:              []string{},
		IgnoreSql:                []string{},
		IgnoreInsertValueExpr:    "",
		IgnoreUpdateNewValueExpr: "",
		IgnoreUpdateOldValueExpr: "",
		IgnoreDeleteValueExpr:    "",
	}
}

func createTestChangeFeedID(name string) common.ChangeFeedID {
	return common.NewChangeFeedIDWithName(name)
}

// Helper functions to verify filter instances
func assertFilterInstancesEqual(t *testing.T, filter1, filter2 Filter) {
	// Use pointer comparison to verify they are the same instance
	require.Equal(t, filter1, filter2, "Filter instances should be the same")
}

func assertFilterInstancesDifferent(t *testing.T, filter1, filter2 Filter) {
	// Use pointer comparison to verify they are different instances
	require.NotEqual(t, filter1, filter2, "Filter instances should be different")
}

func TestSharedFilterStorage_SameConfigSharesFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := createTestFilterConfig()
	timeZone := "UTC"

	// First call - should create new filter
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with same config - should return same filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return the same filter instance
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_SameConfigContentSharesFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig() // Create separate config object with same content
	timeZone := "UTC"

	// First call - should create new filter
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with different config object but same content - should return same filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return the same filter instance due to content equality
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_DifferentConfigCreatesNewFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.CaseSensitive = true // Different configuration
	timeZone := "UTC"

	// First call with cfg1
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with cfg2 - should create new filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_DifferentChangeFeedIDCreatesIndependentFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID1 := createTestChangeFeedID("test-changefeed-1")
	changeFeedID2 := createTestChangeFeedID("test-changefeed-2")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfigWithRules([]string{"test.*"}) // Create different config content
	timeZone := "UTC"

	// Call with different changeFeedID and different config objects with different content
	filter1, err := storage.GetOrSetFilter(changeFeedID1, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	filter2, err := storage.GetOrSetFilter(changeFeedID2, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify different changeFeedID with different config content creates different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_CaseSensitiveFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.CaseSensitive = true
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_ForceReplicateFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.ForceReplicate = true
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_RulesFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfigWithRules([]string{"*.*"})
	cfg2 := createTestFilterConfigWithRules([]string{"test.*"})
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_IgnoreTxnStartTsFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.FilterConfig.IgnoreTxnStartTs = []uint64{123456}
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_EventFiltersFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{createTestEventFilterRule()})
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_ComplexEventFiltersDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	timeZone := "UTC"

	// Create first config with one event filter
	eventFilter1 := createTestEventFilterRule()
	eventFilter1.Matcher = []string{"test1.*"}
	cfg1 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{eventFilter1})

	// Create second config with different event filter
	eventFilter2 := createTestEventFilterRule()
	eventFilter2.Matcher = []string{"test2.*"}
	cfg2 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{eventFilter2})

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_EmptyConfigTest(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := &eventpb.FilterConfig{
		CaseSensitive:  false,
		ForceReplicate: false,
		FilterConfig: &eventpb.InnerFilterConfig{
			Rules:            []string{},
			IgnoreTxnStartTs: []uint64{},
			EventFilters:     []*eventpb.EventFilterRule{},
		},
	}
	timeZone := "UTC"

	filter, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter)
}

func TestSharedFilterStorage_ConcurrentAccessTest(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := createTestFilterConfig()
	timeZone := "UTC"

	// Test concurrent access with same config
	done := make(chan bool, 2)
	var filter1, filter2 Filter
	var err1, err2 error

	go func() {
		filter1, err1 = storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
		done <- true
	}()

	go func() {
		filter2, err2 = storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NotNil(t, filter1)
	require.NotNil(t, filter2)

	// Both should return the same filter instance due to sharing
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_TimeZoneDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	// Create config with expression filters to make timezone difference more apparent
	cfg := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{
		{
			Matcher:                  []string{"test.*"},
			IgnoreEvent:              []string{},
			IgnoreSql:                []string{},
			IgnoreInsertValueExpr:    "id > 1000",
			IgnoreUpdateNewValueExpr: "",
			IgnoreUpdateOldValueExpr: "",
			IgnoreDeleteValueExpr:    "",
		},
	})
	timeZone1 := "UTC"
	timeZone2 := "Asia/Shanghai"

	// First call with UTC timezone
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone1)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with different timezone - should create new filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone2)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify different timezone creates different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}
