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

package v2

import (
	"bytes"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestJSONDurationTextRoundTrip verifies JSONDuration serializes to and parses
// from a human-readable string, which is what makes TOML output round-trippable.
func TestJSONDurationTextRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []time.Duration{
		10 * time.Minute,
		24 * time.Hour,
		5 * time.Second,
		30*time.Minute + 15*time.Second,
	}
	for _, dur := range cases {
		d := JSONDuration{duration: dur}
		text, err := d.MarshalText()
		require.NoError(t, err)

		var d2 JSONDuration
		require.NoError(t, d2.UnmarshalText(text))
		require.Equal(t, d.duration, d2.duration,
			"round-trip failed for %v: got text %q", dur, string(text))
	}
}

// TestJSONDurationUnmarshalTextInvalid verifies invalid duration text is rejected.
func TestJSONDurationUnmarshalTextInvalid(t *testing.T) {
	t.Parallel()

	var d JSONDuration
	require.Error(t, d.UnmarshalText([]byte("not-a-duration")))
	require.Error(t, d.UnmarshalText([]byte("")))
	require.Error(t, d.UnmarshalText([]byte("1d"))) // Go doesn't support "d" unit
}

// TestJSONDurationTOMLNotEmptyTable verifies a JSONDuration field encodes as a
// readable string rather than an empty TOML table (the failure mode that the
// MarshalText/UnmarshalText methods fix).
func TestJSONDurationTOMLNotEmptyTable(t *testing.T) {
	t.Parallel()

	cfg := &ReplicaConfig{
		SyncPointInterval: &JSONDuration{duration: 10 * time.Minute},
	}
	var buf bytes.Buffer
	require.NoError(t, toml.NewEncoder(&buf).Encode(cfg))
	out := buf.String()
	require.Contains(t, out, "sync-point-interval")
	require.Contains(t, out, "10m0s")
	require.NotContains(t, out, "[sync-point-interval]") // not an (empty) table
}

// TestChangeFeedInfoTOMLRoundTripToInternal is the highest-value test: it encodes
// a ChangeFeedInfo to TOML, then decodes the config back into the internal
// config.ReplicaConfig (the exact target that `changefeed create --config`
// parses), proving the TOML output is import-compatible.
func TestChangeFeedInfoTOMLRoundTripToInternal(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		ID:      "test-cf",
		SinkURI: "blackhole://",
		StartTs: 449999999999999999,
		Config: &ReplicaConfig{
			MemoryQuota:       util.AddressOf(uint64(1024)),
			CaseSensitive:     util.AddressOf(true),
			ForceReplicate:    util.AddressOf(true),
			CheckGCSafePoint:  util.AddressOf(false),
			SyncPointInterval: &JSONDuration{duration: 10 * time.Minute},
			Integrity: &IntegrityConfig{
				IntegrityCheckLevel:   util.AddressOf("correctness"),
				CorruptionHandleLevel: util.AddressOf("warn"),
			},
			Consistent: &ConsistentConfig{
				Level:             util.AddressOf("eventual"),
				MaxLogSize:        util.AddressOf(int64(128)),
				FlushIntervalInMs: util.AddressOf(int64(2000)),
				Storage:           util.AddressOf("s3://test"),
			},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, toml.NewEncoder(&buf).Encode(info))
	out := buf.String()

	// Top-level kebab-case keys and runtime field omissions.
	require.Contains(t, out, `sink-uri = "blackhole://"`)
	require.Contains(t, out, "start-ts")
	require.NotContains(t, out, "gid") // GID is omitted from TOML (toml:"-")

	// The [config] section must decode into the internal ReplicaConfig used by
	// `changefeed create --config`.
	var wrapper struct {
		Config config.ReplicaConfig `toml:"config"`
	}
	_, err := toml.Decode(out, &wrapper)
	require.NoError(t, err)
	require.Equal(t, uint64(1024), util.GetOrZero(wrapper.Config.MemoryQuota))
	require.True(t, util.GetOrZero(wrapper.Config.CaseSensitive))
	require.True(t, util.GetOrZero(wrapper.Config.ForceReplicate))
	require.NotNil(t, wrapper.Config.SyncPointInterval)
	require.Equal(t, 10*time.Minute, *wrapper.Config.SyncPointInterval)
	require.Equal(t, "correctness", util.GetOrZero(wrapper.Config.Integrity.IntegrityCheckLevel))
	require.Equal(t, "eventual", util.GetOrZero(wrapper.Config.Consistent.Level))
}

// TestDefaultConfigTOMLRoundTripToInternal encodes the full default replica
// config to TOML and decodes it into the internal config.ReplicaConfig, then
// asserts that no config-section key is left undecoded. This proves every TOML
// tag added to the api/v2 config tree matches what `changefeed create --config`
// parses — a guard against tag drift across the ~200 tagged fields.
func TestDefaultConfigTOMLRoundTripToInternal(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{ID: "cf", SinkURI: "blackhole://", Config: GetDefaultReplicaConfig()}
	var buf bytes.Buffer
	require.NoError(t, toml.NewEncoder(&buf).Encode(info))

	var wrapper struct {
		Config config.ReplicaConfig `toml:"config"`
	}
	meta, err := toml.Decode(buf.String(), &wrapper)
	require.NoError(t, err)

	// Top-level runtime fields (id, sink-uri, ...) are expected to be undecoded
	// against this wrapper; only config.* keys must all map to the internal type.
	var cfgUndecoded []string
	for _, k := range meta.Undecoded() {
		if len(k) > 0 && k[0] == "config" {
			cfgUndecoded = append(cfgUndecoded, k.String())
		}
	}
	require.Empty(t, cfgUndecoded, "config keys not decodable into internal config: %v", cfgUndecoded)
}

// TestRespondWithFormat verifies content negotiation: the default and explicit
// JSON requests yield JSON, while Accept: application/toml yields TOML.
func TestRespondWithFormat(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	obj := &ChangeFeedInfo{ID: "cf-1", SinkURI: "blackhole://"}

	cases := []struct {
		name        string
		accept      string
		wantCType   string
		wantContain string
	}{
		{"default json", "", "application/json", `"id":"cf-1"`},
		{"explicit json", "application/json", "application/json", `"id":"cf-1"`},
		{"toml", "application/toml", "application/toml", `id = "cf-1"`},
		{"toml with charset", "application/toml; charset=utf-8", "application/toml", `id = "cf-1"`},
		{"toml mixed case", "Application/TOML", "application/toml", `id = "cf-1"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/", nil)
			if tc.accept != "" {
				c.Request.Header.Set("Accept", tc.accept)
			}
			respondWithFormat(c, 200, obj)
			require.Equal(t, 200, w.Code)
			require.Contains(t, w.Header().Get("Content-Type"), tc.wantCType)
			require.Contains(t, w.Body.String(), tc.wantContain)
		})
	}
}
