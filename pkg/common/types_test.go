package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChangeFeedDisplayNameJSONCompatibility(t *testing.T) {
	t.Parallel()

	// Scenario: TiCDC historically persisted changefeed IDs using "namespace".
	// Newer versions renamed the dimension to "keyspace". We must be able to
	// read both to avoid cross-version upgrades/rollbacks making changefeeds
	// "disappear" (e.g., keyspace becomes empty and gets filtered out).

	t.Run("unmarshal legacy namespace", func(t *testing.T) {
		t.Parallel()

		// Step: decode legacy JSON that only contains "namespace".
		var got ChangeFeedDisplayName
		require.NoError(t, json.Unmarshal([]byte(`{"name":"cf","namespace":"default"}`), &got))

		// Expect: Keyspace is filled from the legacy field.
		require.Equal(t, ChangeFeedDisplayName{Name: "cf", Keyspace: "default"}, got)
	})

	t.Run("unmarshal new keyspace", func(t *testing.T) {
		t.Parallel()

		// Step: decode newer JSON that only contains "keyspace".
		var got ChangeFeedDisplayName
		require.NoError(t, json.Unmarshal([]byte(`{"name":"cf","keyspace":"default"}`), &got))

		// Expect: Keyspace is filled from the new field.
		require.Equal(t, ChangeFeedDisplayName{Name: "cf", Keyspace: "default"}, got)
	})

	t.Run("keyspace wins when both exist", func(t *testing.T) {
		t.Parallel()

		// Step: decode mixed JSON produced/consumed by different versions.
		var got ChangeFeedDisplayName
		require.NoError(t, json.Unmarshal([]byte(`{"name":"cf","namespace":"ns","keyspace":"ks"}`), &got))

		// Expect: new field takes precedence to match current semantics.
		require.Equal(t, ChangeFeedDisplayName{Name: "cf", Keyspace: "ks"}, got)
	})

	t.Run("marshal emits both fields", func(t *testing.T) {
		t.Parallel()

		// Step: encode a display name.
		data, err := json.Marshal(ChangeFeedDisplayName{Name: "cf", Keyspace: "default"})
		require.NoError(t, err)

		// Expect: both fields exist so that either a legacy ("namespace") or a newer
		// ("keyspace") TiCDC binary can read metadata without rewriting it first.
		var got map[string]any
		require.NoError(t, json.Unmarshal(data, &got))
		require.Equal(t, "cf", got["name"])
		require.Equal(t, "default", got["namespace"])
		require.Equal(t, "default", got["keyspace"])
	})
}
