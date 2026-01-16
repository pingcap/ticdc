// Copyright 2020 PingCAP, Inc.
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

package security

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestGetCommonName(t *testing.T) {
	cd := &Credential{
		CAPath:   "../../tests/integration_tests/_certificates/ca.pem",
		CertPath: "../../tests/integration_tests/_certificates/server.pem",
		KeyPath:  "../../tests/integration_tests/_certificates/server-key.pem",
	}
	cn, err := cd.getSelfCommonName()
	require.Nil(t, err)
	require.Equal(t, "tidb-server", cn)

	cd.CertPath = "../../tests/integration_tests/_certificates/server-key.pem"
	_, err = cd.getSelfCommonName()
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "failed to decode PEM block to certificate")
}

func TestRedactInfoLogTypeUnmarshalTOML(t *testing.T) {
	t.Run("boolean true maps to ON", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalTOML(true)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("ON"), r)
	})

	t.Run("boolean false maps to OFF", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalTOML(false)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("OFF"), r)
	})

	t.Run("string values are canonicalized", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected RedactInfoLogType
		}{
			{"off", "OFF"},
			{"on", "ON"},
			{"marker", "MARKER"},
			{"MARKER", "MARKER"},
			{"OFF", "OFF"},
			{"ON", "ON"},
			// Aliases
			{"true", "ON"},
			{"false", "OFF"},
			{"enable", "ON"},
			{"disable", "OFF"},
		}
		for _, tc := range testCases {
			var r RedactInfoLogType
			err := r.UnmarshalTOML(tc.input)
			require.NoError(t, err, "input: %s", tc.input)
			require.Equal(t, tc.expected, r, "input: %s", tc.input)
		}
	})

	t.Run("empty string remains empty", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalTOML("")
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType(""), r)
	})

	t.Run("invalid string returns error", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalTOML("invalid_mode")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid redact mode")
	})

	t.Run("invalid type returns error", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalTOML(123)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid redact-info-log value type")
	})
}

func TestRedactInfoLogTypeUnmarshalJSON(t *testing.T) {
	t.Run("boolean true maps to ON", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalJSON([]byte("true"))
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("ON"), r)
	})

	t.Run("boolean false maps to OFF", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalJSON([]byte("false"))
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("OFF"), r)
	})

	t.Run("string values are canonicalized", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected RedactInfoLogType
		}{
			{`"off"`, "OFF"},
			{`"on"`, "ON"},
			{`"marker"`, "MARKER"},
			{`"MARKER"`, "MARKER"},
			// Aliases
			{`"true"`, "ON"},
			{`"false"`, "OFF"},
		}
		for _, tc := range testCases {
			var r RedactInfoLogType
			err := r.UnmarshalJSON([]byte(tc.input))
			require.NoError(t, err, "input: %s", tc.input)
			require.Equal(t, tc.expected, r, "input: %s", tc.input)
		}
	})

	t.Run("empty string remains empty", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalJSON([]byte(`""`))
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType(""), r)
	})

	t.Run("invalid string returns error", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalJSON([]byte(`"invalid_mode"`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid redact mode")
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		var r RedactInfoLogType
		err := r.UnmarshalJSON([]byte("invalid"))
		require.Error(t, err)
	})
}

func TestRedactInfoLogTypeMarshalJSON(t *testing.T) {
	testCases := []struct {
		input    RedactInfoLogType
		expected string
	}{
		{"ON", `"ON"`},
		{"OFF", `"OFF"`},
		{"MARKER", `"MARKER"`},
		{"", `""`},
	}
	for _, tc := range testCases {
		result, err := tc.input.MarshalJSON()
		require.NoError(t, err)
		require.Equal(t, tc.expected, string(result))
	}
}

func TestCredentialJSONMarshal(t *testing.T) {
	t.Run("empty RedactInfoLog is omitted from JSON", func(t *testing.T) {
		cred := &Credential{
			CAPath:   "/path/to/ca.pem",
			CertPath: "/path/to/cert.pem",
			KeyPath:  "/path/to/key.pem",
		}
		data, err := json.Marshal(cred)
		require.NoError(t, err)
		require.NotContains(t, string(data), "redact-info-log")
	})

	t.Run("non-empty RedactInfoLog is included in JSON", func(t *testing.T) {
		cred := &Credential{
			CAPath:        "/path/to/ca.pem",
			RedactInfoLog: "MARKER",
		}
		data, err := json.Marshal(cred)
		require.NoError(t, err)
		require.Contains(t, string(data), `"redact-info-log":"MARKER"`)
	})

	t.Run("RedactInfoLog roundtrip preserves canonical value", func(t *testing.T) {
		original := &Credential{
			CAPath:        "/path/to/ca.pem",
			RedactInfoLog: "ON",
		}
		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded Credential
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("ON"), decoded.RedactInfoLog)
	})

	t.Run("JSON boolean true unmarshals to ON", func(t *testing.T) {
		jsonData := `{"ca-path":"/ca.pem","redact-info-log":true}`
		var cred Credential
		err := json.Unmarshal([]byte(jsonData), &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("ON"), cred.RedactInfoLog)
	})

	t.Run("JSON boolean false unmarshals to OFF", func(t *testing.T) {
		jsonData := `{"ca-path":"/ca.pem","redact-info-log":false}`
		var cred Credential
		err := json.Unmarshal([]byte(jsonData), &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("OFF"), cred.RedactInfoLog)
	})
}

func TestCredentialTOMLUnmarshal(t *testing.T) {
	t.Run("TOML boolean true unmarshals to ON", func(t *testing.T) {
		tomlData := `
ca-path = "/ca.pem"
redact-info-log = true
`
		var cred Credential
		_, err := toml.Decode(tomlData, &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("ON"), cred.RedactInfoLog)
	})

	t.Run("TOML boolean false unmarshals to OFF", func(t *testing.T) {
		tomlData := `
ca-path = "/ca.pem"
redact-info-log = false
`
		var cred Credential
		_, err := toml.Decode(tomlData, &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("OFF"), cred.RedactInfoLog)
	})

	t.Run("TOML string marker unmarshals correctly", func(t *testing.T) {
		tomlData := `
ca-path = "/ca.pem"
redact-info-log = "MARKER"
`
		var cred Credential
		_, err := toml.Decode(tomlData, &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType("MARKER"), cred.RedactInfoLog)
	})

	t.Run("TOML missing field leaves empty", func(t *testing.T) {
		tomlData := `
ca-path = "/ca.pem"
`
		var cred Credential
		_, err := toml.Decode(tomlData, &cred)
		require.NoError(t, err)
		require.Equal(t, RedactInfoLogType(""), cred.RedactInfoLog)
	})
}
