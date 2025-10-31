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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redact

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactMode(t *testing.T) {
	// Test initial state
	require.Equal(t, Off, GetRedactMode())
	require.False(t, NeedRedact())

	// Test setting modes
	SetRedactMode(On)
	require.Equal(t, On, GetRedactMode())
	require.True(t, NeedRedact())

	SetRedactMode(Marker)
	require.Equal(t, Marker, GetRedactMode())
	require.True(t, NeedRedact())

	SetRedactMode(Off)
	require.Equal(t, Off, GetRedactMode())
	require.False(t, NeedRedact())
}

func TestString(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    string
		expected string
	}{
		{Off, "sensitive data", "sensitive data"},
		{On, "sensitive data", "?"},
		{Marker, "sensitive data", "‹sensitive data›"},
		{Off, "", ""},
		{On, "", "?"},
		{Marker, "", "‹›"},
		{Off, "SELECT * FROM users WHERE id = 123", "SELECT * FROM users WHERE id = 123"},
		{On, "SELECT * FROM users WHERE id = 123", "?"},
		{Marker, "SELECT * FROM users WHERE id = 123", "‹SELECT * FROM users WHERE id = 123›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := String(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestStringWithMarkerEscaping(t *testing.T) {
	SetRedactMode(Marker)

	testCases := []struct {
		input    string
		expected string
	}{
		{"normal text", "‹normal text›"},
		{"text with ‹ marker", "‹text with ‹‹ marker›"},
		{"text with › marker", "‹text with ›› marker›"},
		{"‹both› markers", "‹‹‹both›› markers›"},
		{"‹", "‹‹‹›"},
		{"›", "‹›››"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := String(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestKey(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    []byte
		expected string
	}{
		{Off, []byte{0x12, 0x34, 0xAB, 0xCD}, "1234ABCD"},
		{On, []byte{0x12, 0x34, 0xAB, 0xCD}, "?"},
		{Marker, []byte{0x12, 0x34, 0xAB, 0xCD}, "‹1234ABCD›"},
		{Off, nil, "NULL"},
		{On, nil, "NULL"},
		{Marker, nil, "NULL"},
		{Off, []byte{}, ""},
		{On, []byte{}, "?"},
		{Marker, []byte{}, "‹›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%v", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Key(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestValue(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    string
		expected string
	}{
		{Off, "password123", "password123"},
		{On, "password123", "?"},
		{Marker, "password123", "‹password123›"},
		{Off, "user@example.com", "user@example.com"},
		{On, "user@example.com", "?"},
		{Marker, "user@example.com", "‹user@example.com›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Value(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestBytes(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    []byte
		expected string
	}{
		{Off, []byte("test data"), "test data"},
		{On, []byte("test data"), "?"},
		{Marker, []byte("test data"), "‹test data›"},
		{Off, nil, "NULL"},
		{On, nil, "NULL"},
		{Marker, nil, "NULL"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.mode, string(tc.input)), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Bytes(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestSQL(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    string
		expected string
	}{
		{Off, "INSERT INTO users VALUES (1, 'john')", "INSERT INTO users VALUES (1, 'john')"},
		{On, "INSERT INTO users VALUES (1, 'john')", "?"},
		{Marker, "INSERT INTO users VALUES (1, 'john')", "‹INSERT INTO users VALUES (1, 'john')›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := SQL(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestArgs(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    []interface{}
		expected string
	}{
		{Off, []interface{}{}, "()"},
		{On, []interface{}{}, "()"},
		{Marker, []interface{}{}, "()"},
		{Off, []interface{}{1, "john", nil}, "(1, john, NULL)"},
		{On, []interface{}{1, "john", nil}, "(?, ?, NULL)"},
		{Marker, []interface{}{1, "john", nil}, "(‹1›, ‹john›, NULL)"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%v", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Args(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAny(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    any
		expected string
	}{
		{Off, "test value", "test value"},
		{On, "test value", "?"},
		{Marker, "test value", "‹test value›"},
		{Off, 12345, "12345"},
		{On, 12345, "?"},
		{Marker, 12345, "‹12345›"},
		{Off, nil, "NULL"},
		{On, nil, "NULL"},
		{Marker, nil, "NULL"},
		{Off, []byte("data"), "[100 97 116 97]"},
		{On, []byte("data"), "?"},
		{Marker, []byte("data"), "‹[100 97 116 97]›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%v", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Any(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestAnys(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    []any
		expected []string
	}{
		{Off, []any{}, []string{}},
		{On, []any{}, []string{}},
		{Marker, []any{}, []string{}},
		{Off, []any{1, "test", nil}, []string{"1", "test", "NULL"}},
		{On, []any{1, "test", nil}, []string{"?", "?", "NULL"}},
		{Marker, []any{1, "test", nil}, []string{"‹1›", "‹test›", "NULL"}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%v", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			result := Anys(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

type testStringer struct {
	value string
}

func (ts testStringer) String() string {
	return ts.value
}

func TestStringer(t *testing.T) {
	testObj := testStringer{"sensitive info"}

	testCases := []struct {
		mode     RedactMode
		expected string
	}{
		{Off, "sensitive info"},
		{On, "?"},
		{Marker, "‹sensitive info›"},
	}

	for _, tc := range testCases {
		t.Run(string(tc.mode), func(t *testing.T) {
			stringer := Stringer(tc.mode, testObj)
			result := stringer.String()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestStringerWithNil(t *testing.T) {
	SetRedactMode(On)
	stringer := Stringer(On, nil)
	result := stringer.String()
	require.Equal(t, "?", result)
}

func TestStringerWithCurrentMode(t *testing.T) {
	testObj := testStringer{"test data"}

	SetRedactMode(On)
	stringer := StringerWithCurrentMode(testObj)
	require.Equal(t, "?", stringer.String())

	SetRedactMode(Marker)
	stringer = StringerWithCurrentMode(testObj)
	require.Equal(t, "‹test data›", stringer.String())
}

func TestInitRedact(t *testing.T) {
	// Test enabling redaction
	InitRedact(true)
	require.Equal(t, On, GetRedactMode())

	// Test disabling redaction
	InitRedact(false)
	require.Equal(t, Off, GetRedactMode())
}

func TestParseRedactMode(t *testing.T) {
	testCases := []struct {
		input    string
		expected RedactMode
	}{
		// On variations
		{"ON", On},
		{"on", On},
		{"On", On},
		{"TRUE", On},
		{"true", On},
		{"1", On},
		{"ENABLE", On},
		{"enable", On},
		{"ENABLED", On},
		{"enabled", On},

		// Off variations
		{"OFF", Off},
		{"off", Off},
		{"Off", Off},
		{"FALSE", Off},
		{"false", Off},
		{"0", Off},
		{"DISABLE", Off},
		{"disable", Off},
		{"DISABLED", Off},
		{"disabled", Off},
		{"", Off},
		{"   ", Off},

		// Marker variations
		{"MARKER", Marker},
		{"marker", Marker},
		{"Marker", Marker},
		{"MARK", Marker},
		{"mark", Marker},

		// Invalid/unknown values default to Off
		{"invalid", Off},
		{"unknown", Off},
		{"random", Off},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := ParseRedactMode(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestIsValidRedactMode(t *testing.T) {
	validModes := []string{
		"ON", "on", "On", "TRUE", "true", "1", "ENABLE", "enable", "ENABLED", "enabled",
		"OFF", "off", "Off", "FALSE", "false", "0", "DISABLE", "disable", "DISABLED", "disabled", "",
		"MARKER", "marker", "Marker", "MARK", "mark",
	}

	invalidModes := []string{
		"invalid", "unknown", "random", "yes", "no", "2", "-1",
	}

	for _, mode := range validModes {
		t.Run("valid_"+mode, func(t *testing.T) {
			require.True(t, IsValidRedactMode(mode), "Expected %s to be valid", mode)
		})
	}

	for _, mode := range invalidModes {
		t.Run("invalid_"+mode, func(t *testing.T) {
			require.False(t, IsValidRedactMode(mode), "Expected %s to be invalid", mode)
		})
	}
}

func TestWriteRedact(t *testing.T) {
	testCases := []struct {
		mode     RedactMode
		input    string
		expected string
	}{
		{Off, "test data", "test data"},
		{On, "test data", "?"},
		{Marker, "test data", "‹test data›"},
		{Off, "", ""},
		{On, "", "?"},
		{Marker, "", "‹›"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.mode, tc.input), func(t *testing.T) {
			SetRedactMode(tc.mode)
			var builder strings.Builder
			WriteRedact(&builder, tc.input)
			result := builder.String()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestConcurrentAccess(t *testing.T) {
	// Test concurrent access to redaction mode
	const numGoroutines = 100
	const numIterations = 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Start multiple goroutines that read and write redaction mode
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				mode := RedactMode([]string{"OFF", "ON", "MARKER"}[j%3])
				SetRedactMode(mode)
				result := String("test")

				// Verify the result is one of the expected values
				require.Contains(t, []string{"test", "?", "‹test›"}, result)

				// Test other functions
				_ = GetRedactMode()
				_ = NeedRedact()
				_ = Key([]byte("key"))
				_ = Value("value")
			}
		}(i)
	}

	wg.Wait()
}

func TestRedactModeConstants(t *testing.T) {
	// Ensure constants match expected string values
	require.Equal(t, "OFF", string(Off))
	require.Equal(t, "ON", string(On))
	require.Equal(t, "MARKER", string(Marker))
}

func BenchmarkString(b *testing.B) {
	testData := "sensitive information that needs redaction"

	benchmarks := []struct {
		name string
		mode RedactMode
	}{
		{"Off", Off},
		{"On", On},
		{"Marker", Marker},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			SetRedactMode(bm.mode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = String(testData)
			}
		})
	}
}

func BenchmarkKey(b *testing.B) {
	keyData := []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}

	benchmarks := []struct {
		name string
		mode RedactMode
	}{
		{"Off", Off},
		{"On", On},
		{"Marker", Marker},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			SetRedactMode(bm.mode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = Key(keyData)
			}
		})
	}
}

func BenchmarkSQL(b *testing.B) {
	sqlData := "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"

	benchmarks := []struct {
		name string
		mode RedactMode
	}{
		{"Off", Off},
		{"On", On},
		{"Marker", Marker},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			SetRedactMode(bm.mode)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = SQL(sqlData)
			}
		})
	}
}
