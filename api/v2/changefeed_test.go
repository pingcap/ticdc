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
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaskSinkURIForError(t *testing.T) {
	sinkURI := "kafka://127.0.0.1:9092/topic?protocol=canal-json" +
		"&sasl-user=ticdc&sasl-password=verysecure&secret-access-key=rawsecret"

	maskedURI := maskSinkURIForError(sinkURI)
	require.NotContains(t, maskedURI, "verysecure")
	require.NotContains(t, maskedURI, "rawsecret")
	require.Contains(t, maskedURI, "sasl-password=xxxxx")
	require.Contains(t, maskedURI, "secret-access-key=xxxxx")
	require.Contains(t, maskedURI, "sasl-user=ticdc")

	invalidURI := "mysql://root:verysecure@127.0.0.1/%zz"
	require.Equal(t, "<invalid uri>", maskSinkURIForError(invalidURI))

	err := genSinkURIInvalidError(invalidURI, mustParseURLError(t, invalidURI))
	require.NotContains(t, err.Error(), "verysecure")
	require.Contains(t, err.Error(), "<invalid uri>")
	require.Contains(t, err.Error(), `parse "<invalid uri>"`)
	require.Contains(t, err.Error(), "invalid URL escape")
}

func mustParseURLError(t *testing.T, rawURL string) error {
	t.Helper()

	_, err := url.Parse(rawURL)
	require.Error(t, err)
	return err
}
