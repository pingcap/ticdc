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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaskSensitiveDataInURIForError(t *testing.T) {
	require.Equal(t, "", MaskSensitiveDataInURIForError(""))
	require.Equal(t, "abc", MaskSensitiveDataInURIForError("abc"))
	require.Equal(t,
		"mysql://root:xxxxx@127.0.0.1:3306/?sasl-password=xxxxx",
		MaskSensitiveDataInURIForError("mysql://root:verysecure@127.0.0.1:3306/?sasl-password=rawsecret"))
	require.Equal(t, "<invalid uri>", MaskSensitiveDataInURIForError("mysql://root:verysecure@127.0.0.1/%zz"))
}
