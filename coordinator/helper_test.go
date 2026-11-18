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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestGetChangefeedErrorMetricLabelsIncludesErrorTime verifies that a current warning is exported
// with a stable UTC occurrence timestamp, normalized message text, and the rest of the panel labels.
func TestGetChangefeedErrorMetricLabelsIncludesErrorTime(t *testing.T) {
	errorTime := time.Date(2026, time.May, 18, 10, 11, 12, 0, time.FixedZone("CST", 8*60*60))
	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: changefeedID,
		State:        config.StateWarning,
		Warning: &config.RunningError{
			Time:    errorTime,
			Code:    "CDC:ErrSinkRetry",
			Message: "sink retry \n warning",
		},
	}

	labels, ok := getChangefeedErrorMetricLabels(info)
	require.True(t, ok)
	require.Equal(t, changefeedErrorMetricLabels{
		keyspace:   common.DefaultKeyspaceName,
		changefeed: "test-changefeed",
		state:      string(config.StateWarning),
		errorTime:  "2026-05-18T02:11:12Z",
		code:       "CDC:ErrSinkRetry",
		message:    "sink retry warning",
	}, labels)
}

// TestNormalizeChangefeedErrorMetricTimeHandlesZeroTime verifies that historical or incomplete
// error records without an occurrence timestamp render as an empty field instead of a misleading date.
func TestNormalizeChangefeedErrorMetricTimeHandlesZeroTime(t *testing.T) {
	require.Equal(t, "", normalizeChangefeedErrorMetricTime(time.Time{}))
}

func TestNormalizeChangefeedErrorMetricMessageProducesValidUTF8(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{
			name:    "normalize whitespace",
			message: "sink retry \n warning",
			want:    "sink retry warning",
		},
		{
			name:    "replace invalid UTF-8",
			message: string([]byte{'a', 0xff, 'b'}),
			want:    "a\uFFFDb",
		},
		{
			name:    "truncate ASCII",
			message: strings.Repeat("a", changefeedErrorMetricMsgLimit+1),
			want:    strings.Repeat("a", changefeedErrorMetricMsgLimit-3) + "...",
		},
		{
			name:    "truncate at rune boundary",
			message: strings.Repeat("a", changefeedErrorMetricMsgLimit-4) + "世ab",
			want:    strings.Repeat("a", changefeedErrorMetricMsgLimit-4) + "...",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeChangefeedErrorMetricMessage(test.message)
			require.Equal(t, test.want, got)
			require.LessOrEqual(t, len(got), changefeedErrorMetricMsgLimit)
			require.True(t, utf8.ValidString(got))
		})
	}
}
