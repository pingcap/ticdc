// Copyright 2022 PingCAP, Inc.
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

package errors

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestRFCCodeUnwrap(t *testing.T) {
	t.Parallel()

	err := WrapError(ErrKafkaInvalidConfig, errors.New("base error"))

	code, ok := RFCCode(err)
	require.True(t, ok)
	require.Equal(t, ErrKafkaInvalidConfig.RFCCode(), code)

	wrapped := errors.Annotate(err, "annotated")
	code, ok = RFCCode(wrapped)
	require.True(t, ok)
	require.Equal(t, ErrKafkaInvalidConfig.RFCCode(), code)
}

func TestRFCCodeNoMatch(t *testing.T) {
	t.Parallel()

	_, ok := RFCCode(errors.New("plain error"))
	require.False(t, ok)
}
