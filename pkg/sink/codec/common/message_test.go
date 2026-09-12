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

package common

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestKafkaRecordBatchLength(t *testing.T) {
	message := NewMsg([]byte("k"), []byte("v"))
	require.Equal(t, len(message.Key)+len(message.Value)+84, message.KafkaRecordBatchLength())
}

func TestConfigMessageLength(t *testing.T) {
	message := NewMsg([]byte("k"), []byte("v"))
	codecConfig := NewConfig(config.ProtocolOpen)
	require.Equal(t, message.Length(), codecConfig.MessageLength(message))

	codecConfig.WithKafkaRecordBatchSize()
	require.Equal(t, message.KafkaRecordBatchLength(), codecConfig.MessageLength(message))
}
