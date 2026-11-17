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

package orchestrator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdEventKeysOmitValues(t *testing.T) {
	events := []*clientv3.Event{
		{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/key1"), Value: []byte("secret-value")}},
		{Type: mvccpb.DELETE, Kv: &mvccpb.KeyValue{Key: []byte("/key2")}},
	}
	keys := etcdEventKeys(events)
	require.Equal(t, []string{"PUT /key1", "DELETE /key2"}, keys)
	require.NotContains(t, strings.Join(keys, ","), "secret-value")
	require.Empty(t, etcdEventKeys(nil))
}
