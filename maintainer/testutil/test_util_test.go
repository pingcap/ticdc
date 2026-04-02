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

package testutil

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/stretchr/testify/require"
)

// The helper stores a concrete messageCenter behind the interface, and the bug is
// specifically about whether its run context has already been canceled on return.
func getMessageCenterContext(t *testing.T, mc messaging.MessageCenter) context.Context {
	t.Helper()

	value := reflect.ValueOf(mc)
	require.Equal(t, reflect.Ptr, value.Kind())

	ctxField := value.Elem().FieldByName("ctx")
	require.True(t, ctxField.IsValid())
	require.True(t, ctxField.CanAddr())

	return *(*context.Context)(unsafe.Pointer(ctxField.UnsafeAddr()))
}

func TestSetUpTestServicesKeepsMessageCenterAlive(t *testing.T) {
	SetUpTestServices(t)

	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	ctx := getMessageCenterContext(t, mc)

	select {
	case <-ctx.Done():
		t.Fatalf("SetUpTestServices stored a closed message center in app context")
	default:
	}
}
