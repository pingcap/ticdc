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
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/stretchr/testify/require"
)

func TestSetUpTestServicesKeepsMessageCenterAlive(t *testing.T) {
	localNodeID := SetUpTestServices(t)

	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	require.NotNil(t, mc)

	const topic = "message-center-lifecycle-regression"
	handlerCalled := make(chan string, 1)
	mc.RegisterHandler(topic, func(_ context.Context, msg *messaging.TargetMessage) error {
		handlerCalled <- string(msg.To)
		return nil
	})
	defer mc.DeRegisterHandler(topic)

	err := mc.SendCommand(messaging.NewSingleTargetMessage(localNodeID, topic, &heartbeatpb.RemoveMaintainerRequest{}))
	require.NoError(t, err)

	select {
	case receivedTarget := <-handlerCalled:
		require.Equal(t, string(localNodeID), receivedTarget)
	case <-time.After(time.Second):
		t.Fatalf("SetUpTestServices stored a closed message center in app context")
	}
}
