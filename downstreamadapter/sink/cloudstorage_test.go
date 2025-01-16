// Copyright 2025 PingCAP, Inc.
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

package sink

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
)

func newCloudStorageSinkForTest() (*CloudStorageSink, error) {
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	// csvProtocol := "csv-protocol"
	// sinkConfig := &config.SinkConfig{Protocol: &csvProtocol}

	sink := &CloudStorageSink{
		changefeedID: changefeedID,
	}
	go sink.Run(ctx)
	return sink, nil
}
