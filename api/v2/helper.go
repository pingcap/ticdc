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

package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tiflow/pkg/httputil"
	"go.uber.org/zap"
)

func getChangeFeed(host, cfName string) (ChangeFeedInfo, error) {
	security := config.GetGlobalServerConfig().Security

	uri := fmt.Sprintf("/api/v2/changefeeds/%s", cfName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx, "GET", uri, nil)
	if err != nil {
		return ChangeFeedInfo{}, err
	}
	req.URL.Host = host

	if tls, _ := security.ToTLSConfigWithVerify(); tls != nil {
		req.URL.Scheme = "https"
	} else {
		req.URL.Scheme = "http"
	}

	client, err := httputil.NewClient(security)
	if err != nil {
		return ChangeFeedInfo{}, err
	}

	log.Info("Send request to coordinator to get changefeed info",
		zap.String("host", host),
		zap.String("uri", uri),
		zap.String("schema", req.URL.Scheme),
	)

	resp, err := client.Do(req)
	if err != nil {
		log.Error("failed to get changefeed", zap.Error(err), zap.String("uri", uri))
		return ChangeFeedInfo{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("failed to read changefeed response", zap.Error(err), zap.String("uri", uri))
		return ChangeFeedInfo{}, err
	}

	var cfInfo ChangeFeedInfo
	if err := json.Unmarshal(body, &cfInfo); err != nil {
		log.Error("failed to unmarshal changefeed response", zap.Error(err), zap.String("uri", uri))
		return ChangeFeedInfo{}, err
	}

	return cfInfo, nil
}
