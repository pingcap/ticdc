// Copyright 2020 PingCAP, Inc.
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

// Description:
// The program tests whether TiCDC can successfully move all tables replicated
// by one capture node to another capture node, ensuring the original capture
// becomes empty after the movement.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/log"
	v2 "github.com/pingcap/ticdc/api/v2"
	clientv2 "github.com/pingcap/ticdc/pkg/api/v2"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	cdcAddr  = flag.String("cdc-addr", "127.0.0.1:8300", "CDC API address and port")
	logLevel = flag.String("log-level", "debug", "Set log level of the logger")
)

// This program moves all tables replicated by a certain capture to other captures,
// and makes sure that the original capture becomes empty.
func main() {
	flag.Parse()
	if strings.ToLower(*logLevel) == "debug" {
		log.SetLevel(zapcore.DebugLevel)
	}

	log.Info("move table test starting...")

	cluster, err := newCluster()
	if err != nil {
		log.Fatal("failed to create cluster info", zap.Error(err))
	}

	sourceNode, targetNode := getSourceAndTargetNode(cluster)

	err = cluster.moveAllTables(sourceNode, targetNode)
	if err != nil {
		log.Fatal("failed to move tables", zap.Error(err))
	}

	log.Info("all tables are moved", zap.String("sourceNode", sourceNode), zap.String("targetNode", targetNode))

	cluster.checkSourceEmpty(sourceNode)
}

type tableInfo struct {
	changefeedNameSpace string
	changefeedName      string

	// table id
	id int64
}

// cluster is a struct that contains the ticdc cluster's
// api client and the table info of each node
// it is used to move all tables from source node to target node
type cluster struct {
	client  clientv2.APIV2Interface
	servers map[string][]tableInfo
}

func newCluster() (*cluster, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := clientv2.NewAPIClient(*cdcAddr, nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var servers []v2.Capture
	for {
		servers, err = client.Captures().List(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(servers) > 1 {
			break
		}
		log.Info("waiting for servers to be ready", zap.Int("serverNum", len(servers)))
		time.Sleep(1 * time.Second)
	}

	serversMap := make(map[string][]tableInfo)
	for _, server := range servers {
		serversMap[server.ID] = make([]tableInfo, 0)
	}

	changefeeds, err := client.Changefeeds().List(ctx, "default", "all")
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, changefeed := range changefeeds {
		nodeTableInfos, err := listTables(*cdcAddr, changefeed.ID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		log.Info("list tables", zap.Any("nodeTableInfos", nodeTableInfos))

		for _, nodeTableInfo := range nodeTableInfos {
			for _, tableID := range nodeTableInfo.TableIDs {
				serversMap[nodeTableInfo.NodeID] = append(serversMap[nodeTableInfo.NodeID], tableInfo{
					changefeedNameSpace: changefeed.Namespace,
					changefeedName:      changefeed.ID,
					id:                  tableID,
				})
			}
		}

	}

	ret := &cluster{
		client:  client,
		servers: serversMap,
	}

	log.Info("new cluster initialized")

	return ret, nil
}

// moveAllTables moves all tables from source node to target node
func (c *cluster) moveAllTables(sourceNode, targetNode string) error {
	for _, table := range c.servers[sourceNode] {
		if table.id == 0 {
			// table trigger dispatcher is not support to move, except the maintainer is crashed
			continue
		}
		ctx := context.Background()
		err := c.
			client.
			Changefeeds().
			MoveTable(ctx, table.changefeedNameSpace, table.changefeedName, table.id, targetNode)

		log.Info("move table",
			zap.String("sourceNode", sourceNode),
			zap.String("targetNode", targetNode),
			zap.Int64("tableID", table.id),
			zap.Error(err))

		if err != nil {
			log.Error("failed to move table", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

// listTables invokes the following API to get the mapping from
// captureIDs to tableIDs:
//
//	GET /api/v2/changefeed/{changefeed_id}/tables
func listTables(
	host string,
	changefeedID string,
) ([]v2.NodeTableInfo, error) {
	httpClient, err := httputil.NewClient(&security.Credential{ /* no TLS */ })
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requestURL := fmt.Sprintf("http://%s/api/v2/changefeeds/%s/tables", host, changefeedID)
	resp, err := httpClient.Get(ctx, requestURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Trace(
			errors.Errorf("HTTP API returned error status: %d, url: %s", resp.StatusCode, requestURL))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ret v2.ListResponse[v2.NodeTableInfo]
	err = json.Unmarshal(bodyBytes, &ret)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret.Items, nil
}

func (c *cluster) checkSourceEmpty(sourceNode string) {
	clusterForCheck, err := newCluster()
	if err != nil {
		log.Fatal("failed to create cluster info", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sourceTables := clusterForCheck.servers[sourceNode]
			if len(sourceTables) != 0 {
				if len(sourceTables) == 1 && sourceTables[0].id == 0 {
					log.Info("source capture is empty, done")
					return
				}
				log.Info("source capture is not empty, retrying", zap.Any("sourceTables", sourceTables))
			} else {
				log.Info("source capture is empty, done")
				return
			}
		case <-ctx.Done():
			log.Fatal("context done")
		}
	}
}

func getSourceAndTargetNode(cluster *cluster) (string, string) {
	var sourceNode string
	for server, tables := range cluster.servers {
		if len(tables) == 0 {
			continue
		}
		sourceNode = server
		break
	}

	var targetNode string
	for candidateNode := range cluster.servers {
		if candidateNode != sourceNode {
			targetNode = candidateNode
		}
	}

	if targetNode == "" {
		log.Fatal("no target, unexpected")
	}

	log.Info("Get source and target node", zap.String("sourceNode", sourceNode), zap.String("targetNode", targetNode))

	return sourceNode, targetNode
}
