// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb-dashboard/util/distro"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// Refer to https://github.com/pingcap/tidb/blob/release-7.5/pkg/domain/infosync/info.go#L78-L79.
	topologyTiDB    = serverinfo.TopologyInformationPath
	topologyTiDBTTL = serverinfo.TopologySessionTTL
	// defaultTimeout is the default timeout for etcd and mysql operations.
	defaultTimeout = time.Second * 2
)

type tidbInstance struct {
	IP   string
	Port uint
}

// AuthenticateMiddleware authenticates the request by query upstream TiDB.
func AuthenticateMiddleware(server server.Server) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		security := config.GetGlobalServerConfig().Security
		if security != nil && security.ClientUserRequired {
			if err := verify(ctx, server.GetEtcdClient().GetEtcdClient()); err != nil {
				ctx.IndentedJSON(http.StatusUnauthorized, api.NewHTTPError(err))
				ctx.Abort()
				return
			}
		}
		ctx.Next()
	}
}

func verify(ctx *gin.Context, etcdCli etcd.Client) error {
	// get the username and password from the authorization header
	username, password, ok := ctx.Request.BasicAuth()
	if !ok {
		errMsg := "please specify the user and password via authorization header"
		return errors.ErrCredentialNotFound.GenWithStackByArgs(errMsg)
	}

	allowed := false
	serverCfg := config.GetGlobalServerConfig()
	for _, user := range serverCfg.Security.ClientAllowedUser {
		if user == username {
			allowed = true
			break
		}
	}
	if !allowed {
		errMsg := "The user is not allowed."
		if username == "" {
			errMsg = "Empty username is not allowed."
		}
		return errors.ErrUnauthorized.GenWithStackByArgs(username, errMsg)
	}

	// TODO tenfyzhong 2025-10-15 15:07:48
	// The next gen kernel does not write topology info into etcd.
	if kerneltype.IsNextGen() {
		return nil
	}

	// verifyTiDBUser verify whether the username and password are valid in TiDB. It does the validation via
	// the successfully build of a connection with upstream TiDB with the username and password.
	tidbs, err := fetchTiDBTopology(ctx, etcdCli)
	if err != nil {
		return errors.Trace(err)
	}
	if len(tidbs) == 0 {
		return errors.New("tidb instance not found in topology, please check if the tidb is running")
	}

	for _, tidb := range tidbs {
		// connect tidb
		host := fmt.Sprintf("%s:%d", tidb.IP, tidb.Port)
		dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
		err = doVerify(dsnStr)
		if err == nil {
			return nil
		}
		if errors.IsAccessDeniedError(err) {
			// For access denied error, we can return immediately.
			// For other errors, we need to continue to verify the next tidb instance.
			return errors.ErrUnauthorized.GenWithStackByArgs(username, err.Error())
		}
	}
	return errors.ErrUnauthorized.GenWithStackByArgs(username, err.Error())
}

// fetchTiDBTopology parses the TiDB topology from etcd.
func fetchTiDBTopology(ctx context.Context, etcdClient etcd.Client) ([]tidbInstance, error) {
	ctx2, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	resp, err := etcdClient.Get(ctx2, topologyTiDB, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.ErrPDEtcdAPIError.Wrap(err)
	}

	nodesAlive := make(map[string]struct{}, len(resp.Kvs))
	nodesInfo := make(map[string]*tidbInstance, len(resp.Kvs))

	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if !strings.HasPrefix(key, topologyTiDB) {
			continue
		}
		// remainingKey looks like `ip:port/info` or `ip:port/ttl`.
		remainingKey := strings.TrimPrefix(key[len(topologyTiDB):], "/")
		keyParts := strings.Split(remainingKey, "/")
		if len(keyParts) != 2 {
			log.Warn("Ignored invalid topology key", zap.String("component", distro.R().TiDB), zap.String("key", key))
			continue
		}

		switch keyParts[1] {
		case "info":
			address := keyParts[0]
			hostname, port, err := util.ParseHostAndPortFromAddress(address)
			if err != nil {
				log.Warn("Ignored invalid tidb topology info entry",
					zap.String("key", key),
					zap.String("value", string(kv.Value)),
					zap.Error(err))
				continue
			}
			nodesInfo[keyParts[0]] = &tidbInstance{
				IP:   hostname,
				Port: port,
			}
		case "ttl":
			unixTimestampNano, err := strconv.ParseUint(string(kv.Value), 10, 64)
			if err != nil {
				log.Warn("Ignored invalid tidb topology TTL entry",
					zap.String("key", key),
					zap.String("value", string(kv.Value)),
					zap.Error(errors.ErrUnmarshalFailed.Wrap(err)))
				continue
			}
			t := time.Unix(0, int64(unixTimestampNano))
			if time.Since(t) > topologyTiDBTTL*time.Second {
				log.Warn("Ignored invalid tidb topology TTL entry",
					zap.String("key", key),
					zap.String("value", string(kv.Value)))
				continue
			}
			nodesAlive[keyParts[0]] = struct{}{}
		}
	}

	nodes := make([]tidbInstance, 0)
	for addr, info := range nodesInfo {
		if _, ok := nodesAlive[addr]; ok {
			nodes = append(nodes, *info)
		}
	}
	return nodes, nil
}

func doVerify(dsnStr string) error {
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return errors.Trace(err)
	}
	// Note: we use "preferred" here to make sure the connection is encrypted if possible. It is the same as the default
	// behavior of mysql client, refer to: https://dev.mysql.com/doc/refman/8.0/en/using-encrypted-connections.html.
	dsn.TLSConfig = "preferred"

	db, err := mysql.GetTestDB(dsn)
	if err != nil {
		return errors.Trace(err)
	}
	defer db.Close()

	rows, err := db.Query("SHOW STATUS LIKE '%Ssl_cipher';")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Warn("query Ssl_cipher close rows failed", zap.Error(err))
		}
		if rows.Err() != nil {
			log.Warn("query Ssl_cipher rows has error", zap.Error(rows.Err()))
		}
	}()

	var name, value string
	err = rows.Scan(&name, &value)
	if err != nil {
		log.Warn("failed to get ssl cipher", zap.Error(err),
			zap.String("username", dsn.User))
	}
	log.Info("verify tidb user successfully", zap.String("username", dsn.User),
		zap.String("sslCipherName", name), zap.String("sslCipherValue", value))
	return nil
}
