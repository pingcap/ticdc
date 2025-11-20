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
	"fmt"
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/upstream"
	"go.uber.org/zap"
)

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

	serverCfg := config.GetGlobalServerConfig()
	allowed := slices.Contains(serverCfg.Security.ClientAllowedUser, username)
	if !allowed {
		errMsg := "The user is not allowed."
		if username == "" {
			errMsg = "Empty username is not allowed."
		}
		return errors.ErrUnauthorized.GenWithStackByArgs(username, errMsg)
	}

	keyspaceMeta := GetKeyspaceFromContext(ctx)

	// verifyTiDBUser verify whether the username and password are valid in TiDB. It does the validation via
	// the successfully build of a connection with upstream TiDB with the username and password.
	tidbs, err := upstream.FetchTiDBTopology(ctx, etcdCli, keyspaceMeta.Id)
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

	if rows.Next() {
		var name, value string
		err = rows.Scan(&name, &value)
		if err != nil {
			log.Warn("failed to get ssl cipher", zap.Error(err),
				zap.String("username", dsn.User))
		}
		log.Info("verify tidb user successfully", zap.String("username", dsn.User),
			zap.String("sslCipherName", name), zap.String("sslCipherValue", value))
	} else {
		log.Warn("no ssl cipher found", zap.String("username", dsn.User))
	}
	return nil
}
