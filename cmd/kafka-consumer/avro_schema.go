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

package main

import (
	"context"
	"database/sql"
	"net/url"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/planner/core" // Initialize expression evaluation for table metadata.
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"go.uber.org/zap"
)

// avroTableSchemas is accessed only by the writer's ordered DML/DDL flush loop.
// Old key-only Avro messages cannot recover the complete schema themselves.
// The downstream schema reflects the last DDL applied by this consumer.
type avroTableSchemas struct {
	ctx    context.Context
	db     *sql.DB
	tables map[[2]string]*commonType.TableInfo
	// version is a consumer schema epoch, not the upstream table version.
	version uint64
}

func (w *writer) initAvroTableSchemas(ctx context.Context, cfg *config.ChangefeedConfig) {
	if w.protocol != config.ProtocolAvro {
		return
	}
	uri, err := url.Parse(cfg.SinkURI)
	if err != nil {
		log.Panic("parse avro downstream URI failed", zap.Error(err))
	}
	_, db, err := mysql.NewMysqlConfigAndDB(ctx, cfg.ChangefeedID, uri, cfg)
	if err != nil {
		log.Panic("open avro schema connection failed", zap.Error(err))
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	w.avroSchemas = &avroTableSchemas{ctx: ctx, db: db, tables: make(map[[2]string]*commonType.TableInfo)}
	for _, progress := range w.progresses {
		progress.decoder.Unwrap().(interface {
			SetTableInfoProvider(func(string, string) (*commonType.TableInfo, error))
		}).SetTableInfoProvider(w.avroSchemas.get)
	}
}

func (s *avroTableSchemas) get(schema, table string) (*commonType.TableInfo, error) {
	key := [2]string{schema, table}
	if info := s.tables[key]; info != nil {
		return info, nil
	}
	createSQL, err := dbutil.GetCreateTableSQL(s.ctx, s.db, schema, table)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCodecDecode, err)
	}
	parser, err := dbutil.GetParserForDB(s.ctx, s.db)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCodecDecode, err)
	}
	stmt, err := parser.ParseOneStmt(createSQL, "", "")
	if err != nil {
		return nil, errors.WrapError(errors.ErrCodecDecode, err)
	}
	create, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return nil, errors.ErrCodecDecode.GenWithStack("expected CREATE TABLE for avro schema")
	}
	info, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), create)
	if err != nil {
		return nil, errors.WrapError(errors.ErrCodecDecode, err)
	}
	info.UpdateTS = s.version
	result := commonType.NewTableInfo4Decoder(schema, info)
	s.tables[key] = result
	return result, nil
}
