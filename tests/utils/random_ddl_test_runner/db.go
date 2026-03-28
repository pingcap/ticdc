package main

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func openMySQL(ctx context.Context, cfg mysqlConnConfig) (*sql.DB, error) {
	db, err := sql.Open("mysql", cfg.dsn(""))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(128)
	db.SetMaxIdleConns(128)
	db.SetConnMaxLifetime(5 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
