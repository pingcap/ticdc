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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func initializeTables(ctx context.Context, db *sql.DB) {
	log.Info("Initializing tables and data")

	// Drop and create tables
	tables := []string{
		// E-commerce tables
		`DROP TABLE IF EXISTS order_items`,
		`DROP TABLE IF EXISTS orders`,
		`DROP TABLE IF EXISTS inventory`,

		// Banking tables
		`DROP TABLE IF EXISTS accounts`,
		`DROP TABLE IF EXISTS transaction_history`,

		// Social network tables
		`DROP TABLE IF EXISTS comments`,
		`DROP TABLE IF EXISTS likes`,
		`DROP TABLE IF EXISTS post_tags`,
		`DROP TABLE IF EXISTS posts`,
		`DROP TABLE IF EXISTS follows`,
		`DROP TABLE IF EXISTS users`,

		// Shared sequence table for verification
		`DROP TABLE IF EXISTS txn_sequence`,

		// E-commerce: Inventory table
		`CREATE TABLE inventory (
			product_id BIGINT PRIMARY KEY,
			product_name VARCHAR(100),
			stock INT NOT NULL,
			total_sold BIGINT DEFAULT 0,
			version BIGINT DEFAULT 0,
			updated_at BIGINT
		)`,

		// E-commerce: Orders table
		`CREATE TABLE orders (
			order_id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			total_amount DECIMAL(10,2) NOT NULL,
			item_count INT NOT NULL,
			status TINYINT NOT NULL DEFAULT 0, -- 0:created, 1:cancelled
			created_at BIGINT,
			updated_at BIGINT,
			KEY idx_user (user_id),
			KEY idx_status (status)
		)`,

		// E-commerce: Order items table
		`CREATE TABLE order_items (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			order_id BIGINT NOT NULL,
			product_id BIGINT NOT NULL,
			quantity INT NOT NULL,
			price DECIMAL(10,2) NOT NULL,
			KEY idx_order (order_id),
			KEY idx_product (product_id),
			UNIQUE KEY uk_order_product (order_id, product_id)
		)`,

		// Banking: Accounts table
		`CREATE TABLE accounts (
			account_id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			balance DECIMAL(15,2) NOT NULL,
			total_in DECIMAL(15,2) DEFAULT 0,
			total_out DECIMAL(15,2) DEFAULT 0,
			version BIGINT DEFAULT 0,
			updated_at BIGINT,
			KEY idx_user (user_id)
		)`,

		// Banking: Transaction history
		`CREATE TABLE transaction_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			from_account BIGINT,
			to_account BIGINT,
			amount DECIMAL(15,2) NOT NULL,
			tx_type TINYINT NOT NULL, -- 1:transfer, 2:deposit, 3:withdraw
			created_at BIGINT,
			KEY idx_from (from_account),
			KEY idx_to (to_account)
		)`,

		// Social: Users table
		`CREATE TABLE users (
			user_id BIGINT PRIMARY KEY,
			username VARCHAR(50) NOT NULL,
			post_count INT DEFAULT 0,
			follower_count INT DEFAULT 0,
			following_count INT DEFAULT 0,
			like_count INT DEFAULT 0,
			version BIGINT DEFAULT 0,
			UNIQUE KEY uk_username (username)
		)`,

		// Social: Follows table
		`CREATE TABLE follows (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			follower_id BIGINT NOT NULL,
			following_id BIGINT NOT NULL,
			created_at BIGINT,
			UNIQUE KEY uk_follow (follower_id, following_id),
			KEY idx_follower (follower_id),
			KEY idx_following (following_id)
		)`,

		// Social: Posts table
		`CREATE TABLE posts (
			post_id BIGINT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			content TEXT,
			like_count INT DEFAULT 0,
			comment_count INT DEFAULT 0,
			status TINYINT DEFAULT 0, -- 0:active, 1:deleted
			created_at BIGINT,
			KEY idx_user (user_id)
		)`,

		// Social: Post tags table
		`CREATE TABLE post_tags (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			post_id BIGINT NOT NULL,
			tag VARCHAR(50) NOT NULL,
			KEY idx_post (post_id),
			KEY idx_tag (tag),
			UNIQUE KEY uk_post_tag (post_id, tag)
		)`,

		// Social: Likes table
		`CREATE TABLE likes (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			user_id BIGINT NOT NULL,
			post_id BIGINT NOT NULL,
			created_at BIGINT,
			UNIQUE KEY uk_user_post (user_id, post_id),
			KEY idx_post (post_id)
		)`,

		// Social: Comments table
		`CREATE TABLE comments (
			comment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
			post_id BIGINT NOT NULL,
			user_id BIGINT NOT NULL,
			content VARCHAR(500),
			created_at BIGINT,
			KEY idx_post (post_id),
			KEY idx_user (user_id)
		)`,

		// Sequence table for verification (to detect transaction loss)
		`CREATE TABLE txn_sequence (
			id BIGINT PRIMARY KEY,
			seq_no BIGINT NOT NULL,
			tx_type VARCHAR(50),
			created_at BIGINT,
			KEY idx_seq (seq_no)
		)`,
	}

	for _, ddl := range tables {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			log.Fatal("Failed to execute DDL", zap.String("ddl", ddl), zap.Error(err))
		}
	}

	// Initialize data
	log.Info("Initializing inventory data", zap.Int("products", *numProducts))
	initInventory(ctx, db)

	log.Info("Initializing accounts data", zap.Int("users", *numUsers))
	initAccounts(ctx, db)

	log.Info("Initializing users data", zap.Int("users", *numUsers))
	initUsers(ctx, db)

	log.Info("Tables and data initialized successfully")
}

func initInventory(ctx context.Context, db *sql.DB) {
	batchSize := 100
	for i := 0; i < *numProducts; i += batchSize {
		var values []interface{}
		var placeholders string

		for j := 0; j < batchSize && i+j < *numProducts; j++ {
			if j > 0 {
				placeholders += ","
			}
			placeholders += "(?, ?, ?, 0, 0, ?)"
			productID := int64(i + j + 1)
			values = append(values,
				productID,
				fmt.Sprintf("Product_%d", productID),
				initialStock,
				time.Now().Unix())
		}

		query := "INSERT INTO inventory (product_id, product_name, stock, total_sold, version, updated_at) VALUES " + placeholders
		if _, err := db.ExecContext(ctx, query, values...); err != nil {
			log.Fatal("Failed to initialize inventory", zap.Error(err))
		}
	}
}

func initAccounts(ctx context.Context, db *sql.DB) {
	batchSize := 100
	for i := 0; i < *numUsers; i += batchSize {
		var values []interface{}
		var placeholders string

		for j := 0; j < batchSize && i+j < *numUsers; j++ {
			if j > 0 {
				placeholders += ","
			}
			placeholders += "(?, ?, ?, 0, 0, 0, ?)"
			userID := int64(i + j + 1)
			values = append(values,
				userID, // account_id
				userID, // user_id
				float64(initialBalance),
				time.Now().Unix())
		}

		query := "INSERT INTO accounts (account_id, user_id, balance, total_in, total_out, version, updated_at) VALUES " + placeholders
		if _, err := db.ExecContext(ctx, query, values...); err != nil {
			log.Fatal("Failed to initialize accounts", zap.Error(err))
		}
	}
}

func initUsers(ctx context.Context, db *sql.DB) {
	batchSize := 100
	for i := 0; i < *numUsers; i += batchSize {
		var values []interface{}
		var placeholders string

		for j := 0; j < batchSize && i+j < *numUsers; j++ {
			if j > 0 {
				placeholders += ","
			}
			placeholders += "(?, ?, 0, 0, 0, 0, 0)"
			userID := int64(i + j + 1)
			values = append(values,
				userID,
				fmt.Sprintf("user_%d", userID))
		}

		query := "INSERT INTO users (user_id, username, post_count, follower_count, following_count, like_count, version) VALUES " + placeholders
		if _, err := db.ExecContext(ctx, query, values...); err != nil {
			log.Fatal("Failed to initialize users", zap.Error(err))
		}
	}
}
