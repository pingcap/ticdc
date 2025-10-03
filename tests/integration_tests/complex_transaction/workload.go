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
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type WorkloadExecutor struct {
	db       *sql.DB
	workerID int
	seqNo    int64
}

func NewWorkloadExecutor(db *sql.DB, workerID int) *WorkloadExecutor {
	return &WorkloadExecutor{
		db:       db,
		workerID: workerID,
		seqNo:    0,
	}
}

func (w *WorkloadExecutor) ExecuteTransaction(ctx context.Context, txType string) error {
	maxRetries := 3
	var err error

	for retry := 0; retry < maxRetries; retry++ {
		switch txType {
		case "create_order":
			err = w.createOrderTxn(ctx)
		case "cancel_order":
			err = w.cancelOrderTxn(ctx)
		case "bank_transfer":
			err = w.bankTransferTxn(ctx)
		case "bank_multi_transfer":
			err = w.bankMultiTransferTxn(ctx)
		case "social_create_post":
			err = w.socialCreatePostTxn(ctx)
		case "social_interact":
			err = w.socialInteractTxn(ctx)
		case "inventory_adjust":
			err = w.inventoryAdjustTxn(ctx)
		case "user_activity":
			err = w.userActivityTxn(ctx)
		case "complex_mixed":
			err = w.complexMixedTxn(ctx)
		default:
			return fmt.Errorf("unknown transaction type: %s", txType)
		}

		if err == nil {
			return nil
		}

		// Retry on deadlock or lock wait timeout
		if isRetryableError(err) && retry < maxRetries-1 {
			time.Sleep(time.Millisecond * time.Duration(10*(retry+1)))
			continue
		}

		return errors.Trace(err)
	}

	return err
}

// E-commerce: Create order transaction
// Includes: INSERT order, INSERT order_items (multiple rows), UPDATE inventory, UPDATE accounts
func (w *WorkloadExecutor) createOrderTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	userID := randomUserID()
	orderID := generateID()

	// Random 1-10 items, but 10% chance for large order (20-50 items)
	itemCount := rand.Intn(10) + 1
	if rand.Intn(100) < 10 {
		itemCount = rand.Intn(31) + 20
	}

	var totalAmount float64
	var items []struct {
		productID int64
		quantity  int
		price     float64
	}

	// Select products and calculate total
	for i := 0; i < itemCount; i++ {
		productID := randomProductID()
		quantity := rand.Intn(5) + 1
		price := float64(rand.Intn(100)+1) * 10.0

		items = append(items, struct {
			productID int64
			quantity  int
			price     float64
		}{productID, quantity, price})

		totalAmount += price * float64(quantity)
	}

	// Insert order
	_, err = tx.ExecContext(ctx,
		`INSERT INTO orders (order_id, user_id, total_amount, item_count, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, 0, ?, ?)`,
		orderID, userID, totalAmount, itemCount, now, now)
	if err != nil {
		return errors.Trace(err)
	}

	// Insert order items and update inventory
	for _, item := range items {
		// Insert order item
		_, err = tx.ExecContext(ctx,
			`INSERT INTO order_items (order_id, product_id, quantity, price)
			 VALUES (?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity)`,
			orderID, item.productID, item.quantity, item.price)
		if err != nil {
			return errors.Trace(err)
		}

		// Update inventory
		result, err := tx.ExecContext(ctx,
			`UPDATE inventory
			 SET stock = stock - ?, total_sold = total_sold + ?, version = version + 1, updated_at = ?
			 WHERE product_id = ? AND stock >= ?`,
			item.quantity, item.quantity, now, item.productID, item.quantity)
		if err != nil {
			return errors.Trace(err)
		}

		affected, _ := result.RowsAffected()
		if affected == 0 {
			// Insufficient stock, rollback
			return nil
		}
	}

	// Update account balance (pay for order)
	_, err = tx.ExecContext(ctx,
		`UPDATE accounts
		 SET balance = balance - ?, total_out = total_out + ?, version = version + 1, updated_at = ?
		 WHERE account_id = ? AND balance >= ?`,
		totalAmount, totalAmount, now, userID, totalAmount)
	if err != nil {
		return errors.Trace(err)
	}

	// Record sequence
	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "create_order", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// E-commerce: Cancel order transaction
// Includes: UPDATE order, DELETE order_items, UPDATE inventory (restore stock)
func (w *WorkloadExecutor) cancelOrderTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	// Find a random active order
	var orderID int64
	var userID int64
	var totalAmount float64

	err = tx.QueryRowContext(ctx,
		`SELECT order_id, user_id, total_amount FROM orders
		 WHERE status = 0 ORDER BY RAND() LIMIT 1`).Scan(&orderID, &userID, &totalAmount)
	if err == sql.ErrNoRows {
		// No active orders to cancel
		return tx.Commit()
	}
	if err != nil {
		return errors.Trace(err)
	}

	// Get order items
	rows, err := tx.QueryContext(ctx,
		`SELECT product_id, quantity FROM order_items WHERE order_id = ?`, orderID)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	var items []struct {
		productID int64
		quantity  int
	}

	for rows.Next() {
		var item struct {
			productID int64
			quantity  int
		}
		if err := rows.Scan(&item.productID, &item.quantity); err != nil {
			return errors.Trace(err)
		}
		items = append(items, item)
	}
	rows.Close()

	// Update order status to cancelled
	_, err = tx.ExecContext(ctx,
		`UPDATE orders SET status = 1, updated_at = ? WHERE order_id = ?`,
		now, orderID)
	if err != nil {
		return errors.Trace(err)
	}

	// Delete order items and restore inventory
	for _, item := range items {
		_, err = tx.ExecContext(ctx,
			`DELETE FROM order_items WHERE order_id = ? AND product_id = ?`,
			orderID, item.productID)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE inventory
			 SET stock = stock + ?, total_sold = total_sold - ?, version = version + 1, updated_at = ?
			 WHERE product_id = ?`,
			item.quantity, item.quantity, now, item.productID)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Refund to account
	_, err = tx.ExecContext(ctx,
		`UPDATE accounts
		 SET balance = balance + ?, total_in = total_in + ?, version = version + 1, updated_at = ?
		 WHERE account_id = ?`,
		totalAmount, totalAmount, now, userID)
	if err != nil {
		return errors.Trace(err)
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "cancel_order", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Banking: Simple transfer transaction
// Includes: UPDATE 2 accounts, INSERT transaction_history
func (w *WorkloadExecutor) bankTransferTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	fromAccount := randomUserID()
	toAccount := randomUserID()
	if fromAccount == toAccount {
		toAccount = (toAccount % int64(*numUsers)) + 1
	}

	amount := float64(rand.Intn(100)+1) * 10.0

	// Deduct from sender
	result, err := tx.ExecContext(ctx,
		`UPDATE accounts
		 SET balance = balance - ?, total_out = total_out + ?, version = version + 1, updated_at = ?
		 WHERE account_id = ? AND balance >= ?`,
		amount, amount, now, fromAccount, amount)
	if err != nil {
		return errors.Trace(err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		// Insufficient balance
		return tx.Commit()
	}

	// Add to receiver
	_, err = tx.ExecContext(ctx,
		`UPDATE accounts
		 SET balance = balance + ?, total_in = total_in + ?, version = version + 1, updated_at = ?
		 WHERE account_id = ?`,
		amount, amount, now, toAccount)
	if err != nil {
		return errors.Trace(err)
	}

	// Record transaction history
	_, err = tx.ExecContext(ctx,
		`INSERT INTO transaction_history (from_account, to_account, amount, tx_type, created_at)
		 VALUES (?, ?, ?, 1, ?)`,
		fromAccount, toAccount, amount, now)
	if err != nil {
		return errors.Trace(err)
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "bank_transfer", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Banking: Multi-account transfer (complex)
// Transfer from A to B, C to D, etc. in one transaction
func (w *WorkloadExecutor) bankMultiTransferTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	transferCount := rand.Intn(5) + 2 // 2-6 transfers

	for i := 0; i < transferCount; i++ {
		fromAccount := randomUserID()
		toAccount := randomUserID()
		if fromAccount == toAccount {
			toAccount = (toAccount % int64(*numUsers)) + 1
		}

		amount := float64(rand.Intn(50)+1) * 10.0

		result, err := tx.ExecContext(ctx,
			`UPDATE accounts
			 SET balance = balance - ?, total_out = total_out + ?, version = version + 1, updated_at = ?
			 WHERE account_id = ? AND balance >= ?`,
			amount, amount, now, fromAccount, amount)
		if err != nil {
			return errors.Trace(err)
		}

		affected, _ := result.RowsAffected()
		if affected == 0 {
			continue
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE accounts
			 SET balance = balance + ?, total_in = total_in + ?, version = version + 1, updated_at = ?
			 WHERE account_id = ?`,
			amount, amount, now, toAccount)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO transaction_history (from_account, to_account, amount, tx_type, created_at)
			 VALUES (?, ?, ?, 1, ?)`,
			fromAccount, toAccount, amount, now)
		if err != nil {
			return errors.Trace(err)
		}
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "bank_multi_transfer", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Social: Create post transaction
// Includes: INSERT post, INSERT post_tags (multiple rows), UPDATE user stats
func (w *WorkloadExecutor) socialCreatePostTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	userID := randomUserID()
	postID := generateID()
	content := fmt.Sprintf("Post content %d from user %d", postID, userID)

	// Insert post
	_, err = tx.ExecContext(ctx,
		`INSERT INTO posts (post_id, user_id, content, like_count, comment_count, status, created_at)
		 VALUES (?, ?, ?, 0, 0, 0, ?)`,
		postID, userID, content, now)
	if err != nil {
		return errors.Trace(err)
	}

	// Add tags (1-5 tags)
	tagCount := rand.Intn(5) + 1
	tags := []string{"tech", "life", "food", "travel", "sports", "music", "art", "news"}

	for i := 0; i < tagCount; i++ {
		tag := tags[rand.Intn(len(tags))]
		_, err = tx.ExecContext(ctx,
			`INSERT IGNORE INTO post_tags (post_id, tag) VALUES (?, ?)`,
			postID, tag)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Update user post count
	_, err = tx.ExecContext(ctx,
		`UPDATE users SET post_count = post_count + 1, version = version + 1 WHERE user_id = ?`,
		userID)
	if err != nil {
		return errors.Trace(err)
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "social_create_post", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Social: User interactions (like, comment, follow)
func (w *WorkloadExecutor) socialInteractTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	userID := randomUserID()
	actionType := rand.Intn(3) // 0:like, 1:comment, 2:follow

	switch actionType {
	case 0: // Like a post
		var postID int64
		var postUserID int64
		err = tx.QueryRowContext(ctx,
			`SELECT post_id, user_id FROM posts WHERE status = 0 ORDER BY RAND() LIMIT 1`).
			Scan(&postID, &postUserID)
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT IGNORE INTO likes (user_id, post_id, created_at) VALUES (?, ?, ?)`,
			userID, postID, now)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE posts SET like_count = like_count + 1 WHERE post_id = ?`,
			postID)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE users SET like_count = like_count + 1 WHERE user_id = ?`,
			postUserID)
		if err != nil {
			return errors.Trace(err)
		}

	case 1: // Comment on a post
		var postID int64
		err = tx.QueryRowContext(ctx,
			`SELECT post_id FROM posts WHERE status = 0 ORDER BY RAND() LIMIT 1`).
			Scan(&postID)
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		if err != nil {
			return errors.Trace(err)
		}

		comment := fmt.Sprintf("Comment from user %d", userID)
		_, err = tx.ExecContext(ctx,
			`INSERT INTO comments (post_id, user_id, content, created_at) VALUES (?, ?, ?, ?)`,
			postID, userID, comment, now)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE posts SET comment_count = comment_count + 1 WHERE post_id = ?`,
			postID)
		if err != nil {
			return errors.Trace(err)
		}

	case 2: // Follow a user
		followingID := randomUserID()
		if followingID == userID {
			followingID = (followingID % int64(*numUsers)) + 1
		}

		_, err = tx.ExecContext(ctx,
			`INSERT IGNORE INTO follows (follower_id, following_id, created_at) VALUES (?, ?, ?)`,
			userID, followingID, now)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE users SET following_count = following_count + 1 WHERE user_id = ?`,
			userID)
		if err != nil {
			return errors.Trace(err)
		}

		_, err = tx.ExecContext(ctx,
			`UPDATE users SET follower_count = follower_count + 1 WHERE user_id = ?`,
			followingID)
		if err != nil {
			return errors.Trace(err)
		}
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "social_interact", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Inventory adjustment (restock)
func (w *WorkloadExecutor) inventoryAdjustTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	// Adjust 3-10 products
	adjustCount := rand.Intn(8) + 3

	for i := 0; i < adjustCount; i++ {
		productID := randomProductID()
		adjustment := rand.Intn(200) + 50 // Add 50-250 stock

		_, err = tx.ExecContext(ctx,
			`UPDATE inventory
			 SET stock = stock + ?, version = version + 1, updated_at = ?
			 WHERE product_id = ?`,
			adjustment, now, productID)
		if err != nil {
			return errors.Trace(err)
		}
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "inventory_adjust", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// User activity update (mixed operations)
func (w *WorkloadExecutor) userActivityTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	userID := randomUserID()

	// REPLACE INTO to test this feature
	_, err = tx.ExecContext(ctx,
		`REPLACE INTO users (user_id, username, post_count, follower_count, following_count, like_count, version)
		 VALUES (?, ?,
		         (SELECT COALESCE(post_count, 0) FROM (SELECT post_count FROM users WHERE user_id = ?) t1),
		         (SELECT COALESCE(follower_count, 0) FROM (SELECT follower_count FROM users WHERE user_id = ?) t2) + 1,
		         (SELECT COALESCE(following_count, 0) FROM (SELECT following_count FROM users WHERE user_id = ?) t3),
		         (SELECT COALESCE(like_count, 0) FROM (SELECT like_count FROM users WHERE user_id = ?) t4),
		         (SELECT COALESCE(version, 0) FROM (SELECT version FROM users WHERE user_id = ?) t5) + 1)`,
		userID, fmt.Sprintf("user_%d", userID), userID, userID, userID, userID, userID)
	if err != nil {
		return errors.Trace(err)
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "user_activity", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Complex mixed transaction (large transaction with many operations)
// Can have up to 500 operations
func (w *WorkloadExecutor) complexMixedTxn(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()
	opCount := rand.Intn(451) + 50 // 50-500 operations

	var failedOps int

	for i := 0; i < opCount; i++ {
		opType := rand.Intn(4)

		switch opType {
		case 0: // Update inventory
			productID := randomProductID()
			delta := rand.Intn(20) - 10 // -10 to +10
			_, err = tx.ExecContext(ctx,
				`UPDATE inventory SET stock = stock + ?, version = version + 1, updated_at = ? WHERE product_id = ?`,
				delta, now, productID)

		case 1: // Update account
			accountID := randomUserID()
			delta := float64(rand.Intn(200)-100) * 10.0
			if delta > 0 {
				_, err = tx.ExecContext(ctx,
					`UPDATE accounts SET balance = balance + ?, total_in = total_in + ?, version = version + 1 WHERE account_id = ?`,
					delta, delta, accountID)
			}

		case 2: // Insert transaction history
			fromAcc := randomUserID()
			toAcc := randomUserID()
			amount := float64(rand.Intn(100) + 1)
			_, err = tx.ExecContext(ctx,
				`INSERT INTO transaction_history (from_account, to_account, amount, tx_type, created_at) VALUES (?, ?, ?, 1, ?)`,
				fromAcc, toAcc, amount, now)

		case 3: // Update user stats
			userID := randomUserID()
			_, err = tx.ExecContext(ctx,
				`UPDATE users SET post_count = post_count + 1, version = version + 1 WHERE user_id = ?`,
				userID)
		}

		if err != nil {
			failedOps++
			// Don't log each failed operation, just count them
			// If it's a deadlock, the whole transaction will be retried
		}
	}

	// Only log if many operations failed
	if failedOps > opCount/10 {
		log.Debug("Some operations failed in complex transaction",
			zap.Int("failed", failedOps),
			zap.Int("total", opCount))
	}

	w.seqNo++
	_, err = tx.ExecContext(ctx,
		`INSERT INTO txn_sequence (id, seq_no, tx_type, created_at) VALUES (?, ?, ?, ?)`,
		generateID(), w.seqNo, "complex_mixed", now)
	if err != nil {
		return errors.Trace(err)
	}

	return tx.Commit()
}

// Helper functions
func randomUserID() int64 {
	return int64(rand.Intn(*numUsers) + 1)
}

func randomProductID() int64 {
	return int64(rand.Intn(*numProducts) + 1)
}

var idCounter int64

func generateID() int64 {
	return time.Now().UnixNano() + atomic.AddInt64(&idCounter, 1)
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return contains(errMsg, "Deadlock") ||
		contains(errMsg, "Lock wait timeout") ||
		contains(errMsg, "try again later")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
