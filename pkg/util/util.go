// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/pingcap/errors"
)

// ParseHostAndPortFromAddress parse an address in format `host:port` like `127.0.0.1:2379`.
// Returns error if parse failed.
func ParseHostAndPortFromAddress(address string) (string, uint, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, errors.Errorf("Invalid address `%s`, expect format `host:port`", address)
	}
	portNumeric, err := strconv.Atoi(port)
	if err != nil || portNumeric == 0 {
		return "", 0, errors.Errorf("Invalid address `%s`, expect port to be numeric", address)
	}
	return host, uint(portNumeric), nil
}

// Hang will block the goroutine for a given duration, or return when `ctx` is done.
func Hang(ctx context.Context, dur time.Duration) error {
	timer := time.NewTimer(dur)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Must panics if err is not nil.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
