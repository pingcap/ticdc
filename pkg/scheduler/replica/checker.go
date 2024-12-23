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

package replica

type OpType int

const (
	OpSplit         OpType = iota // Split one span to multiple subspans
	OpMerge                       // merge multiple spans to one span
	OpMergeAndSplit               // remove old spans and split to multiple subspans
)

type CheckResult[T ReplicationID, R Replication[T]] struct {
	OpType       OpType
	Replications []R
}

type Checker[T ReplicationID, R Replication[T], S any] interface {
	UpdateStatus(replication R, status S)
	Check() []CheckResult[T, R]
}

type GroupCheckResult any
type ReplicationStatus any

type GroupChecker[T ReplicationID, R Replication[T]] interface {
	AddReplica(replication R)
	RemoveReplica(replication R)
	UpdateStatus(replication R)
	Check(batch int) GroupCheckResult
	Name() string
	Stat() string
}

func NewEmptyChecker[T ReplicationID, R Replication[T]](GroupID) GroupChecker[T, R] {
	return &EmptyStatusChecker[T, R]{}
}

// implement a empty status checker
type EmptyStatusChecker[T ReplicationID, R Replication[T]] struct{}

func (c *EmptyStatusChecker[T, R]) AddReplica(_ R) {}

func (c *EmptyStatusChecker[T, R]) RemoveReplica(_ R) {}

func (c *EmptyStatusChecker[T, R]) UpdateStatus(_ R) {}

func (c *EmptyStatusChecker[T, R]) Check(_ int) GroupCheckResult {
	return nil
}

func (c *EmptyStatusChecker[T, R]) Name() string {
	return "empty checker"
}

func (c *EmptyStatusChecker[T, R]) Stat() string {
	return ""
}
