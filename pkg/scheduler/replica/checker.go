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

import "fmt"

type (
	GroupID           = int64
	GroupType         int8
	GroupCheckResult  any
	ReplicationStatus any
)

const DefaultGroupID GroupID = 0

const (
	GroupDefault GroupType = iota
	GroupTable
	// add more group strategy later
	// groupHotLevel1
)

// Notice: all methods are NOT thread-safe.
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

func GetGroupName(id GroupID) string {
	gt := GroupType(id >> 56)
	if gt == GroupTable {
		return fmt.Sprintf("%s-%d", gt.String(), id&0x00FFFFFFFFFFFFFF)
	}
	return gt.String()
}

func (gt GroupType) Less(other GroupType) bool {
	return gt < other
}

func (gt GroupType) String() string {
	switch gt {
	case GroupDefault:
		return "default"
	case GroupTable:
		return "table"
	default:
		// return "HotLevel" + strconv.Itoa(int(gt-groupHotLevel1))
		panic("unreachable")
	}
}

func GenGroupID(gt GroupType, tableID int64) GroupID {
	// use high 8 bits to store the group type
	id := int64(gt) << 56
	if gt == GroupTable {
		return id | tableID
	}
	return id
}

func GetGroupType(id GroupID) GroupType {
	return GroupType(id >> 56)
}
