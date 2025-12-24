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

package node

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/version"
)

type ID string

func (s ID) String() string {
	return string(s)
}

func (s ID) GetSize() int64 {
	return int64(len(s))
}

func (s ID) IsEmpty() bool {
	return s == ""
}

func NewID() ID {
	return ID(uuid.New().String())
}

// Info store in etcd.
type Info struct {
	ID            ID     `json:"id"`
	AdvertiseAddr string `json:"address"`

	Version        string `json:"version"`
	GitHash        string `json:"git-hash"`
	DeployPath     string `json:"deploy-path"`
	StartTimestamp int64  `json:"start-timestamp"`

	// Epoch represents how many times the node has been restarted.
	Epoch uint64 `json:"epoch"`
}

func NewInfo(addr string, deployPath string) *Info {
	return &Info{
		ID:             NewID(),
		AdvertiseAddr:  addr,
		Version:        version.ReleaseVersion,
		GitHash:        version.GitHash,
		DeployPath:     deployPath,
		StartTimestamp: time.Now().Unix(),
	}
}

func (c *Info) String() string {
	return fmt.Sprintf("ID: %s, AdvertiseAddr: %s, Version: %s, GitHash: %s, DeployPath: %s, StartTimestamp: %d, Epoch: %d",
		c.ID, c.AdvertiseAddr, c.Version, c.GitHash, c.DeployPath, c.StartTimestamp, c.Epoch)
}

// Marshal using json.Marshal.
func (c *Info) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *Info) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(errors.WrapError(errors.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

func CaptureInfoToNodeInfo(captureInfo *config.CaptureInfo) *Info {
	return &Info{
		ID:             ID(captureInfo.ID),
		AdvertiseAddr:  captureInfo.AdvertiseAddr,
		Version:        captureInfo.Version,
		GitHash:        captureInfo.GitHash,
		DeployPath:     captureInfo.DeployPath,
		StartTimestamp: captureInfo.StartTimestamp,
	}
}

type state int

const (
	// stateUninitialized means the node Status is unknown,
	// no bootstrap response of this node received yet.
	stateUninitialized state = iota
	// stateInitialized means bootstrapper has received the bootstrap response of this node.
	stateInitialized
)

// Status represents the bootstrap state and metadata of a node in the system.
// It tracks initialization Status, node information, cached bootstrap response,
// and timing data for bootstrap message retries.
type Status[T any] struct {
	state state
	node  *Info

	// response is the bootstrap response of this node.
	response *T

	// lastBootstrapTime is the time when the bootstrap message is created for this node.
	// It approximates the time when we send the bootstrap message to the node.
	// It is used to limit the frequency of sending bootstrap message.
	lastBootstrapTime time.Time
}

func NewStatus[T any](node *Info) *Status[T] {
	return &Status[T]{
		state: stateUninitialized,
		node:  node,
	}
}

func (t *Status[T]) GetNodeInfo() *Info {
	return t.node
}

func (t *Status[T]) SetLastBootstrapTime(currentTime time.Time) {
	t.lastBootstrapTime = currentTime
}

func (t *Status[T]) GetLastBootstrapTime() time.Time {
	return t.lastBootstrapTime
}

func (t *Status[T]) SetResponse(msg *T) {
	t.response = msg
	t.state = stateInitialized
}

func (t *Status[T]) Initialized() bool {
	return t.state == stateInitialized
}

func (t *Status[T]) GetResponse() *T {
	response := t.response
	t.response = nil
	return response
}
