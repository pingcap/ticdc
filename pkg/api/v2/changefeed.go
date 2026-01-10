// Copyright 2022 PingCAP, Inc.
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

package v2

import (
	"context"
	"fmt"
	"strconv"

	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/api/internal/rest"
	"github.com/pingcap/ticdc/pkg/common"
)

// ChangefeedsGetter has a method to return a ChangefeedInterface.
type ChangefeedsGetter interface {
	Changefeeds() ChangefeedInterface
}

// ChangefeedInterface has methods to work with Changefeed items.
// We can also mock the changefeed operations by implement this interface.
type ChangefeedInterface interface {
	// Create creates a changefeed
	Create(ctx context.Context, cfg *v2.ChangefeedConfig, keyspace string) (*v2.ChangeFeedInfo, error)
	// VerifyTable verifies table for a changefeed
	VerifyTable(ctx context.Context, cfg *v2.VerifyTableConfig, keyspace string) (*v2.Tables, error)
	// Update updates a changefeed
	Update(ctx context.Context, cfg *v2.ChangefeedConfig,
		keyspace string, name string) (*v2.ChangeFeedInfo, error)
	// Resume resumes a changefeed with given config
	Resume(ctx context.Context, cfg *v2.ResumeChangefeedConfig, keyspace string, name string) error
	// Delete deletes a changefeed by name
	Delete(ctx context.Context, keyspace string, name string) error
	// Pause pauses a changefeed with given name
	Pause(ctx context.Context, keyspace string, name string) error
	// Get gets a changefeed detaail info
	Get(ctx context.Context, keyspace string, name string) (*v2.ChangeFeedInfo, error)
	// List lists all changefeeds
	List(ctx context.Context, keyspace string, state string) ([]v2.ChangefeedCommonInfo, error)
	// Move Table to target node, it just for make test case now. **Not for public use.**
	MoveTable(ctx context.Context, keyspace string, name string, tableID int64, targetNode string, mode int64, wait bool) error
	// Move dispatchers in a split Table to target node, it just for make test case now. **Not for public use.**
	MoveSplitTable(ctx context.Context, keyspace string, name string, tableID int64, targetNode string, mode int64) error
	// split table based on region count, it just for make test case now. **Not for public use.**
	SplitTableByRegionCount(ctx context.Context, keyspace string, name string, tableID int64, mode int64) error
	// merge table, it just for make test case now. **Not for public use.**
	MergeTable(ctx context.Context, keyspace string, name string, tableID int64, mode int64) error
}

// changefeeds implements ChangefeedInterface
type changefeeds struct {
	client rest.CDCRESTInterface
}

// newChangefeed returns changefeeds
func newChangefeeds(c *APIV2Client) *changefeeds {
	return &changefeeds{
		client: c.RESTClient(),
	}
}

func (c *changefeeds) Create(ctx context.Context,
	cfg *v2.ChangefeedConfig,
	keyspace string,
) (*v2.ChangeFeedInfo, error) {
	result := &v2.ChangeFeedInfo{}
	u := fmt.Sprintf("changefeeds?%s=%s", api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).Into(result)
	return result, err
}

func (c *changefeeds) VerifyTable(ctx context.Context,
	cfg *v2.VerifyTableConfig,
	keyspace string,
) (*v2.Tables, error) {
	result := &v2.Tables{}
	u := fmt.Sprintf("verify_table?%s=%s", api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *changefeeds) Update(ctx context.Context,
	cfg *v2.ChangefeedConfig, keyspace string, name string,
) (*v2.ChangeFeedInfo, error) {
	result := &v2.ChangeFeedInfo{}
	u := fmt.Sprintf("changefeeds/%s?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err := c.client.Put().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}

// Resume a changefeed
func (c *changefeeds) Resume(ctx context.Context,
	cfg *v2.ResumeChangefeedConfig, keyspace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s/resume?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	return c.client.Post().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).Error()
}

// Delete a changefeed
func (c *changefeeds) Delete(ctx context.Context,
	keyspace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	return c.client.Delete().
		WithURI(u).
		Do(ctx).Error()
}

// Pause a changefeed
func (c *changefeeds) Pause(ctx context.Context,
	keyspace string, name string,
) error {
	u := fmt.Sprintf("changefeeds/%s/pause?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	return c.client.Post().
		WithURI(u).
		Do(ctx).Error()
}

// Get gets a changefeed detaail info
func (c *changefeeds) Get(ctx context.Context,
	keyspace string, name string,
) (*v2.ChangeFeedInfo, error) {
	err := common.ValidateChangefeedID(name)
	if err != nil {
		return nil, err
	}
	result := new(v2.ChangeFeedInfo)
	u := fmt.Sprintf("changefeeds/%s?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err = c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}

// List lists all changefeeds
func (c *changefeeds) List(ctx context.Context,
	keyspace string, state string,
) ([]v2.ChangefeedCommonInfo, error) {
	result := &v2.ListResponse[v2.ChangefeedCommonInfo]{}
	u := fmt.Sprintf("changefeeds?%s=%s", api.APIOpVarKeyspace, keyspace)
	err := c.client.Get().
		WithURI(u).
		WithParam("state", state).
		Do(ctx).
		Into(result)
	return result.Items, err
}

// MoveTable to target node, it just for make test case now. **Not for public use.**
func (c *changefeeds) MoveTable(ctx context.Context,
	keyspace string, name string, tableID int64, targetNode string, mode int64, wait bool,
) error {
	url := fmt.Sprintf("changefeeds/%s/move_table?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("targetNodeID", targetNode).
		WithParam("mode", strconv.FormatInt(mode, 10)).
		WithParam("wait", strconv.FormatBool(wait)).
		Do(ctx).Error()
	return err
}

// move dispatchers in a split table to target node, it just for make test case now. **Not for public use.**
func (c *changefeeds) MoveSplitTable(ctx context.Context,
	keyspace string, name string, tableID int64, targetNode string, mode int64,
) error {
	url := fmt.Sprintf("changefeeds/%s/move_split_table?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("targetNodeID", targetNode).
		WithParam("mode", strconv.FormatInt(mode, 10)).
		Do(ctx).Error()
	return err
}

// SplitTableByRegionCount split table based on region count, it just for make test case now. **Not for public use.**
func (c *changefeeds) SplitTableByRegionCount(ctx context.Context,
	keyspace string, name string, tableID int64, mode int64,
) error {
	url := fmt.Sprintf("changefeeds/%s/split_table_by_region_count?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("mode", strconv.FormatInt(mode, 10)).
		Do(ctx).Error()
	return err
}

// MergeTable merge table, it just for make test case now. **Not for public use.**
func (c *changefeeds) MergeTable(ctx context.Context,
	keyspace string, name string, tableID int64, mode int64,
) error {
	url := fmt.Sprintf("changefeeds/%s/merge_table?%s=%s", name, api.APIOpVarKeyspace, keyspace)
	err := c.client.Post().
		WithURI(url).
		WithParam("tableID", strconv.FormatInt(tableID, 10)).
		WithParam("mode", strconv.FormatInt(mode, 10)).
		Do(ctx).Error()
	return err
}
