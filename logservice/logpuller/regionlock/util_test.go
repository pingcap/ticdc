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

package regionlock

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestCheckRegionsLeftCover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		regions []*metapb.Region
		span    heartbeatpb.TableSpan
		cover   bool
	}{
		{
			regions: []*metapb.Region{},
			span:    *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0), cover: false,
		},
		{regions: []*metapb.Region{
			{StartKey: nil, EndKey: nil},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0), cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0), cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{0}, EndKey: []byte{4}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0), cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{3}, 0), cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{3}, EndKey: []byte{4}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{4}, 0), cover: false},
		{regions: []*metapb.Region{
			{StartKey: []byte{1}, EndKey: []byte{2}},
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{4}, 0), cover: true},
		{regions: []*metapb.Region{
			{StartKey: []byte{2}, EndKey: []byte{3}},
		}, span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{3}, 0), cover: false},
	}

	for _, tc := range cases {
		require.Equal(t, tc.cover, CheckRegionsLeftCover(tc.regions, tc.span))
	}
}

func TestCutRegionsLeftCoverSpan(t *testing.T) {
	t.Parallel()

	cases := []struct {
		regions []*metapb.Region
		span    heartbeatpb.TableSpan
		covered []*metapb.Region
	}{
		{
			regions: []*metapb.Region{},
			span:    *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			covered: nil,
		},
		{
			regions: []*metapb.Region{{StartKey: nil, EndKey: nil}},
			span:    *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			covered: []*metapb.Region{{StartKey: nil, EndKey: nil}},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
			span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{0}, EndKey: []byte{4}},
			},
			span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			covered: []*metapb.Region{
				{StartKey: []byte{0}, EndKey: []byte{4}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{3}, 0),
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{3}, EndKey: []byte{4}},
			},
			span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{4}, 0),
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{4}, 0),
			covered: []*metapb.Region{
				{StartKey: []byte{1}, EndKey: []byte{2}},
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
		},
		{
			regions: []*metapb.Region{
				{StartKey: []byte{2}, EndKey: []byte{3}},
			},
			span:    *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{3}, 0),
			covered: nil,
		},
	}

	for _, tc := range cases {
		require.Equal(t, tc.covered, CutRegionsLeftCoverSpan(tc.regions, tc.span))
	}
}
