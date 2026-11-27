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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"bytes"
	"context"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

// spanSplitPlan retains only prospective boundaries, rather than all Region
// metadata. Discovery must finish before any child can publish progress.
type spanSplitPlan struct {
	span        heartbeatpb.TableSpan
	config      config.PullerSplitConfig
	regionCount int
	startKey    []byte
	spans       []heartbeatpb.TableSpan
}

func (p *spanSplitPlan) addRegion(region *metapb.Region) {
	p.regionCount++
	if p.regionCount%p.config.RegionCountPerSpan != 0 {
		return
	}
	endKey := region.EndKey
	if common.EndCompare(endKey, p.span.EndKey) > 0 {
		endKey = p.span.EndKey
	}
	p.appendSpan(endKey)
}

func (p *spanSplitPlan) appendSpan(endKey []byte) {
	span := p.span
	span.StartKey = bytes.Clone(p.startKey)
	span.EndKey = bytes.Clone(endKey)
	p.spans = append(p.spans, span)
	p.startKey = span.EndKey
}

func (p *spanSplitPlan) finish() []heartbeatpb.TableSpan {
	if p.regionCount <= p.config.RegionThreshold {
		return []heartbeatpb.TableSpan{p.span}
	}
	if !bytes.Equal(p.startKey, p.span.EndKey) {
		p.appendSpan(p.span.EndKey)
	}
	return p.spans
}

// pendingSubscription owns only the asynchronous creation of a subscription.
// Once the decision is installed, a caller already holding this object can
// still cancel the selected implementation safely.
type pendingSubscription struct {
	mu     sync.Mutex
	span   *subscribedSpan
	active bool
	split  *splitSubscription
}

func (p *pendingSubscription) run(ctx context.Context, client *subscriptionClient) error {
	defer client.pendingSubscriptions.CompareAndDelete(p.span.subID, p)
	plan := spanSplitPlan{
		span:     p.span.span,
		config:   client.splitConfig,
		startKey: p.span.span.StartKey,
	}
	if err := client.forEachRegion(ctx, p.span.span, p.span, plan.addRegion); err != nil {
		return err
	}
	spans := p.activate(client, plan.finish())
	if len(spans) > 1 {
		log.Info("split upstream subscription into internal spans",
			zap.Uint64("subscriptionID", uint64(p.span.subID)),
			zap.Int64("tableID", p.span.span.TableID),
			zap.Int("regionCount", plan.regionCount),
			zap.Int("spanCount", len(spans)))
	}
	for _, span := range spans {
		if err := client.divideSpanAndScheduleRegionRequests(ctx, rangeTask{
			span:           span.span,
			subscribedSpan: span,
			filterLoop:     span.filterLoop,
			priority:       span.baseScanPriority,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (p *pendingSubscription) activate(client *subscriptionClient, spans []heartbeatpb.TableSpan) []*subscribedSpan {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.span.stopped.Load() {
		return nil
	}
	var subscriptions []*subscribedSpan
	if len(spans) == 1 {
		client.spanRegistry.Add(p.span)
		client.eventSink.AddPath(p.span)
		subscriptions = []*subscribedSpan{p.span}
	} else {
		p.split = newSplitSubscription(client, p.span)
		subscriptions = p.split.prepare(spans)
		client.splitSubscriptions.Store(p.span.subID, p.split)
	}
	p.active = true
	// Publish the selected implementation before removing the pending entry.
	// Unsubscribe can consequently find an owner throughout this handoff.
	client.pendingSubscriptions.CompareAndDelete(p.span.subID, p)
	return subscriptions
}

func (p *pendingSubscription) stop(client *subscriptionClient) {
	p.mu.Lock()
	if !p.active {
		p.span.stopped.Store(true)
		p.mu.Unlock()
		return
	}
	split := p.split
	p.mu.Unlock()
	if split != nil {
		split.stop()
	} else {
		client.setTableStopped(p.span)
	}
}

type splitSubscriptionProgress struct {
	span       *subscribedSpan
	resolvedTs uint64
	ready      bool
}

// splitSubscription preserves one public subscription while each child owns a
// dynstream path and independently waits for its consumer acknowledgment.
// Only acknowledged child watermarks participate in the public minimum.
type splitSubscription struct {
	client *subscriptionClient
	parent *subscribedSpan

	mu         sync.Mutex
	children   map[SubscriptionID]*splitSubscriptionProgress
	resolvedTs uint64
	notified   bool

	// Serialize consumer invocations without waiting for their acknowledgments.
	// Progress has a separate lock because a consumer may acknowledge inline.
	consumeMu sync.Mutex
	advanceMu sync.Mutex
}

func newSplitSubscription(client *subscriptionClient, parent *subscribedSpan) *splitSubscription {
	return &splitSubscription{
		client:     client,
		parent:     parent,
		children:   make(map[SubscriptionID]*splitSubscriptionProgress),
		resolvedTs: parent.startTs,
	}
}

// prepare registers every child at startTs before scheduling any Region. This
// prevents a fast child from advancing past a not-yet-discovered tail.
func (g *splitSubscription) prepare(spans []heartbeatpb.TableSpan) []*subscribedSpan {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parent.stopped.Load() {
		return nil
	}
	children := make([]*subscribedSpan, 0, len(spans))
	for _, span := range spans {
		id := g.client.AllocSubscriptionID()
		child := newSubscribedSpan(
			g.client.ctx, g.client.resolveLockRateLimiter, g.client.resolveLockTaskCh,
			id, span, g.parent.startTs, g.consume,
			func(ts uint64) { g.advance(id, ts) },
			g.parent.advanceInterval, g.parent.filterLoop,
			g.client.upstream.pdClock, g.parent.priorityPolicy.lagThreshold,
		)
		child.baseScanPriority = g.parent.baseScanPriority
		child.splitSubscription = g
		g.children[id] = &splitSubscriptionProgress{span: child, resolvedTs: child.startTs}
		g.client.spanRegistry.Add(child)
		g.client.eventSink.AddPath(child)
		children = append(children, child)
	}
	return children
}

func (g *splitSubscription) consume(events []common.RawKVEntry, wakeCallback func()) bool {
	g.consumeMu.Lock()
	defer g.consumeMu.Unlock()
	if g.parent.stopped.Load() {
		return false
	}
	return g.parent.consumeKVEvents(events, wakeCallback)
}

func (g *splitSubscription) advance(id SubscriptionID, ts uint64) {
	// The public callback must remain serialized even when children complete
	// concurrently. Never invoke it under mu, which also guards child teardown.
	g.advanceMu.Lock()
	defer g.advanceMu.Unlock()
	g.mu.Lock()
	child, ok := g.children[id]
	if !ok || g.parent.stopped.Load() {
		g.mu.Unlock()
		return
	}
	child.resolvedTs = max(child.resolvedTs, ts)
	child.ready = true
	minimum := child.resolvedTs
	for _, progress := range g.children {
		if !progress.ready {
			g.mu.Unlock()
			return
		}
		minimum = min(minimum, progress.resolvedTs)
	}
	if g.notified && minimum <= g.resolvedTs {
		g.mu.Unlock()
		return
	}
	g.resolvedTs = minimum
	g.notified = true
	g.mu.Unlock()
	g.parent.recordResolvedTs(minimum)
	g.parent.advanceResolvedTs(minimum)
}

func (g *splitSubscription) stop() {
	g.mu.Lock()
	if !g.parent.stopped.CompareAndSwap(false, true) {
		g.mu.Unlock()
		return
	}
	children := make([]*subscribedSpan, 0, len(g.children))
	for _, child := range g.children {
		children = append(children, child.span)
	}
	g.removeIfDrained()
	g.mu.Unlock()
	for _, child := range children {
		g.client.setTableStopped(child)
	}
}

func (g *splitSubscription) drained(id SubscriptionID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.children, id)
	g.removeIfDrained()
}

// removeIfDrained is called with mu held. The child set is fixed before the
// split subscription is published and can only shrink during teardown.
func (g *splitSubscription) removeIfDrained() {
	if g.parent.stopped.Load() && len(g.children) == 0 {
		g.client.splitSubscriptions.CompareAndDelete(g.parent.subID, g)
	}
}
