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

package logservicepb

func (s *SubscriptionState) Copy() *SubscriptionState {
	return &SubscriptionState{
		SubID:        s.SubID,
		Span:         s.Span.Copy(),
		CheckpointTs: s.CheckpointTs,
		ResolvedTs:   s.ResolvedTs,
	}
}

func (t *TableState) Copy() *TableState {
	subs := make([]*SubscriptionState, 0, len(t.Subscriptions))
	for _, sub := range t.Subscriptions {
		subs = append(subs, sub.Copy())
	}
	return &TableState{
		Subscriptions: subs,
	}
}

func (e *EventStoreState) Copy() *EventStoreState {
	newState := &EventStoreState{
		TableStates: make(map[int64]*TableState),
	}
	for key, state := range e.TableStates {
		newState.TableStates[key] = state.Copy()
	}
	return newState
}
