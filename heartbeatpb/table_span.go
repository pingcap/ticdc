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

package heartbeatpb

import "bytes"

// Less compares two Spans, defines the order between spans.
func (s *TableSpan) Less(other *TableSpan) bool {
	if s.TableID < other.TableID {
		return true
	}
	if bytes.Compare(s.StartKey, other.StartKey) < 0 {
		return true
	}
	return false
}

func (s *TableSpan) Equal(other *TableSpan) bool {
	return s.TableID == other.TableID &&
		bytes.Equal(s.StartKey, other.StartKey) &&
		bytes.Equal(s.EndKey, other.EndKey) &&
		s.KeyspaceID == other.KeyspaceID
}

func (s *TableSpan) Copy() *TableSpan {
	return &TableSpan{
		TableID:    s.TableID,
		StartKey:   s.StartKey,
		EndKey:     s.EndKey,
		KeyspaceID: s.KeyspaceID,
	}
}
