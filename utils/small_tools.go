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

package utils

func SliceToSet[E comparable](s []E) map[E]struct{} {
	m := make(map[E]struct{}, len(s))
	for _, k := range s {
		m[k] = struct{}{}
	}
	return m
}

func CopySetToSet[E comparable](from map[E]struct{}, to map[E]struct{}) {
	for k := range from {
		to[k] = struct{}{}
	}
}
