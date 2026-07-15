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

package main

import "testing"

func TestNextInsertTableIndexUniform(t *testing.T) {
	app := &WorkloadApp{Config: &WorkloadConfig{
		TableCount:      3,
		TableStartIndex: 4,
		UniformWrite:    true,
	}}

	var actual []int
	for range 9 {
		actual = append(actual, app.nextInsertTableIndex())
	}
	expected := []int{4, 5, 6, 4, 5, 6, 4, 5, 6}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Fatalf("table index %d = %d, want %d (all indexes: %v)", i, actual[i], expected[i], actual)
		}
	}
}

func TestValidateUniformWritePayload(t *testing.T) {
	tests := []struct {
		name    string
		config  WorkloadConfig
		wantErr bool
	}{
		{
			name: "valid payload",
			config: WorkloadConfig{
				WorkloadType:        bank3,
				Partitioned:         false,
				UniformWrite:        true,
				UniformPayloadBytes: 2048,
			},
		},
		{
			name: "payload without uniform write",
			config: WorkloadConfig{
				WorkloadType:        bank3,
				UniformPayloadBytes: 2048,
			},
			wantErr: true,
		},
		{
			name: "payload too large",
			config: WorkloadConfig{
				WorkloadType:        bank3,
				Partitioned:         false,
				UniformWrite:        true,
				UniformPayloadBytes: 4097,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.validateUniformWrite()
			if (err != nil) != test.wantErr {
				t.Fatalf("validateUniformWrite() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
