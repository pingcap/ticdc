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

package main

import (
	"io"
	"os"
	"strings"
	"testing"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/spf13/cobra"
)

// go test -gcflags=all=-l
func Test_runConfig2Model(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		wantOutput string
		wantErr    bool
		setup      func()
		cleanup    func()
	}{
		{
			name: "valid config with expected output",
			config: `
memory-quota = 100
case-sensitive = false
`,
			wantOutput: `"memory_quota": 100`,
			wantErr:    false,
			setup: func() {
				file, err := os.CreateTemp("", "test-config-*.toml")
				if err != nil {
					t.Fatal(err)
				}
				cfgPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(cfgPath)
			},
		},
		{
			name:    "invalid config",
			config:  `invalid toml content`,
			wantErr: true,
			setup: func() {
				file, err := os.CreateTemp("", "test-config-*.toml")
				if err != nil {
					t.Fatal(err)
				}
				cfgPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(cfgPath)
			},
		},
		{
			name:    "non-existent file",
			wantErr: true,
			setup: func() {
				cfgPath = "/nonexistent/file.toml"
			},
		},
		{
			name:       "empty config",
			config:     ``,
			wantOutput: `"memory_quota": 0`,
			wantErr:    false,
			setup: func() {
				file, err := os.CreateTemp("", "test-config-*.toml")
				if err != nil {
					t.Fatal(err)
				}
				cfgPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(cfgPath)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
				if tt.config != "" {
					err := os.WriteFile(cfgPath, []byte(tt.config), 0o644)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}

			// Mock cobra.Command and args
			cmd := &cobra.Command{}
			args := []string{}

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = oldStdout
				w.Close()
			}()

			// Run the test
			func() {
				// Replace os.Exit to capture exit codes
				patches := gomonkey.ApplyFuncReturn(os.Exit)
				defer patches.Reset()

				defer func() {
					if r := recover(); r != nil {
						if !tt.wantErr {
							t.Errorf("unexpected panic: %v", r)
						}
					}
				}()
				modelPath = ""
				run(cmd, args)

				// Verify output for success case
				if !tt.wantErr {
					w.Close()
					out, _ := io.ReadAll(r)
					output := string(out)
					if tt.wantOutput != "" && !strings.Contains(output, tt.wantOutput) {
						t.Errorf("output doesn't contain expected content. Got: %s Want to contain: %s", output, tt.wantOutput)
					}
				}
			}()
		})
	}
}

func Test_runModel2Config(t *testing.T) {
	tests := []struct {
		name         string
		modelContent string
		wantOutput   string
		wantErr      bool
		setup        func()
		cleanup      func()
	}{
		{
			name:         "valid model with expected output",
			modelContent: `{"case_sensitive":true, "memory_quota": 2048}`,
			wantOutput:   `memory-quota = 2048`,
			wantErr:      false,
			setup: func() {
				file, err := os.CreateTemp("", "test-model-*.json")
				if err != nil {
					t.Fatal(err)
				}
				modelPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(modelPath)
			},
		},
		{
			name:         "invalid model",
			modelContent: `invalid json content`,
			wantErr:      true,
			setup: func() {
				file, err := os.CreateTemp("", "test-model-*.json")
				if err != nil {
					t.Fatal(err)
				}
				modelPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(modelPath)
			},
		},
		{
			name:    "non-existent file",
			wantErr: true,
			setup: func() {
				modelPath = "/nonexistent/file.json"
			},
		},
		{
			name:         "empty model",
			modelContent: `{}`,
			wantOutput:   `case-sensitive = false`,
			wantErr:      false,
			setup: func() {
				file, err := os.CreateTemp("", "test-model-*.json")
				if err != nil {
					t.Fatal(err)
				}
				modelPath = file.Name()
				file.Close()
			},
			cleanup: func() {
				os.Remove(modelPath)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
				if tt.modelContent != "" {
					err := os.WriteFile(modelPath, []byte(tt.modelContent), 0o644)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if tt.cleanup != nil {
				defer tt.cleanup()
			}

			// Mock cobra.Command and args
			cmd := &cobra.Command{}
			args := []string{}

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w
			defer func() {
				os.Stdout = oldStdout
				w.Close()
			}()

			// Run the test
			func() {
				// Replace os.Exit to capture exit codes
				patches := gomonkey.ApplyFuncReturn(os.Exit)
				defer patches.Reset()

				defer func() {
					if r := recover(); r != nil {
						if !tt.wantErr {
							t.Errorf("unexpected panic: %v", r)
						}
					}
				}()

				// Set cfgPath to empty to ensure runModel2Config is called
				cfgPath = ""
				run(cmd, args)

				// Verify output for success case
				if !tt.wantErr {
					w.Close()
					out, _ := io.ReadAll(r)
					output := string(out)
					if tt.wantOutput != "" && !strings.Contains(output, tt.wantOutput) {
						t.Errorf("output doesn't contain expected content. Got: %s Want to contain: %s", output, tt.wantOutput)
					}
				}
			}()
		})
	}
}
