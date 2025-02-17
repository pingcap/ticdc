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

package util

import (
	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/errors"
	"strings"
)

// StrictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func StrictDecodeFile(path, component string, cfg interface{}, ignoreCheckItems ...string) error {
	metaData, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	// check if item is a ignoreCheckItem
	hasIgnoreItem := func(item []string) bool {
		for _, ignoreCheckItem := range ignoreCheckItems {
			if item[0] == ignoreCheckItem {
				return true
			}
		}
		return false
	}

	if undecoded := metaData.Undecoded(); len(undecoded) > 0 {
		var b strings.Builder
		hasUnknownConfigSize := 0
		for _, item := range undecoded {
			if hasIgnoreItem(item) {
				continue
			}

			if hasUnknownConfigSize > 0 {
				b.WriteString(", ")
			}
			b.WriteString(item.String())
			hasUnknownConfigSize++
		}
		if hasUnknownConfigSize > 0 {
			err = errors.Errorf("component %s's config file %s contained unknown configuration options: %s",
				component, path, b.String())
		}
	}
	return errors.Trace(err)
}
