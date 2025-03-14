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
	"math/rand"
	"strings"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandomBytes(r *rand.Rand, buffer []byte) {
	for i := range buffer {
		idx := 1
		if r == nil {
			idx = rand.Intn(len(letters))
		} else {
			idx = r.Intn(len(letters))
		}
		buffer[i] = letters[idx]
	}
}

// generateRandomString generates a random string of a given length
func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// to improve the efficiency, we generate a short random string, then repeat it
	baseLength := 100
	if length < baseLength {
		baseLength = length
	}

	b := make([]byte, baseLength)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	baseString := string(b)

	var builder strings.Builder
	repeats := length / baseLength
	remainder := length % baseLength

	for i := 0; i < repeats; i++ {
		builder.WriteString(baseString)
	}

	if remainder > 0 {
		builder.WriteString(baseString[:remainder])
	}

	return builder.String()
}

func GenerateRandomInt() int {
	return rand.Intn(1000000)
}
