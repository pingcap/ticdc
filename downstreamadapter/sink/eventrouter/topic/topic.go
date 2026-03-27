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

package topic

import (
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TopicGeneratorType int

const (
	StaticTopicGeneratorType TopicGeneratorType = iota
	DynamicTopicGeneratorType
)

type Generator interface {
	Substitute(schema, table string) string
	SubstituteWithValues(schema, table string, columnValues map[string]string) (string, error)
	UsesColumnPlaceholders() bool
	ReferencedColumns() []string
	TopicGeneratorType() TopicGeneratorType
}

type StaticTopicGenerator struct {
	topic string
}

// NewStaticTopicDispatcher returns a StaticTopicDispatcher.
func newStaticTopic(defaultTopic string) *StaticTopicGenerator {
	return &StaticTopicGenerator{
		topic: defaultTopic,
	}
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (s *StaticTopicGenerator) Substitute(schema, table string) string {
	return s.topic
}

// SubstituteWithValues returns the configured static topic without consulting
// row values.
func (s *StaticTopicGenerator) SubstituteWithValues(
	schema, table string, columnValues map[string]string,
) (string, error) {
	// StaticTopicGenerator does not use column values, so we ignore the
	// columnValues parameter and return the static topic directly.
	return s.topic, nil
}

// UsesColumnPlaceholders reports whether the static topic depends on row data.
func (s *StaticTopicGenerator) UsesColumnPlaceholders() bool {
	return false
}

// ReferencedColumns returns nil because a static topic does not reference row
// columns.
func (s *StaticTopicGenerator) ReferencedColumns() []string {
	return nil
}

func (s *StaticTopicGenerator) TopicGeneratorType() TopicGeneratorType {
	return StaticTopicGeneratorType
}

// DynamicTopicGenerator is a topic generator which dispatches rows and DDLs
// dynamically to the target topics.
type DynamicTopicGenerator struct {
	expression             Expression
	tokens                 []exprToken
	usesColumnPlaceholders bool
	referencedColumns      []string
}

// NewDynamicTopicDispatcher creates a DynamicTopicDispatcher.
func newDynamicTopicGenerator(topicExpr Expression) *DynamicTopicGenerator {
	// parseExpressionTokens must succeed here: the expression is validated by
	// validateTopicExpression before this constructor is called. A failure
	// indicates a programming error.
	tokens, err := parseExpressionTokens(string(topicExpr))
	if err != nil {
		log.Panic("dynamic topic generator constructed with invalid expression",
			zap.String("expression", string(topicExpr)),
			zap.Error(err))
	}

	usesCols := false
	seen := make(map[string]struct{})
	cols := make([]string, 0)
	for _, tok := range tokens {
		if tok.typ == tokenColumn {
			usesCols = true
			lower := strings.ToLower(tok.value)
			if _, ok := seen[lower]; !ok {
				seen[lower] = struct{}{}
				cols = append(cols, tok.value)
			}
		}
	}

	return &DynamicTopicGenerator{
		expression:             topicExpr,
		tokens:                 tokens,
		usesColumnPlaceholders: usesCols,
		referencedColumns:      cols,
	}
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (d *DynamicTopicGenerator) Substitute(schema, table string) string {
	if d.usesColumnPlaceholders {
		log.Panic("topic expression with column placeholder requires row context",
			zap.String("expression", string(d.expression)),
			zap.String("schema", schema),
			zap.String("table", table))
	}
	return applyTopicNameConstraints(substituteTokens(d.tokens, schema, table, nil))
}

// SubstituteWithValues resolves the dynamic topic using schema, table, and row
// values.
func (d *DynamicTopicGenerator) SubstituteWithValues(
	schema, table string, columnValues map[string]string,
) (string, error) {
	return substituteTokensWithMap(d.tokens, schema, table, columnValues)
}

// UsesColumnPlaceholders reports whether the dynamic topic requires row data to
// be resolved.
func (d *DynamicTopicGenerator) UsesColumnPlaceholders() bool {
	return d.usesColumnPlaceholders
}

// ReferencedColumns returns a copy of the row columns needed to resolve the
// dynamic topic.
func (d *DynamicTopicGenerator) ReferencedColumns() []string {
	return append([]string(nil), d.referencedColumns...)
}

func (d *DynamicTopicGenerator) TopicGeneratorType() TopicGeneratorType {
	return DynamicTopicGeneratorType
}

func GetTopicGenerator(
	rule string, defaultTopic string, isPulsar bool, isAvro bool,
) (Generator, error) {
	if rule == "" {
		return newStaticTopic(defaultTopic), nil
	}

	if isHardCode(rule) {
		return newStaticTopic(rule), nil
	}

	// check if this rule is a valid topic expression
	topicExpr := Expression(rule)
	err := validateTopicExpression(topicExpr, isPulsar, isAvro)
	if err != nil {
		return nil, err
	}
	return newDynamicTopicGenerator(topicExpr), nil
}
