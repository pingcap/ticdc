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

type TopicGeneratorType int

const (
	StaticTopicGeneratorType TopicGeneratorType = iota
	DynamicTopicGeneratorType
)

type Generator interface {
	Substitute(schema, table string) string
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

func (s *StaticTopicGenerator) TopicGeneratorType() TopicGeneratorType {
	return StaticTopicGeneratorType
}

// DynamicTopicGenerator is a topic generator which dispatches rows and DDLs
// dynamically to the target topics.
type DynamicTopicGenerator struct {
	expression Expression
}

// NewDynamicTopicDispatcher creates a DynamicTopicDispatcher.
func newDynamicTopicGenerator(topicExpr Expression) *DynamicTopicGenerator {
	return &DynamicTopicGenerator{
		expression: topicExpr,
	}
}

// Substitute converts schema/table name in a topic expression to kafka topic name.
func (d *DynamicTopicGenerator) Substitute(schema, table string) string {
	return d.expression.Substitute(schema, table)
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
