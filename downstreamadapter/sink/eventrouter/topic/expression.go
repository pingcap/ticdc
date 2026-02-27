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
	"regexp"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

var (
	// hardCodeTopicNameRe is used to match a topic name which is hard coded in the config.
	hardCodeTopicNameRe = regexp.MustCompile(`^([A-Za-z0-9\._\-]+)$`)
	// kafkaForbidRE is used to reject the characters which are forbidden in kafka topic name.
	kafkaForbidRE = regexp.MustCompile(`[^a-zA-Z0-9\._\-]`)
	// genericTopicNameRe validates non-pulsar topic names after placeholder substitution.
	genericTopicNameRe = regexp.MustCompile(`^[A-Za-z0-9\._\-]*$`)
	// pulsarFullTopicNameRe validates full pulsar topic names after placeholder substitution.
	pulsarFullTopicNameRe = regexp.MustCompile(`^(persistent|non-persistent)://[A-Za-z0-9._-]+/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$`)

	// The max length of kafka topic name is 249.
	// See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L35
	kafkaTopicNameMaxLength = 249
)

type exprTokenType int

const (
	tokenLiteral exprTokenType = iota
	tokenSchema
	tokenTable
	tokenColumn
)

type exprToken struct {
	typ   exprTokenType
	value string
}

// Expression represent a kafka topic expression.
// The expression can contain literals and placeholders:
// {schema}, {table}, {column:<column-name>}.
type Expression string

// Validate checks whether a kafka topic name is valid or not.
func (e Expression) validate() error {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e)
	}
	for _, token := range tokens {
		if token.typ != tokenLiteral {
			continue
		}
		if !genericTopicNameRe.MatchString(token.value) {
			return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e)
		}
	}
	return nil
}

// ValidateForAvro checks whether topic pattern contains both {schema} and {table}.
func (e Expression) validateForAvro() error {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e,
			"topic rule for Avro must contain {schema} and {table}",
		)
	}

	hasSchema, hasTable := false, false
	for _, token := range tokens {
		switch token.typ {
		case tokenLiteral:
			if !genericTopicNameRe.MatchString(token.value) {
				return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e,
					"topic rule for Avro must contain {schema} and {table}",
				)
			}
		case tokenSchema:
			hasSchema = true
		case tokenTable:
			hasTable = true
		case tokenColumn:
			return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e,
				"topic rule for Avro does not support {column:<name>} placeholder",
			)
		}
	}
	if !hasSchema || !hasTable {
		return errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e,
			"topic rule for Avro must contain {schema} and {table}",
		)
	}
	return nil
}

// PulsarValidate checks whether a pulsar topic name is valid or not.
func (e Expression) validateForPulsar() error {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs("invalid topic expression")
	}
	columnValues := make(map[string]string)
	for _, token := range tokens {
		if token.typ != tokenColumn {
			continue
		}
		columnValues[token.value] = "v"
		columnValues[strings.ToLower(token.value)] = "v"
	}
	substituted := substituteTokens(tokens, "schema", "table", columnValues)
	if len(substituted) == 0 {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs("topic name is empty")
	}

	if strings.HasPrefix(substituted, "persistent://") || strings.HasPrefix(substituted, "non-persistent://") {
		if !pulsarFullTopicNameRe.MatchString(substituted) {
			return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
				"it should be in the format of <tenant>/<namespace>/<topic> or <topic>")
		}
		return nil
	}

	if strings.Contains(substituted, "/") {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
			"simple topic name must not contain '/'")
	}
	if !genericTopicNameRe.MatchString(substituted) {
		return errors.ErrPulsarInvalidTopicExpression.GenWithStackByArgs(
			"topic contains invalid characters")
	}
	return nil
}

// Substitute converts schema/table name in a topic expression to topic name.
// It does not support expressions with {column:<name>} placeholder.
func (e Expression) Substitute(schema, table string) string {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return ""
	}
	for _, token := range tokens {
		if token.typ == tokenColumn {
			log.Panic("topic expression with column placeholder requires row context",
				zap.String("expression", string(e)),
				zap.String("schema", schema),
				zap.String("table", table))
		}
	}
	return applyTopicNameConstraints(substituteTokens(tokens, schema, table, nil))
}

// SubstituteWithValues converts schema/table/column placeholder into a topic name.
func (e Expression) SubstituteWithValues(
	schema, table string, columnValues map[string]string,
) (string, error) {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return "", errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(e)
	}
	return substituteTokensWithMap(tokens, schema, table, columnValues)
}

func (e Expression) UsesColumnPlaceholders() bool {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return false
	}
	for _, token := range tokens {
		if token.typ == tokenColumn {
			return true
		}
	}
	return false
}

func (e Expression) ReferencedColumns() []string {
	tokens, err := parseExpressionTokens(string(e))
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{})
	columns := make([]string, 0)
	for _, token := range tokens {
		if token.typ != tokenColumn {
			continue
		}
		lowerName := strings.ToLower(token.value)
		if _, ok := seen[lowerName]; ok {
			continue
		}
		seen[lowerName] = struct{}{}
		columns = append(columns, token.value)
	}
	return columns
}

// IsHardCode checks whether a topic name is hard coded or not.
func isHardCode(topicName string) bool {
	return hardCodeTopicNameRe.MatchString(topicName)
}

func validateTopicExpression(expr Expression, isPulsar, isAvro bool) error {
	if isPulsar {
		return expr.validateForPulsar()
	}
	if isAvro {
		return expr.validateForAvro()
	}
	return expr.validate()
}

func parseExpressionTokens(expression string) ([]exprToken, error) {
	if len(expression) == 0 {
		return nil, errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(expression)
	}
	tokens := make([]exprToken, 0, 8)
	for i := 0; i < len(expression); {
		open := strings.IndexByte(expression[i:], '{')
		if open < 0 {
			tokens = append(tokens, exprToken{typ: tokenLiteral, value: expression[i:]})
			break
		}
		open += i
		if open > i {
			tokens = append(tokens, exprToken{typ: tokenLiteral, value: expression[i:open]})
		}
		close := strings.IndexByte(expression[open+1:], '}')
		if close < 0 {
			return nil, errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(expression)
		}
		close += open + 1
		placeholder := expression[open+1 : close]
		switch {
		case placeholder == "schema":
			tokens = append(tokens, exprToken{typ: tokenSchema})
		case placeholder == "table":
			tokens = append(tokens, exprToken{typ: tokenTable})
		case strings.HasPrefix(placeholder, "column:"):
			columnName := strings.TrimSpace(strings.TrimPrefix(placeholder, "column:"))
			if columnName == "" {
				return nil, errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(expression)
			}
			tokens = append(tokens, exprToken{typ: tokenColumn, value: columnName})
		default:
			return nil, errors.ErrKafkaInvalidTopicExpression.GenWithStackByArgs(expression)
		}
		i = close + 1
	}
	return tokens, nil
}

func substituteTokensWithMap(
	tokens []exprToken, schema, table string, columnValues map[string]string,
) (string, error) {
	topicName := substituteTokens(tokens, schema, table, columnValues)
	var err error
	if strings.Contains(topicName, "\x00") {
		err = errors.ErrDispatcherFailed.GenWithStack("invalid null character in topic name")
	}
	if err != nil {
		return "", err
	}
	for _, token := range tokens {
		if token.typ != tokenColumn {
			continue
		}
		if columnValues == nil {
			return "", errors.ErrDispatcherFailed.GenWithStack(
				"column placeholder requires row context, column: %s", token.value)
		}
		if _, ok := findColumnValue(columnValues, token.value); !ok {
			return "", errors.ErrDispatcherFailed.GenWithStack(
				"column value for topic dispatch is not found, column: %s", token.value)
		}
	}
	return applyTopicNameConstraints(topicName), nil
}

func substituteTokens(tokens []exprToken, schema, table string, columnValues map[string]string) string {
	var b strings.Builder
	for _, token := range tokens {
		switch token.typ {
		case tokenLiteral:
			b.WriteString(token.value)
		case tokenSchema:
			b.WriteString(sanitizeTopicValue(schema))
		case tokenTable:
			b.WriteString(sanitizeTopicValue(table))
		case tokenColumn:
			columnValue, _ := findColumnValue(columnValues, token.value)
			b.WriteString(sanitizeTopicValue(columnValue))
		}
	}
	return b.String()
}

func findColumnValue(columnValues map[string]string, columnName string) (string, bool) {
	if columnValues == nil {
		return "", false
	}
	if v, ok := columnValues[columnName]; ok {
		return v, true
	}
	if v, ok := columnValues[strings.ToLower(columnName)]; ok {
		return v, true
	}
	return "", false
}

func sanitizeTopicValue(value string) string {
	return kafkaForbidRE.ReplaceAllString(value, "_")
}

func applyTopicNameConstraints(topicName string) string {
	if len(topicName) > kafkaTopicNameMaxLength {
		return topicName[:kafkaTopicNameMaxLength]
	}
	switch topicName {
	case ".":
		return "_"
	case "..":
		return "__"
	default:
		return topicName
	}
}
