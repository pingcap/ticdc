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
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

// Routing expression placeholders that can be used in SchemaRule and TableRule.
const (
	// SchemaPlaceholder is replaced with the source schema name in routing expressions.
	SchemaPlaceholder = "{schema}"
	// TablePlaceholder is replaced with the source table name in routing expressions.
	TablePlaceholder = "{table}"
)

// Router routes source schema/table names to target schema/table names.
type Router struct {
	rules []*routingRule
}

// routingRule represents a single routing rule.
type routingRule struct {
	filter     tfilter.Filter
	schemaExpr string
	tableExpr  string
}

// RoutingRuleConfig represents configuration for a single routing rule.
type RoutingRuleConfig struct {
	// Matcher is a filter pattern like "db1.*" or "db1.table1"
	Matcher []string `toml:"matcher" json:"matcher"`
	// SchemaRule is an expression for the target schema, e.g., "{schema}" or "target_db"
	SchemaRule string `toml:"schema" json:"schema"`
	// TableRule is an expression for the target table, e.g., "{table}" or "target_table"
	TableRule string `toml:"table" json:"table"`
}

// NewRouter creates a new Router from a list of routing rule configurations.
// Returns nil if no rules are provided.
func NewRouter(caseSensitive bool, rules []RoutingRuleConfig) (*Router, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	routingRules := make([]*routingRule, 0, len(rules))

	for _, ruleConfig := range rules {
		if ruleConfig.SchemaRule == "" && ruleConfig.TableRule == "" {
			continue
		}

		if err := config.ValidateRoutingExpression(ruleConfig.SchemaRule); err != nil {
			return nil, fmt.Errorf("invalid schema rule %q: %w", ruleConfig.SchemaRule, err)
		}
		if err := config.ValidateRoutingExpression(ruleConfig.TableRule); err != nil {
			return nil, fmt.Errorf("invalid table rule %q: %w", ruleConfig.TableRule, err)
		}

		f, err := tfilter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, fmt.Errorf("invalid matcher %v: %w", ruleConfig.Matcher, err)
		}
		if !caseSensitive {
			f = tfilter.CaseInsensitive(f)
		}

		routingRules = append(routingRules, &routingRule{
			filter:     f,
			schemaExpr: ruleConfig.SchemaRule,
			tableExpr:  ruleConfig.TableRule,
		})
	}

	if len(routingRules) == 0 {
		return nil, nil
	}

	return &Router{rules: routingRules}, nil
}

// Route returns the target schema and table names for the given source schema/table.
// If no rule matches, returns the source schema and table unchanged.
func (r *Router) Route(sourceSchema, sourceTable string) (targetSchema, targetTable string) {
	if r == nil || len(r.rules) == 0 {
		return sourceSchema, sourceTable
	}

	rule := r.matchRule(sourceSchema, sourceTable)
	if rule == nil {
		return sourceSchema, sourceTable
	}

	targetSchema = substituteExpression(rule.schemaExpr, sourceSchema, sourceTable, sourceSchema)
	targetTable = substituteExpression(rule.tableExpr, sourceSchema, sourceTable, sourceTable)

	log.Debug("sink routing applied",
		zap.String("sourceSchema", sourceSchema),
		zap.String("sourceTable", sourceTable),
		zap.String("targetSchema", targetSchema),
		zap.String("targetTable", targetTable),
	)

	return targetSchema, targetTable
}

// matchRule finds the first rule that matches the given schema/table.
func (r *Router) matchRule(schema, table string) *routingRule {
	for _, rule := range r.rules {
		if rule.filter.MatchTable(schema, table) {
			return rule
		}
	}
	return nil
}

// substituteExpression replaces {schema} and {table} placeholders with actual values.
// If expr is empty, returns defaultValue (typically sourceSchema for schema expressions,
// sourceTable for table expressions).
func substituteExpression(expr, sourceSchema, sourceTable, defaultValue string) string {
	if expr == "" {
		return defaultValue
	}

	result := expr
	result = strings.ReplaceAll(result, SchemaPlaceholder, sourceSchema)
	result = strings.ReplaceAll(result, TablePlaceholder, sourceTable)
	return result
}

// NewRouterFromDispatchRules creates a new Router from dispatch rule configurations.
// This is a convenience function that extracts routing rules from DispatchRule configs.
func NewRouterFromDispatchRules(caseSensitive bool, rules []*config.DispatchRule) (*Router, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	routingConfigs := make([]RoutingRuleConfig, 0, len(rules))
	for _, rule := range rules {
		// Skip rules without routing configuration
		if rule.SchemaRule == "" && rule.TableRule == "" {
			continue
		}

		routingConfigs = append(routingConfigs, RoutingRuleConfig{
			Matcher:    rule.Matcher,
			SchemaRule: rule.SchemaRule,
			TableRule:  rule.TableRule,
		})
	}

	return NewRouter(caseSensitive, routingConfigs)
}
