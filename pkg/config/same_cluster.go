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
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"slices"
	"strings"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
)

// This file decides whether a changefeed which may replicate into its own cluster
// (`allow-same-cluster`) can capture the writes of its own sink.
//
// Let S be the tables the filter replicates and route(t) the target name of the routing rules.
// The changefeed is safe exactly when route(S) ∩ S = ∅, that is when no replicated table is routed
// to a table which the filter replicates as well. The decision here is exact for the supported
// pattern class (see parseNamePattern): it searches for a witness table which the filter replicates
// and whose routed target the filter replicates as well, and rejects the configuration only when
// such a witness exists. Configurations outside the supported class are rejected as unsupported
// instead of being approximated.
//
// The check reasons about the patterns instead of the tables which exist at that moment, so it
// covers tables created later as well.

const (
	schemaPlaceholder = "{schema}"
	tablePlaceholder  = "{table}"
	// unsupportedPatternChars are characters which this check does not parse, so a pattern which
	// contains one of them is rejected as unsupported.
	unsupportedPatternChars = "*?[]{}/\\`\""
)

// namePatternKind classifies the supported pattern forms.
type namePatternKind int

const (
	// patternAny is "*".
	patternAny namePatternKind = iota
	// patternLiteral is a plain name.
	patternLiteral
	// patternPrefix is "prefix*".
	patternPrefix
	// patternSuffix is "*suffix".
	patternSuffix
)

// namePattern is one side (schema or table) of a filter rule or a routing matcher.
type namePattern struct {
	kind  namePatternKind
	value string
}

// matches reports whether a concrete name matches the pattern.
func (p namePattern) matches(name string) bool {
	switch p.kind {
	case patternAny:
		return true
	case patternLiteral:
		return name == p.value
	case patternPrefix:
		return strings.HasPrefix(name, p.value)
	case patternSuffix:
		return strings.HasSuffix(name, p.value)
	default:
		return false
	}
}

// covers reports whether every name matching the filter pattern matches the matcher pattern.
func (p namePattern) covers(matcher namePattern) bool {
	switch matcher.kind {
	case patternAny:
		return true
	case patternLiteral:
		return p.kind == patternLiteral && p.value == matcher.value
	case patternPrefix:
		return (p.kind == patternLiteral || p.kind == patternPrefix) && strings.HasPrefix(p.value, matcher.value)
	case patternSuffix:
		return (p.kind == patternLiteral || p.kind == patternSuffix) && strings.HasSuffix(p.value, matcher.value)
	default:
		return false
	}
}

// substitution is the literal text around a placeholder in a route target expression, so the
// target name of sub.apply(name) is prefix + name + suffix.
type substitution struct {
	prefix string
	suffix string
}

func (s substitution) apply(name string) string {
	return s.prefix + name + s.suffix
}

// targetName is a parsed route target expression: literal text around one placeholder, or a literal
// name. An empty expression keeps the source name, which is an empty substitution.
type targetName struct {
	literal     string
	sub         substitution
	hasVariable bool
}

func (t targetName) apply(name string) string {
	if t.hasVariable {
		return t.sub.apply(name)
	}
	return t.literal
}

// requirement is a conjunction of constraints over one name: the name is a given value, or it
// starts with and ends with given text.
type requirement struct {
	exact    *string
	starts   []string
	ends     []string
	conflict bool
}

func (r requirement) merge(other requirement) requirement {
	if r.exact != nil && other.exact != nil && *r.exact != *other.exact {
		r.conflict = true
	}
	if other.exact != nil {
		r.exact = other.exact
	}
	r.starts = slices.Concat(r.starts, other.starts)
	r.ends = slices.Concat(r.ends, other.ends)
	return r
}

// name returns a concrete name which satisfies every constraint, if one exists.
func (r requirement) name() (string, bool) {
	if r.conflict {
		return "", false
	}
	if r.exact != nil {
		name := *r.exact
		for _, prefix := range r.starts {
			if !strings.HasPrefix(name, prefix) {
				return "", false
			}
		}
		for _, suffix := range r.ends {
			if !strings.HasSuffix(name, suffix) {
				return "", false
			}
		}
		return name, true
	}

	head := ""
	for _, prefix := range r.starts {
		if len(prefix) > len(head) {
			if head != "" && !strings.HasPrefix(prefix, head) {
				return "", false
			}
			head = prefix
			continue
		}
		if !strings.HasPrefix(head, prefix) {
			return "", false
		}
	}
	tail := ""
	for _, suffix := range r.ends {
		if len(suffix) > len(tail) {
			if tail != "" && !strings.HasSuffix(suffix, tail) {
				return "", false
			}
			tail = suffix
			continue
		}
		if !strings.HasSuffix(tail, suffix) {
			return "", false
		}
	}
	if head == "" && tail == "" {
		return "x", true
	}
	return head + tail, true
}

// requirements lists the alternatives which make `sub.apply(name)` match the pattern, expressed as
// constraints on name. No alternative means the pattern can never match a substituted name.
func (p namePattern) requirements(sub substitution) []requirement {
	switch p.kind {
	case patternAny:
		return []requirement{{}}
	case patternLiteral:
		value, ok := trimLiteral(p.value, sub)
		if !ok {
			return nil
		}
		return []requirement{{exact: &value}}
	case patternPrefix:
		return prefixRequirements(p.value, sub)
	case patternSuffix:
		return suffixRequirements(p.value, sub)
	default:
		return nil
	}
}

// trimLiteral removes the substitution from a literal pattern value.
func trimLiteral(value string, sub substitution) (string, bool) {
	if !strings.HasPrefix(value, sub.prefix) || !strings.HasSuffix(value, sub.suffix) {
		return "", false
	}
	if len(value) < len(sub.prefix)+len(sub.suffix) {
		return "", false
	}
	middle := value[len(sub.prefix) : len(value)-len(sub.suffix)]
	if middle == "" {
		return "", false
	}
	return middle, true
}

// prefixRequirements lists the alternatives for "prefix + name + suffix starts with need".
func prefixRequirements(need string, sub substitution) []requirement {
	if len(need) <= len(sub.prefix) {
		// The prefix covers the requirement on its own.
		if !strings.HasPrefix(sub.prefix, need) {
			return nil
		}
		return []requirement{{}}
	}
	if !strings.HasPrefix(need, sub.prefix) {
		return nil
	}
	rest := need[len(sub.prefix):]
	alternatives := []requirement{{starts: []string{rest}}}
	// The name can be shorter than rest when the suffix covers its tail.
	for length := 1; length < len(rest); length++ {
		if strings.HasPrefix(sub.suffix, rest[length:]) {
			value := rest[:length]
			alternatives = append(alternatives, requirement{exact: &value})
		}
	}
	return alternatives
}

// suffixRequirements lists the alternatives for "prefix + name + suffix ends with need".
func suffixRequirements(need string, sub substitution) []requirement {
	if len(need) <= len(sub.suffix) {
		// The suffix covers the requirement on its own.
		if !strings.HasSuffix(sub.suffix, need) {
			return nil
		}
		return []requirement{{}}
	}
	if !strings.HasSuffix(need, sub.suffix) {
		return nil
	}
	rest := need[:len(need)-len(sub.suffix)]
	alternatives := []requirement{{ends: []string{rest}}}
	// The name can be shorter than rest when the prefix covers its head.
	for length := 1; length < len(rest); length++ {
		if strings.HasSuffix(sub.prefix, rest[:len(rest)-length]) {
			value := rest[len(rest)-length:]
			alternatives = append(alternatives, requirement{exact: &value})
		}
	}
	return alternatives
}

// tablePattern is a parsed `schema.table` pattern: a filter rule, or a matcher of a dispatch rule
// which has a target.
type tablePattern struct {
	raw    string
	schema namePattern
	table  namePattern
}

func (p tablePattern) matches(schema, table string) bool {
	return p.schema.matches(schema) && p.table.matches(table)
}

// routeRuleToCheck is a parsed dispatch rule which has a target.
type routeRuleToCheck struct {
	rawMatchers []string
	matchers    []tablePattern
	schema      targetName
	table       targetName
}

func (r routeRuleToCheck) target(schema, table string) (string, string) {
	return r.schema.apply(schema), r.table.apply(table)
}

// validateSameClusterRouting rejects a changefeed which replicates into the same cluster as its
// upstream unless it is proven that it cannot capture the writes of its own sink.
func (c *ReplicaConfig) validateSameClusterRouting() error {
	if !util.GetOrZero(c.AllowSameCluster) {
		return nil
	}
	if !c.Sink.TableRouteEnabled() {
		return errors.ErrInvalidReplicaConfig.FastGenByArgs("allow-same-cluster requires table routing to be enabled")
	}

	caseSensitive := util.GetOrZero(c.CaseSensitive)
	normalize := func(name string) string {
		if caseSensitive {
			return name
		}
		return strings.ToLower(name)
	}

	filters, err := parseFilterRules(effectiveFilterRules(c.Filter), normalize)
	if err != nil {
		return err
	}
	routes, err := parseRouteRules(c.Sink.DispatchRules, normalize)
	if err != nil {
		return err
	}
	if err := checkFilterRulesCovered(filters, routes); err != nil {
		return err
	}
	return checkRouteTargets(filters, routes)
}

// effectiveFilterRules returns the filter rules which are in effect. It mirrors
// pkg/filter.VerifyTableRules, where unset rules replicate every table.
func effectiveFilterRules(cfg *FilterConfig) []string {
	if cfg == nil || len(cfg.Rules) == 0 {
		return []string{"*.*"}
	}
	return cfg.Rules
}

func parseFilterRules(rules []string, normalize func(string) string) ([]tablePattern, error) {
	parsed := make([]tablePattern, 0, len(rules))
	for _, rule := range rules {
		pattern, err := parseRulePattern(rule, normalize)
		if err != nil {
			return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err,
				"allow-same-cluster does not support the filter rule "+rule)
		}
		parsed = append(parsed, pattern)
	}
	return parsed, nil
}

func parseRouteRules(rules []*DispatchRule, normalize func(string) string) ([]routeRuleToCheck, error) {
	parsed := make([]routeRuleToCheck, 0, len(rules))
	for _, rule := range rules {
		// Rules without a target keep the table name and are ignored by the router.
		if rule == nil || (rule.TargetSchema == "" && rule.TargetTable == "") {
			continue
		}
		route := routeRuleToCheck{rawMatchers: rule.Matcher}
		for _, matcher := range rule.Matcher {
			pattern, err := parseRulePattern(matcher, normalize)
			if err != nil {
				return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err,
					"allow-same-cluster does not support the dispatch rule matcher "+matcher)
			}
			route.matchers = append(route.matchers, pattern)
		}
		var err error
		if route.schema, err = parseTargetExpression(rule.TargetSchema, schemaPlaceholder); err != nil {
			return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err,
				"allow-same-cluster does not support the target schema of the dispatch rule matching "+strings.Join(rule.Matcher, ","))
		}
		if route.table, err = parseTargetExpression(rule.TargetTable, tablePlaceholder); err != nil {
			return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err,
				"allow-same-cluster does not support the target table of the dispatch rule matching "+strings.Join(rule.Matcher, ","))
		}
		parsed = append(parsed, route)
	}
	return parsed, nil
}

// parseRulePattern parses a `schema.table` pattern into its two parts.
func parseRulePattern(pattern string, normalize func(string) string) (tablePattern, error) {
	schemaPart, tablePart, ok := splitRulePattern(pattern)
	if !ok {
		return tablePattern{}, errors.New("expected a `schema.table` pattern")
	}
	schema, ok := parseNamePattern(normalize(schemaPart))
	if !ok {
		return tablePattern{}, errors.New("unsupported schema pattern")
	}
	table, ok := parseNamePattern(normalize(tablePart))
	if !ok {
		return tablePattern{}, errors.New("unsupported table pattern")
	}
	return tablePattern{raw: pattern, schema: schema, table: table}, nil
}

// splitRulePattern splits a `schema.table` pattern at its single dot. A dot inside a quoted name
// does not split the pattern.
func splitRulePattern(pattern string) (string, string, bool) {
	var quote byte
	dot := -1
	for i := range len(pattern) {
		switch c := pattern[i]; {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '`' || c == '"':
			quote = c
		case c == '.':
			if dot >= 0 {
				return "", "", false
			}
			dot = i
		}
	}
	if quote != 0 || dot < 0 {
		return "", "", false
	}
	return pattern[:dot], pattern[dot+1:], true
}

// parseNamePattern parses one part of a pattern. Supported forms are a literal name, `*`, a single
// leading `*`, a single trailing `*`, and quoted names.
func parseNamePattern(part string) (namePattern, bool) {
	if part == "" {
		return namePattern{}, false
	}
	switch part[0] {
	case '!':
		// Negated rules depend on the rule order of the table filter, which this check does not
		// reason about.
		return namePattern{}, false
	case '`', '"':
		name, ok := unquoteName(part)
		if !ok {
			return namePattern{}, false
		}
		return namePattern{kind: patternLiteral, value: name}, true
	}
	if rest, ok := strings.CutPrefix(part, "*"); ok {
		if rest == "" {
			return namePattern{kind: patternAny}, true
		}
		if strings.ContainsAny(rest, unsupportedPatternChars) {
			return namePattern{}, false
		}
		return namePattern{kind: patternSuffix, value: rest}, true
	}
	if rest, ok := strings.CutSuffix(part, "*"); ok {
		if strings.ContainsAny(rest, unsupportedPatternChars) {
			return namePattern{}, false
		}
		return namePattern{kind: patternPrefix, value: rest}, true
	}
	if strings.ContainsAny(part, unsupportedPatternChars) {
		return namePattern{}, false
	}
	return namePattern{kind: patternLiteral, value: part}, true
}

// unquoteName removes a pair of backticks or double quotes around a name.
func unquoteName(part string) (string, bool) {
	if len(part) < 2 || part[len(part)-1] != part[0] {
		return "", false
	}
	quote := part[0]
	name := strings.ReplaceAll(part[1:len(part)-1], strings.Repeat(string(quote), 2), string(quote))
	if strings.Contains(name, "*") {
		return "", false
	}
	return name, true
}

// parseTargetExpression parses a route target expression. An empty expression keeps the source
// name, which is an empty substitution.
func parseTargetExpression(expr, placeholder string) (targetName, error) {
	if expr == "" {
		return targetName{hasVariable: true}, nil
	}
	head, tail, found := strings.Cut(expr, placeholder)
	if !found {
		if strings.ContainsAny(expr, "{}") {
			return targetName{}, errors.New("expected literal text or a single " + placeholder + " placeholder")
		}
		return targetName{literal: expr}, nil
	}
	if strings.ContainsAny(head+tail, "{}") {
		return targetName{}, errors.New("expected literal text or a single " + placeholder + " placeholder")
	}
	return targetName{sub: substitution{prefix: head, suffix: tail}, hasVariable: true}, nil
}

// checkFilterRulesCovered rejects filter rules which no dispatch rule with a target would route:
// such a table keeps its own name and is replicated into itself.
func checkFilterRulesCovered(filters []tablePattern, routes []routeRuleToCheck) error {
	matchers := make([]tablePattern, 0, len(routes))
	for _, route := range routes {
		matchers = append(matchers, route.matchers...)
	}
	for _, filter := range filters {
		covered := slices.ContainsFunc(matchers, func(matcher tablePattern) bool {
			return filter.schema.covers(matcher.schema) && filter.table.covers(matcher.table)
		})
		if !covered {
			return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster requires every filter rule to be routed to another table, but filter rule %q is not covered by any dispatch rule matcher", filter.raw)
		}
	}
	return nil
}

// witness is a replicated table whose routed target is replicated as well.
type witness struct {
	schema, table             string
	targetSchema, targetTable string
}

// checkRouteTargets rejects a dispatch rule which can route a replicated table to a table which the
// filter replicates as well. It searches for a witness of such a pair and reports the witness.
func checkRouteTargets(filters []tablePattern, routes []routeRuleToCheck) error {
	for _, route := range routes {
		for _, matcher := range route.matchers {
			if found, ok := findWitness(filters, route, matcher); ok {
				return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster requires route targets to stay outside the filter, but the dispatch rule matching %v routes table %s.%s to %s.%s, which the filter replicates", route.rawMatchers, found.schema, found.table, found.targetSchema, found.targetTable)
			}
		}
	}
	return nil
}

// findWitness looks for a table matched by one filter rule, routed by the given matcher, and
// captured again by another filter rule after routing.
func findWitness(filters []tablePattern, route routeRuleToCheck, matcher tablePattern) (witness, bool) {
	for _, source := range filters {
		for _, target := range filters {
			if found, ok := buildWitness(source, target, route, matcher); ok {
				return found, true
			}
		}
	}
	return witness{}, false
}

// buildWitness combines the constraints of the source rule, the matcher and the target rule, and
// verifies the candidate names against every pattern.
func buildWitness(source, target tablePattern, route routeRuleToCheck, matcher tablePattern) (witness, bool) {
	schemaRequirements := combineRequirements(
		source.schema.requirements(substitution{}),
		matcher.schema.requirements(substitution{}),
		targetRequirements(target.schema, route.schema),
	)
	tableRequirements := combineRequirements(
		source.table.requirements(substitution{}),
		matcher.table.requirements(substitution{}),
		targetRequirements(target.table, route.table),
	)
	for _, schemaRequirement := range schemaRequirements {
		schemaName, ok := schemaRequirement.name()
		if !ok {
			continue
		}
		for _, tableRequirement := range tableRequirements {
			tableName, ok := tableRequirement.name()
			if !ok {
				continue
			}
			if found, ok := verifyWitness(source, target, route, matcher, schemaName, tableName); ok {
				return found, true
			}
		}
	}
	return witness{}, false
}

// verifyWitness checks the candidate table against every pattern.
func verifyWitness(source, target tablePattern, route routeRuleToCheck, matcher tablePattern, schema, table string) (witness, bool) {
	if !source.matches(schema, table) || !matcher.matches(schema, table) {
		return witness{}, false
	}
	targetSchema, targetTable := route.target(schema, table)
	if !target.matches(targetSchema, targetTable) {
		return witness{}, false
	}
	return witness{schema: schema, table: table, targetSchema: targetSchema, targetTable: targetTable}, true
}

// targetRequirements returns the constraints which a target-capturing rule puts on the source name,
// or no alternative when the target can never match that rule.
func targetRequirements(capture namePattern, target targetName) []requirement {
	if !target.hasVariable {
		if !capture.matches(target.literal) {
			return nil
		}
		return []requirement{{}}
	}
	return capture.requirements(target.sub)
}

// combineRequirements merges the alternatives of three constraint groups into one list.
func combineRequirements(first, second, third []requirement) []requirement {
	if len(first) == 0 || len(second) == 0 || len(third) == 0 {
		return nil
	}
	combined := make([]requirement, 0, len(first)*len(second)*len(third))
	for _, a := range first {
		for _, b := range second {
			for _, c := range third {
				combined = append(combined, a.merge(b).merge(c))
			}
		}
	}
	return combined
}
