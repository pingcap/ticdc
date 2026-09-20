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

package check

import (
	"slices"
	"strings"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

// This file validates database isolation for `allow-same-cluster`. Every replicated table must
// be routed, and every target schema must stay outside the schemas selected by the filter.
// Database DDL matches schema names even when a filter or matcher names only specific tables.
// Consequently, changing only the table name cannot isolate a changefeed from its own writes.
//
// Validation reasons about patterns to cover future databases and tables. Unsupported patterns
// are rejected. Rules overlapping on a source schema must use the same target-schema expression
// so database DDL has an unambiguous target, including rules whose table matchers are disjoint.

const (
	schemaPlaceholder = "{schema}"
	tablePlaceholder  = "{table}"
	// unsupportedPatternChars are characters which this decision does not parse, so a pattern which
	// contains one of them is rejected as unsupported.
	unsupportedPatternChars = "*?[]{}/\\`\""
)

// ValidateSameClusterRouting rejects a changefeed which may replicate into its own cluster unless
// its routing rules place every replicated table outside the source schema range. It is the static
// counterpart of IsSameUpstreamDownstream: the flag is only safe when this validation passes, so it is evaluated
// where the flag takes effect, on the configuration which is actually in use.
func ValidateSameClusterRouting(cfg *config.ChangefeedConfig) error {
	if cfg == nil || !cfg.AllowSameCluster {
		return nil
	}
	if !cfg.SinkConfig.TableRouteEnabled() {
		return errors.ErrInvalidReplicaConfig.FastGenByArgs("allow-same-cluster requires table routing to be enabled")
	}
	return validateRouting(cfg.Filter, cfg.SinkConfig.DispatchRules, cfg.CaseSensitive)
}

// validateRouting checks the routing rules against the filter rules.
func validateRouting(filter *config.FilterConfig, dispatch []*config.DispatchRule, caseSensitive bool) error {
	filters, err := parseFilterRules(effectiveFilterRules(filter), caseSensitive)
	if err != nil {
		return err
	}
	routes, err := parseRouteRules(dispatch, caseSensitive)
	if err != nil {
		return err
	}
	if err := verifyFilterRulesCovered(filters, routes); err != nil {
		return err
	}
	if err := verifySchemaTargets(filters, routes); err != nil {
		return err
	}
	return verifySchemaRoutingConsistent(filters, routes)
}

func normalizeName(name string, caseSensitive bool) string {
	if caseSensitive {
		return name
	}
	return strings.ToLower(name)
}

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

// overlaps reports whether the patterns share a name, preserving case sensitivity.
func (p namePattern) overlaps(other namePattern) bool {
	// A prefix and a suffix always share their concatenation. For all other
	// supported pattern pairs, an overlap means one pattern covers the other.
	return p.covers(other) || other.covers(p) ||
		(p.kind == patternPrefix && other.kind == patternSuffix) ||
		(p.kind == patternSuffix && other.kind == patternPrefix)
}

// targetName is a parsed route target expression: literal text around one placeholder, or a literal
// name. An empty expression keeps the source name, which is an empty substitution.
type targetName struct {
	literal     string
	prefix      string
	suffix      string
	hasVariable bool
}

// apply returns the target name of a source name.
func (t targetName) apply(name string) string {
	if t.hasVariable {
		return t.prefix + name + t.suffix
	}
	return t.literal
}

// tablePattern is a parsed `schema.table` pattern: a filter rule, or a matcher of a dispatch rule
// which has a target.
type tablePattern struct {
	raw    string
	schema namePattern
	table  namePattern
}

// routeRuleToCheck pairs one matcher with its dispatch rule target.
// Dispatch rules with multiple matchers are expanded during parsing.
type routeRuleToCheck struct {
	rawMatchers      []string
	matcher          tablePattern
	schema           targetName
	schemaExpression string
}

// effectiveFilterRules returns the filter rules which are in effect. It mirrors
// pkg/filter.VerifyTableRules, where unset rules replicate every table.
func effectiveFilterRules(cfg *config.FilterConfig) []string {
	if cfg == nil || len(cfg.Rules) == 0 {
		return []string{"*.*"}
	}
	return cfg.Rules
}

func parseFilterRules(rules []string, caseSensitive bool) ([]tablePattern, error) {
	parsed := make([]tablePattern, 0, len(rules))
	for _, rule := range rules {
		pattern, err := parseRulePattern("filter rule", rule, caseSensitive)
		if err != nil {
			return nil, err
		}
		parsed = append(parsed, pattern)
	}
	return parsed, nil
}

func parseRouteRules(rules []*config.DispatchRule, caseSensitive bool) ([]routeRuleToCheck, error) {
	parsed := make([]routeRuleToCheck, 0, len(rules))
	for _, rule := range rules {
		// Rules without a target keep the table name and are ignored by the router.
		if rule == nil || (rule.TargetSchema == "" && rule.TargetTable == "") {
			continue
		}
		route := routeRuleToCheck{rawMatchers: rule.Matcher, schemaExpression: rule.TargetSchema}
		matchers := make([]tablePattern, 0, len(rule.Matcher))
		for _, matcher := range rule.Matcher {
			pattern, err := parseRulePattern("dispatch rule matcher", matcher, caseSensitive)
			if err != nil {
				return nil, err
			}
			matchers = append(matchers, pattern)
		}
		var reason string
		var ok bool
		if route.schema, reason, ok = parseTargetExpression(rule.TargetSchema, schemaPlaceholder); !ok {
			return nil, unsupportedError("target schema of the dispatch rule matching "+strings.Join(rule.Matcher, ","), rule.TargetSchema, reason)
		}
		if _, reason, ok = parseTargetExpression(rule.TargetTable, tablePlaceholder); !ok {
			return nil, unsupportedError("target table of the dispatch rule matching "+strings.Join(rule.Matcher, ","), rule.TargetTable, reason)
		}
		// Normalize literal text after parsing so placeholder names remain case sensitive.
		// The runtime filter normalizes the entire substituted target before matching it.
		route.schema.literal = normalizeName(route.schema.literal, caseSensitive)
		route.schema.prefix = normalizeName(route.schema.prefix, caseSensitive)
		route.schema.suffix = normalizeName(route.schema.suffix, caseSensitive)
		for _, matcher := range matchers {
			route.matcher = matcher
			parsed = append(parsed, route)
		}
	}
	return parsed, nil
}

// parseRulePattern parses a `schema.table` pattern into its two parts. It rejects, instead of
// approximating, the patterns which this decision cannot handle.
func parseRulePattern(kind, pattern string, caseSensitive bool) (tablePattern, error) {
	// Match table-filter's rule preprocessing without trimming inside quoted names.
	schemaPart, tablePart, ok := splitRulePattern(strings.Trim(pattern, " \t"))
	if !ok {
		return tablePattern{}, unsupportedError(kind, pattern, "expected a `schema.table` pattern")
	}
	schema, reason, ok := parseNamePattern(normalizeName(schemaPart, caseSensitive))
	if !ok {
		return tablePattern{}, unsupportedError(kind, pattern, "schema pattern "+reason)
	}
	table, reason, ok := parseNamePattern(normalizeName(tablePart, caseSensitive))
	if !ok {
		return tablePattern{}, unsupportedError(kind, pattern, "table pattern "+reason)
	}
	return tablePattern{raw: pattern, schema: schema, table: table}, nil
}

// unsupportedError reports a configuration which the decision cannot handle.
func unsupportedError(what, name, reason string) error {
	return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster does not support the %s %q: %s", what, name, reason)
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
// leading `*`, a single trailing `*`, and quoted names. It returns the reason for a pattern which
// is not supported.
func parseNamePattern(part string) (namePattern, string, bool) {
	if part == "" {
		return namePattern{}, "is empty", false
	}
	switch part[0] {
	case '!':
		// Negated rules depend on the rule order of the table filter, which this decision does not
		// reason about.
		return namePattern{}, "is negated, which is not supported", false
	case '`', '"':
		name, ok := unquoteName(part)
		if !ok {
			return namePattern{}, "has an unsupported quoted name", false
		}
		return namePattern{kind: patternLiteral, value: name}, "", true
	}
	if rest, ok := strings.CutPrefix(part, "*"); ok {
		if rest == "" {
			return namePattern{kind: patternAny}, "", true
		}
		if strings.ContainsAny(rest, unsupportedPatternChars) {
			return namePattern{}, "uses characters or wildcards which are not supported", false
		}
		return namePattern{kind: patternSuffix, value: rest}, "", true
	}
	if rest, ok := strings.CutSuffix(part, "*"); ok {
		if strings.ContainsAny(rest, unsupportedPatternChars) {
			return namePattern{}, "uses characters or wildcards which are not supported", false
		}
		return namePattern{kind: patternPrefix, value: rest}, "", true
	}
	if strings.ContainsAny(part, unsupportedPatternChars) {
		return namePattern{}, "uses characters or wildcards which are not supported", false
	}
	return namePattern{kind: patternLiteral, value: part}, "", true
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

// parseTargetExpression parses a route target expression. An empty expression keeps the source name,
// which is an empty substitution. It returns the reason for an expression which is not supported.
func parseTargetExpression(expr, placeholder string) (targetName, string, bool) {
	unsupported := "expected literal text with at most one " + placeholder + " placeholder"
	if expr == "" {
		return targetName{hasVariable: true}, "", true
	}
	head, tail, found := strings.Cut(expr, placeholder)
	if !found {
		if strings.ContainsAny(expr, "{}") {
			return targetName{}, unsupported, false
		}
		return targetName{literal: expr}, "", true
	}
	if strings.ContainsAny(head+tail, "{}") {
		return targetName{}, unsupported, false
	}
	return targetName{prefix: head, suffix: tail, hasVariable: true}, "", true
}

// verifyFilterRulesCovered rejects filter rules which no dispatch rule with a target would route:
// such a table keeps its own name and is replicated into itself.
func verifyFilterRulesCovered(filters []tablePattern, routes []routeRuleToCheck) error {
	for _, filter := range filters {
		covered := slices.ContainsFunc(routes, func(route routeRuleToCheck) bool {
			return filter.schema.covers(route.matcher.schema) && filter.table.covers(route.matcher.table)
		})
		if !covered {
			return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster requires every filter rule to be routed to another table, but filter rule %q is not covered by any dispatch rule matcher", filter.raw)
		}
	}
	return nil
}

// verifySchemaTargets considers all schema matches, regardless of the table part, just like
// the runtime router and filter do for database DDL.
func verifySchemaTargets(filters []tablePattern, routes []routeRuleToCheck) error {
	for _, route := range routes {
		if schema, ok := route.findCapturedSchema(filters); ok {
			return errors.ErrInvalidReplicaConfig.FastGen(
				"allow-same-cluster requires database isolation, but the dispatch rule matching %v "+
					"routes schema %q to %q, which the filter replicates",
				route.rawMatchers, schema, route.schema.apply(schema))
		}
	}
	return nil
}

// findCapturedSchema checks target overlap using TiDB's case insensitive identifier lookup.
// Folding is local to this isolation check: route coverage and schema routing consistency
// must still use the configured filter semantics.
func (r routeRuleToCheck) findCapturedSchema(filters []tablePattern) (string, bool) {
	matcher := r.matcher.schema
	matcher.value = strings.ToLower(matcher.value)
	target := r.schema
	target.literal = strings.ToLower(target.literal)
	target.prefix = strings.ToLower(target.prefix)
	target.suffix = strings.ToLower(target.suffix)
	for _, source := range filters {
		// A case sensitive matcher that cannot select this source never routes its DDL.
		if !source.schema.overlaps(r.matcher.schema) {
			continue
		}
		source.schema.value = strings.ToLower(source.schema.value)
		for _, capture := range filters {
			capture.schema.value = strings.ToLower(capture.schema.value)
			if schema, ok := findNameWitness(source.schema, matcher, capture.schema, target); ok {
				return schema, true
			}
		}
	}
	return "", false
}

// verifySchemaRoutingConsistent requires identical target-schema expressions wherever two routes
// overlap on a source schema. The runtime router compares all matching schema targets, including
// rules shadowed at table level, and compares their original spelling even in case insensitive mode.
func verifySchemaRoutingConsistent(filters []tablePattern, routes []routeRuleToCheck) error {
	for i, left := range routes {
		for _, right := range routes[i+1:] {
			if left.schemaExpression == right.schemaExpression {
				continue
			}
			if schema, ok := findSharedSourceSchema(filters, left.matcher.schema, right.matcher.schema); ok {
				return errors.ErrInvalidReplicaConfig.FastGen(
					"allow-same-cluster does not support different target-schema expressions %q and %q "+
						"for routes matching source schema %q",
					left.schemaExpression, right.schemaExpression, schema)
			}
		}
	}
	return nil
}

// findSharedSourceSchema intersects two schema matchers within the replicated source range.
func findSharedSourceSchema(filters []tablePattern, left, right namePattern) (string, bool) {
	for _, source := range filters {
		if schema, ok := findNameWitness(source.schema, left, right, targetName{hasVariable: true}); ok {
			return schema, true
		}
	}
	return "", false
}

// findNameWitness finds one source name whose routed target is captured again.
func findNameWitness(source, matcher, capture namePattern, target targetName) (string, bool) {
	if !target.hasVariable && !capture.matches(target.literal) {
		return "", false
	}
	for _, name := range candidateNames(source, matcher, capture, target) {
		if source.matches(name) && matcher.matches(name) && capture.matches(target.apply(name)) {
			return name, true
		}
	}
	return "", false
}

// candidateNames lists the names which can satisfy the source rule, the matcher and the target rule
// at the same time. Every candidate is verified before use. The set contains the literal text of
// each pattern, the head or tail the pattern requires, every combination of a head with a tail,
// and the names where the substitution completes a pattern.
func candidateNames(source, matcher, capture namePattern, target targetName) []string {
	atoms := slices.Concat(
		atomsOf(source, "", ""),
		atomsOf(matcher, "", ""),
		atomsOf(capture, target.prefix, target.suffix),
	)
	names := make([]string, 0, 3*len(atoms)+len(atoms)*len(atoms)+1)
	names = append(names, "x")
	for _, atom := range atoms {
		names = append(names, atom, atom+"x", "x"+atom)
		for _, other := range atoms {
			names = append(names, atom+other)
		}
	}
	return names
}

// atomsOf returns the names which this pattern contributes to the candidates, given that `prefix`
// and `suffix` are added around the name before the pattern is matched (both empty when the pattern
// is matched directly).
func atomsOf(p namePattern, prefix, suffix string) []string {
	switch p.kind {
	case patternAny:
		return nil
	case patternLiteral:
		if !strings.HasPrefix(p.value, prefix) || !strings.HasSuffix(p.value, suffix) || len(p.value) < len(prefix)+len(suffix) {
			return nil
		}
		if name := p.value[len(prefix) : len(p.value)-len(suffix)]; name != "" {
			return []string{name}
		}
		return nil
	case patternPrefix:
		if len(p.value) <= len(prefix) {
			// The prefix covers the requirement on its own.
			return nil
		}
		if !strings.HasPrefix(p.value, prefix) {
			return nil
		}
		return prefixesOf(p.value[len(prefix):])
	case patternSuffix:
		if len(p.value) <= len(suffix) {
			// The suffix covers the requirement on its own.
			return nil
		}
		if !strings.HasSuffix(p.value, suffix) {
			return nil
		}
		return suffixesOf(p.value[:len(p.value)-len(suffix)])
	default:
		return nil
	}
}

// prefixesOf returns every non-empty prefix of text, shortest first.
func prefixesOf(text string) []string {
	prefixes := make([]string, 0, len(text))
	for end := range len(text) {
		prefixes = append(prefixes, text[:end+1])
	}
	return prefixes
}

// suffixesOf returns every non-empty suffix of text, longest first.
func suffixesOf(text string) []string {
	suffixes := make([]string, 0, len(text))
	for start := range len(text) {
		suffixes = append(suffixes, text[start:])
	}
	return suffixes
}
