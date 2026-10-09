#!/bin/bash

# This case verifies the API-layer TOML support added for issue #4936:
#   GET /api/v2/changefeeds/:id with `Accept: application/toml` returns the
#   changefeed as TOML, while the default (or application/json) keeps JSON.
# It mirrors the behaviors covered by the PR1 pytest suite (kebab-case keys,
# config sections, human-readable durations/timestamps, JSON<->TOML field
# correspondence, password redaction) and checks that the exported TOML config
# is import-compatible by feeding it back through `changefeed create --config`.

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test only exercises the HTTP API output format; no downstream sink needed.
if [ "$SINK_TYPE" != "mysql" ]; then
	echo "[$(date)] <<<<<< skip $TEST_NAME for sink type $SINK_TYPE (API output only) >>>>>>"
	exit 0
fi

API="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds"

# assert_jq runs a jq filter and prints an explicit FAIL (instead of letting
# `set -e` abort silently with stdout redirected) so failures are debuggable.
assert_jq() {
	local file="$1" filter="$2" msg="$3"
	if ! jq -e "$filter" "$file" >/dev/null; then
		echo "FAIL: $msg (filter: $filter)"
		echo "--- response body ---"
		cat "$file"
		exit 1
	fi
}

# assert_grep / refute_grep give explicit FAIL output instead of letting a bare
# `grep -q` abort the script under `set -e` with no diagnostic.
assert_grep() {
	local pattern="$1" file="$2" msg="$3"
	if ! grep -q "$pattern" "$file"; then
		echo "FAIL: $msg (missing pattern: $pattern)"
		echo "--- body ---"
		cat "$file"
		exit 1
	fi
}
refute_grep() {
	local pattern="$1" file="$2" msg="$3"
	if grep -q "$pattern" "$file"; then
		echo "FAIL: $msg (unexpected pattern: $pattern)"
		echo "--- body ---"
		cat "$file"
		exit 1
	fi
}

query_json() {
	curl -sf -X GET "$API/$1?keyspace=$KEYSPACE_NAME" -o "$2"
}
query_json_accept() {
	curl -sf -X GET -H 'Accept: application/json' "$API/$1?keyspace=$KEYSPACE_NAME" -o "$2"
}
query_toml() {
	curl -sf -X GET -H 'Accept: application/toml' "$API/$1?keyspace=$KEYSPACE_NAME" -o "$2"
}

check_credential_redaction() {
	local outputs="$WORK_DIR/credential_outputs"
	mkdir -p "$outputs"
	cdc_cli_changefeed create -c cf-credentials \
		--sink-uri='blackhole://user:uri-credential-sentinel@localhost/?password=query-credential-sentinel&kafka-client=franz' \
		--config="$CUR/conf/credentials.toml" >"$outputs/create.txt" 2>&1
	query_json cf-credentials "$outputs/query.json"
	query_toml cf-credentials "$outputs/query.toml"
	cdc_cli_changefeed query -c cf-credentials >"$outputs/cli-query.txt" 2>&1
	curl -fsS "$API?keyspace=$KEYSPACE_NAME" -o "$outputs/list.json"
	for endpoint in debug/info api/v2/unsafe/metadata; do
		curl -fsS "http://${CDC_HOST}:${CDC_PORT}/$endpoint?keyspace=$KEYSPACE_NAME" \
			-o "$outputs/${endpoint//\//-}.txt"
	done

	# Secrets are absent, while public identifiers and the Kafka driver survive.
	jq -e '.config.sink.kafka_config.sasl_user == "visible-user" and
		.config.sink.kafka_config.sasl_oauth_client_id == "visible-client" and
		.config.sink.kafka_config.glue_schema_registry_config.registry_name == "visible-registry" and
		.config.sink.pulsar_config["tls-certificate-path"] == "visible-pulsar-key" and
		.config.sink.pulsar_config["tls-private-key-path"] == "visible-pulsar-cert" and
		(.sink_uri | contains("kafka-client=franz"))' "$outputs/query.json" >/dev/null
	python3 - "$outputs/query.json" "$outputs/query.toml" <<'PY'
import json, sys, tomllib

with open(sys.argv[1]) as f:
    j = json.load(f)["config"]
with open(sys.argv[2], "rb") as f:
    t = tomllib.load(f)["config"]
paths = [
    ("consistent", "storage"),
    ("sink", "kafka_config", "sasl_password"),
    ("sink", "kafka_config", "sasl_gssapi_password"),
    ("sink", "kafka_config", "sasl_oauth_client_secret"),
    ("sink", "kafka_config", "sasl_oauth_token_url"),
    ("sink", "kafka_config", "key"),
    ("sink", "kafka_config", "large_message_handle", "claim_check_storage_uri"),
    ("sink", "kafka_config", "glue_schema_registry_config", "access_key"),
    ("sink", "kafka_config", "glue_schema_registry_config", "secret_access_key"),
    ("sink", "kafka_config", "glue_schema_registry_config", "token"),
    ("sink", "pulsar_config", "authentication-token"),
    ("sink", "pulsar_config", "basic-password"),
    ("sink", "pulsar_config", "oauth2", "oauth2-private-key"),
    ("sink", "pulsar_config", "oauth2", "oauth2-issuer-url"),
]
for fmt, cfg in [("JSON", j), ("TOML", t)]:
    for path in paths:
        keys = [key.replace("_", "-") if fmt == "TOML" else key for key in path]
        node = cfg
        for key in keys[:-1]:
            node = node[key]
        if keys[-1] in node:
            raise SystemExit("credential field was not omitted in %s: %s" % (fmt, ".".join(keys)))
print("PASS: credential fields omitted in JSON and TOML")
PY
	cdc_cli_changefeed pause -c cf-credentials
	echo 'memory-quota = 2097152' >"$WORK_DIR/credential-update.toml"
	cdc_cli_changefeed update -c cf-credentials --config="$WORK_DIR/credential-update.toml" --no-confirm \
		>"$outputs/update.txt" 2>&1
	query_json cf-credentials "$outputs/updated.json"
	jq -e '.config.memory_quota == 2097152 and
		.config.sink.pulsar_config["tls-certificate-path"] == "visible-pulsar-key" and
		.config.sink.pulsar_config["tls-private-key-path"] == "visible-pulsar-cert"' "$outputs/updated.json" >/dev/null
	# Invalid redo and sink storage URIs must reject updates and redact errors.
	for kind in redo sink; do
		if [ "$kind" = redo ]; then
			jq -n '{replica_config:{consistent:{level:"eventual",storage:"s3:///missing-bucket?secret-access-key=redo-credential-sentinel"}}}' >"$WORK_DIR/storage-error.json"
		else
			jq -n '{sink_uri:"s3:///missing-bucket?protocol=canal-json&secret-access-key=storage-credential-sentinel"}' >"$WORK_DIR/storage-error.json"
		fi
		local status
		status=$(curl -sS -X PUT "$API/cf-credentials?keyspace=$KEYSPACE_NAME" \
			-H 'Content-Type: application/json' --data-binary "@$WORK_DIR/storage-error.json" \
			-o "$outputs/$kind-error.json" -w '%{http_code}')
		[ "$status" = 400 ]
		query_json cf-credentials "$outputs/$kind-after.json"
		jq -e --slurp '.[0].config == .[1].config and .[0].sink_uri == .[1].sink_uri' \
			"$outputs/updated.json" "$outputs/$kind-after.json" >/dev/null
	done
	cdc_cli_changefeed resume -c cf-credentials >"$outputs/resume.txt" 2>&1
	# The diagnostic API must also redact the retained credentials after an update.
	curl -fsS "http://${CDC_HOST}:${CDC_PORT}/api/v2/unsafe/metadata?keyspace=$KEYSPACE_NAME" -o "$outputs/metadata-after.json"
	jq -e 'any(.[]; (.value | fromjson? | .config.sink."kafka-config"."sasl-password") == "******")' \
		"$outputs/metadata-after.json" >/dev/null
	# Confluent and Glue are mutually exclusive, so cover Confluent separately.
	cat >"$WORK_DIR/schema-credential.toml" <<EOF
[sink]
schema-registry = "https://user:schema-credential-sentinel@registry.example"
EOF
	cdc_cli_changefeed create -c cf-schema-credential --sink-uri=blackhole:// \
		--config="$WORK_DIR/schema-credential.toml" >"$outputs/schema-create.txt" 2>&1
	query_json cf-schema-credential "$outputs/schema.json"
	query_toml cf-schema-credential "$outputs/schema.toml"
	jq -e '.config.sink | has("schema_registry") | not' "$outputs/schema.json" >/dev/null
	if grep -q 'schema-registry' "$outputs/schema.toml"; then
		echo "FAIL: sensitive schema registry URI was not omitted in TOML"
		exit 1
	fi
	cdc_cli_changefeed remove -c cf-schema-credential >"$outputs/schema-remove.txt" 2>&1
	cdc_cli_changefeed remove -c cf-credentials >"$outputs/remove.txt" 2>&1
	if grep -Eq 'credential-sentinel' "$outputs"/* "$WORK_DIR"/cdc*.log "$WORK_DIR"/stdout*.log; then
		echo "FAIL: credential leaked in API, CLI or CDC log output"
		exit 1
	fi
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Three changefeeds: default config, a non-default config, and a realistic
	# one with credentials in the sink URI (for redaction checks).
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-default"
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-overrides" \
		--config="$CUR/conf/overrides.toml"
	# blackhole sink with embedded credentials: it never dials, but the URI still
	# flows through MaskSinkURI so we can assert redaction.
	cdc_cli_changefeed create \
		--sink-uri="blackhole://root:topsecretpass@10.0.0.9:3306/" \
		-c "cf-realistic" --config="$CUR/conf/overrides.toml"

	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-default" "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-overrides" "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-realistic" "normal" "null" ""

	query_json "cf-default" "$WORK_DIR/default.json"
	query_toml "cf-default" "$WORK_DIR/default.toml"
	query_json "cf-overrides" "$WORK_DIR/overrides.json"
	query_toml "cf-overrides" "$WORK_DIR/overrides.toml"
	query_toml "cf-realistic" "$WORK_DIR/realistic.toml"
	query_json "cf-realistic" "$WORK_DIR/realistic.json"

	# --- Test 1: default response and Accept: application/json return JSON ---
	assert_jq "$WORK_DIR/default.json" '.id == "cf-default"' "Test 1 - default response id"
	assert_jq "$WORK_DIR/default.json" '.config != null' "Test 1 - default response has config"
	query_json_accept "cf-default" "$WORK_DIR/default_json.json"
	assert_jq "$WORK_DIR/default_json.json" '.id == "cf-default"' "Test 1 - Accept application/json"
	echo "PASS: Test 1 - default and Accept application/json return JSON"

	# --- Test 2: Accept: application/toml returns a TOML body ---
	# A TOML body has bare key = value lines, not a leading JSON brace.
	if [ "$(head -c 1 "$WORK_DIR/default.toml")" = "{" ]; then
		echo "FAIL: Test 2 - expected TOML, got JSON-looking body"
		cat "$WORK_DIR/default.toml"
		exit 1
	fi
	assert_grep '^id = "cf-default"' "$WORK_DIR/default.toml" "Test 2 - top-level id"
	assert_grep '^sink-uri' "$WORK_DIR/default.toml" "Test 2 - top-level sink-uri"
	# GID is runtime metadata and must be omitted from TOML (toml:"-").
	refute_grep '^gid' "$WORK_DIR/default.toml" "Test 2 - gid omitted"
	echo "PASS: Test 2 - Accept application/toml returns TOML"

	# --- Test 3: TOML uses kebab-case keys, never snake_case ---
	for k in sink-uri start-ts upstream-id case-sensitive check-gc-safe-point worker-num; do
		assert_grep "$k" "$WORK_DIR/default.toml" "Test 3 - kebab-case key $k present"
	done
	for k in sink_uri start_ts upstream_id case_sensitive check_gc_safe_point worker_num; do
		refute_grep "$k" "$WORK_DIR/default.toml" "Test 3 - snake_case key $k absent"
	done
	echo "PASS: Test 3 - TOML uses kebab-case keys"

	# --- Test 4: config lives under [config] and nested [config.*] sections ---
	for sec in '^\[config\]' '^  \[config.filter\]' '^  \[config.mounter\]' '^  \[config.sink\]'; do
		assert_grep "$sec" "$WORK_DIR/default.toml" "Test 4 - section $sec"
	done
	echo "PASS: Test 4 - config under [config] sections"

	# --- Test 5: durations are human-readable strings, not nanoseconds/tables ---
	refute_grep '^  \[changefeed-error-stuck-duration\]' "$WORK_DIR/default.toml" \
		"Test 5 - duration must not be an empty table"
	python3 - "$WORK_DIR/default.toml" <<'PY'
import sys, tomllib


def fail(msg):
    print("FAIL: Test 5 - " + msg)
    sys.exit(1)


with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)
cfg = data.get("config", {})
for key, want in [
    ("sync-point-interval", "10m0s"),
    ("sync-point-retention", "24h0m0s"),
    ("changefeed-error-stuck-duration", "30m0s"),
]:
    val = cfg.get(key)
    if val is not None and val != want:
        fail("%s: expected %r got %r" % (key, want, val))
print("PASS: Test 5 - durations are human-readable strings")
PY

	# --- Test 6: JSONTime fields render in readable form (not raw structs) ---
	# checkpoint-time is api.JSONTime, whose MarshalText emits "2006-01-02 15:04:05.000".
	python3 - "$WORK_DIR/default.toml" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
if not re.search(r'checkpoint-time = "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', text):
    print("FAIL: Test 6 - checkpoint-time not in readable format")
    print(text[:400])
    sys.exit(1)
print("PASS: Test 6 - JSONTime fields are readable")
PY

	# --- Test 7: overrides applied, in both JSON and TOML ---
	assert_jq "$WORK_DIR/overrides.json" '.config.case_sensitive == true' "Test 7 - json case_sensitive"
	assert_jq "$WORK_DIR/overrides.json" '.config.mounter.worker_num == 8' "Test 7 - json worker_num"
	python3 - "$WORK_DIR/overrides.toml" <<'PY'
import sys, tomllib


def fail(msg):
    print("FAIL: Test 7 - " + msg)
    sys.exit(1)


with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)
if data.get("id") != "cf-overrides":
    fail("id mismatch: %r" % data.get("id"))
cfg = data.get("config")
if not isinstance(cfg, dict):
    fail("missing [config] section")
if cfg.get("case-sensitive") is not True:
    fail("case-sensitive: %r" % cfg.get("case-sensitive"))
mounter = cfg.get("mounter")
if not isinstance(mounter, dict) or mounter.get("worker-num") != 8:
    fail("mounter.worker-num: %r" % (mounter,))
flt = cfg.get("filter")
want_rules = ["test_db.*", "db_alpha.*", "db_bravo.*", "db_charlie.*"]
if not isinstance(flt, dict) or flt.get("rules") != want_rules:
    fail("filter.rules: %r" % (flt,))
sched = cfg.get("scheduler")
if not isinstance(sched, dict) or sched.get("enable-table-across-nodes") is not True \
        or sched.get("region-threshold") != 1000:
    fail("scheduler: %r" % (sched,))
print("PASS: Test 7 - overrides applied in TOML")
PY

	# --- Test 8: JSON and TOML carry the same data (kebab vs snake) ---
	python3 - "$WORK_DIR/overrides.json" "$WORK_DIR/overrides.toml" <<'PY'
import json, sys, tomllib


def fail(msg):
    print("FAIL: Test 8 - " + msg)
    sys.exit(1)


with open(sys.argv[1]) as f:
    j = json.load(f)
with open(sys.argv[2], "rb") as f:
    t = tomllib.load(f)

for jk, tk in [("id", "id"), ("keyspace", "keyspace"), ("sink_uri", "sink-uri"),
               ("start_ts", "start-ts"), ("upstream_id", "upstream-id"),
               ("state", "state")]:
    if j.get(jk) != t.get(tk):
        fail("top-level %s/%s: json=%r toml=%r" % (jk, tk, j.get(jk), t.get(tk)))

jc, tc = j["config"], t["config"]
if jc["case_sensitive"] != tc["case-sensitive"]:
    fail("config.case-sensitive: json=%r toml=%r" % (jc["case_sensitive"], tc["case-sensitive"]))
if jc["mounter"]["worker_num"] != tc["mounter"]["worker-num"]:
    fail("mounter.worker-num: json=%r toml=%r" % (jc["mounter"]["worker_num"], tc["mounter"]["worker-num"]))
if jc["filter"]["rules"] != tc["filter"]["rules"]:
    fail("filter.rules: json=%r toml=%r" % (jc["filter"]["rules"], tc["filter"]["rules"]))
if jc["scheduler"]["enable_table_across_nodes"] != tc["scheduler"]["enable-table-across-nodes"]:
    fail("scheduler.enable-table-across-nodes mismatch")
if jc["scheduler"]["region_threshold"] != tc["scheduler"]["region-threshold"]:
    fail("scheduler.region-threshold mismatch")
print("PASS: Test 8 - JSON and TOML carry the same data")
PY

	# --- Test 9: sink URI credentials are redacted in both formats ---
	refute_grep 'topsecretpass' "$WORK_DIR/realistic.toml" "Test 9 - password redacted in TOML"
	refute_grep 'topsecretpass' "$WORK_DIR/realistic.json" "Test 9 - password redacted in JSON"
	assert_grep 'xxxxx' "$WORK_DIR/realistic.toml" "Test 9 - redaction marker in TOML"
	echo "PASS: Test 9 - sink URI credentials redacted"

	# --- Test 10: exported TOML config is import-compatible ---
	# Extract the [config] subset into a standalone file and create a new
	# changefeed from it, proving round-trip with `changefeed create --config`.
	python3 - "$WORK_DIR/overrides.toml" "$WORK_DIR/reimport.toml" <<'PY'
import sys, tomllib


def fail(msg):
    print("FAIL: Test 10 - " + msg)
    sys.exit(1)


with open(sys.argv[1], "rb") as f:
    data = tomllib.load(f)
cfg = data.get("config")
if not isinstance(cfg, dict):
    fail("missing [config] section")
mounter = cfg.get("mounter")
flt = cfg.get("filter")
if not isinstance(mounter, dict) or "worker-num" not in mounter:
    fail("missing mounter.worker-num")
if not isinstance(flt, dict) or "rules" not in flt:
    fail("missing filter.rules")
# Minimal hand-serialization of the subset we set, in kebab-case TOML, so we
# do not depend on a TOML writer being installed in the test environment.
lines = [
    'case-sensitive = %s' % ("true" if cfg.get("case-sensitive") else "false"),
    '[mounter]',
    '  worker-num = %d' % mounter["worker-num"],
    '[filter]',
    '  rules = [%s]' % ", ".join('"%s"' % r for r in flt["rules"]),
]
with open(sys.argv[2], "w") as f:
    f.write("\n".join(lines) + "\n")
print("wrote reimport config")
PY
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-reimport" \
		--config="$WORK_DIR/reimport.toml"
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-reimport" "normal" "null" ""
	echo "PASS: Test 10 - exported TOML config re-imports successfully"
	check_credential_redaction

	# Cleanup changefeeds
	cdc_cli_changefeed --changefeed-id "cf-default" remove
	cdc_cli_changefeed --changefeed-id "cf-overrides" remove
	cdc_cli_changefeed --changefeed-id "cf-realistic" remove
	cdc_cli_changefeed --changefeed-id "cf-reimport" remove

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
